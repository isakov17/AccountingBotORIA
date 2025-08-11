from aiogram import Router, Bot
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove, ReplyKeyboardMarkup, KeyboardButton
from sheets import sheets_service, is_user_allowed, save_receipt, is_fiscal_doc_unique
from utils import parse_qr_from_photo
import logging
from datetime import datetime
import aiohttp
from config import PROVERKACHEKA_TOKEN
import re

logger = logging.getLogger("AccountingBot")
router = Router()

class AddReceipt(StatesGroup):
    UPLOAD_QR = State()
    CUSTOMER = State()
    DELIVERY_STATUS = State()
    DELIVERY_DATE = State()
    TYPE = State()
    FISCAL_DOC = State()
    DATE = State()
    AMOUNT = State()
    STORE = State()
    ITEMS = State()
    CONFIRM_ACTION = State()
    CANCEL_ACTION = State()

@router.message(Command("add_manual"))
async def add_manual_receipt(message: Message, state: FSMContext):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("Доступ запрещен.")
        logger.info(f"Доступ запрещен для /add_manual: user_id={message.from_user.id}")
        return
    await state.update_data(username=message.from_user.username or str(message.from_user.id))  # Сохраняем username или id как запасной вариант
    keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
    await message.answer("Введите заказчика (или /skip):", reply_markup=keyboard)
    await state.set_state(AddReceipt.CUSTOMER)
    logger.info(f"Начало ручного добавления чека: user_id={message.from_user.id}")

@router.message(AddReceipt.CUSTOMER)
async def process_customer(message: Message, state: FSMContext):
    customer = message.text if message.text != "/skip" else "Неизвестно"
    await state.update_data(customer=customer)
    inline_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Доставка", callback_data="доставка")],
        [InlineKeyboardButton(text="Покупка в магазине", callback_data="магазин")]
    ])
    reply_keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
    await message.answer("Это доставка или покупка в магазине?", reply_markup=inline_keyboard)
    await message.answer("Или сбросьте действие:", reply_markup=reply_keyboard)
    await state.set_state(AddReceipt.DELIVERY_STATUS)
    logger.info(f"Заказчик принят: {customer}, user_id={message.from_user.id}")

@router.message(AddReceipt.DELIVERY_STATUS)
async def process_delivery_status(message: Message, state: FSMContext):
    keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
    if message.text == "/skip":
        delivery_status = "магазин"
    else:
        delivery_status = message.text.lower()
    await state.update_data(delivery_status=delivery_status)
    if delivery_status == "доставка":
        await message.answer("Введите дату доставки (ддммгг, например 010125 для 01.01.2025) или /skip:", reply_markup=keyboard)
        await state.set_state(AddReceipt.DELIVERY_DATE)
    else:
        await message.answer("Введите фискальный номер:", reply_markup=keyboard)
        await state.set_state(AddReceipt.FISCAL_DOC)
    logger.info(f"Статус доставки принят: {delivery_status}, user_id={message.from_user.id}")

@router.message(AddReceipt.DELIVERY_DATE)
async def process_delivery_date(message: Message, state: FSMContext):
    keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
    if message.text == "/skip":
        delivery_date = ""
    else:
        date_pattern = r"^\d{6}$"  # Проверяем, что введено ровно 6 цифр
        if not re.match(date_pattern, message.text):
            await message.answer("Неверный формат даты. Используйте ддммгг (6 цифр, например 110825 для 11.08.2025) или /skip.", reply_markup=keyboard)
            return
        try:
            day = message.text[0:2]
            month = message.text[2:4]
            year = message.text[4:6]
            # Предполагаем, что текущий год - 2025, берем последние две цифры
            full_year = f"20{year}"  # Формируем полный год (например, 2025)
            normalized_date = f"{day}.{month}.{full_year}"
            datetime.strptime(normalized_date, "%d.%m.%Y")
            delivery_date = normalized_date
        except ValueError:
            await message.answer("Неверный формат даты. Используйте ддммгг (6 цифр, например 110825 для 11.08.2025) или /skip.", reply_markup=keyboard)
            return
    await state.update_data(delivery_date=delivery_date)
    await message.answer("Введите фискальный номер:", reply_markup=keyboard)
    await state.set_state(AddReceipt.FISCAL_DOC)
    logger.info(f"Дата доставки принята: {delivery_date}, user_id={message.from_user.id}")

@router.message(AddReceipt.FISCAL_DOC)
async def process_fiscal_doc(message: Message, state: FSMContext):
    fiscal_doc = message.text
    if not fiscal_doc.isdigit() or len(fiscal_doc) > 20:
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer("Фискальный номер должен содержать только цифры и быть не длиннее 20 символов.", reply_markup=keyboard)
        return
    if not await is_fiscal_doc_unique(fiscal_doc):
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer(f"Чек с фискальным номером {fiscal_doc} уже существует.", reply_markup=keyboard)
        await state.clear()
        return
    await state.update_data(fiscal_doc=fiscal_doc)
    keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
    await message.answer("Введите дату чека (дд.мм.гггг или дд-мм-гггг):", reply_markup=keyboard)
    await state.set_state(AddReceipt.DATE)
    logger.info(f"Фискальный номер принят: {fiscal_doc}, user_id={message.from_user.id}")

@router.message(AddReceipt.DATE)
async def process_date(message: Message, state: FSMContext):
    date_pattern = r"^\d{2}[-.]\d{2}[-.]\d{4}$"
    keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
    if not re.match(date_pattern, message.text):
        await message.answer("Неверный формат даты. Используйте дд.мм.гггг или дд-мм-гггг.", reply_markup=keyboard)
        return
    try:
        normalized_date = message.text.replace("-", ".")
        datetime.strptime(normalized_date, "%d.%m.%Y")
        await state.update_data(date=normalized_date)
        await message.answer("Введите сумму чека:", reply_markup=keyboard)
        await state.set_state(AddReceipt.AMOUNT)
        logger.info(f"Дата чека принята: {normalized_date}, user_id={message.from_user.id}")
    except ValueError:
        await message.answer("Неверный формат даты. Используйте дд.мм.гггг или дд-мм-гггг.", reply_markup=keyboard)

@router.message(AddReceipt.AMOUNT)
async def process_amount(message: Message, state: FSMContext):
    try:
        total_sum = float(message.text)
        if total_sum <= 0:
            keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
            await message.answer("Сумма должна быть больше 0.", reply_markup=keyboard)
            return
        await state.update_data(total_sum=total_sum)
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer("Введите название магазина:", reply_markup=keyboard)
        await state.set_state(AddReceipt.STORE)
        logger.info(f"Сумма принята: {total_sum}, user_id={message.from_user.id}")
    except ValueError:
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer("Неверный формат суммы. Введите число.", reply_markup=keyboard)

@router.message(AddReceipt.STORE)
async def process_store(message: Message, state: FSMContext):
    store = message.text
    await state.update_data(store=store)
    keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
    await message.answer("Введите товары в формате: Товар,Количество,Сумма;Товар,Количество,Сумма (например: Хлеб,2,100;Молоко,1,50):", reply_markup=keyboard)
    await state.set_state(AddReceipt.ITEMS)
    logger.info(f"Магазин принят: {store}, user_id={message.from_user.id}")

@router.message(AddReceipt.ITEMS)
async def process_items(message: Message, state: FSMContext):
    try:
        items = []
        total_sum_check = 0
        data = await state.get_data()
        total_sum = data["total_sum"]
        for item_str in message.text.split(";"):
            name, quantity, price = item_str.split(",")
            quantity = int(quantity)
            price = float(price)
            if quantity <= 0 or price <= 0:
                keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
                await message.answer("Количество и сумма должны быть больше 0.", reply_markup=keyboard)
                return
            items.append({"name": name.strip(), "quantity": quantity, "sum": price * quantity})
            total_sum_check += price * quantity
        if abs(total_sum_check - total_sum) > 0.01:
            keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
            await message.answer(f"Сумма товаров ({total_sum_check:.2f}) не совпадает с указанной суммой чека ({total_sum:.2f}).", reply_markup=keyboard)
            return
        receipt = {
            "date": data["date"],
            "store": data["store"],
            "items": items,
            "receipt_type": "Предоплата" if data.get("delivery_status") == "доставка" else "Полный",
            "fiscal_doc": data["fiscal_doc"],
            "qr_string": "",
            "delivery_date": data.get("delivery_date", ""),
            "status": "Ожидает" if data.get("delivery_status") == "доставка" else "Доставлено",
            "customer": data["customer"]
        }
        items_list = "\n".join([f"- {item['name']} ({item['quantity']} шт, Сумма: {item['sum']:.2f} RUB)" for item in items])
        details = (
            f"Детали чека:\n"
            f"Магазин: {receipt['store']}\n"
            f"Заказчик: {receipt['customer']}\n"
            f"Дата доставки: {receipt['delivery_date'] or 'Не указана'}\n"
            f"Сумма: {total_sum:.2f} RUB\n"
            f"Товары:\n{items_list}\n"
            f"Фискальный номер: {receipt['fiscal_doc']}"
        )
        inline_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Подтвердить", callback_data="confirm_manual_add")],
            [InlineKeyboardButton(text="Отменить", callback_data="cancel_manual_add")]
        ])
        reply_keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer(details, reply_markup=inline_keyboard)
        await message.answer("Или сбросьте действие:", reply_markup=reply_keyboard)
        await state.update_data(receipt=receipt)
        await state.set_state(AddReceipt.CONFIRM_ACTION)
        logger.info(f"Товары приняты: {message.text}, user_id={message.from_user.id}")
    except ValueError:
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer("Неверный формат товаров. Пример: Товар1,2,100;Товар2,1,200", reply_markup=keyboard)
        logger.info(f"Неверный формат товаров: text={message.text}, user_id={message.from_user.id}")
        return


@router.callback_query(AddReceipt.CONFIRM_ACTION, lambda c: c.data == "confirm_manual_add")
async def confirm_manual_add_action(callback, state: FSMContext):
    data = await state.get_data()
    receipt = data["receipt"]
    username = callback.from_user.username or str(callback.from_user.id)  # Используем username или id как запасной
    for item in receipt["items"]:
        item_receipt = receipt.copy()
        item_receipt["items"] = [item]
        await save_receipt(item_receipt, username, callback.from_user.id, receipt_type="Покупка")  # Передаём username
    await callback.message.answer(f"Чек с фискальным номером {receipt['fiscal_doc']} успешно добавлен.")
    logger.info(f"Чек подтвержден пользователем: fiscal_doc={receipt['fiscal_doc']}, user_id={callback.from_user.id}")
    await state.clear()
    await callback.answer()

@router.callback_query(AddReceipt.CONFIRM_ACTION, lambda c: c.data == "cancel_manual_add")
async def cancel_manual_add_action(callback, state: FSMContext):
    await callback.message.answer("Добавление чека отменено. Начать заново: /add_manual")
    logger.info(f"Добавление чека отменено: user_id={callback.from_user.id}")
    await state.clear()
    await callback.answer()