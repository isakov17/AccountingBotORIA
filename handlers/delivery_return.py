from config import SHEET_NAME, PROVERKACHEKA_TOKEN
from aiogram import Router, Bot
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from sheets import sheets_service, is_user_allowed, is_fiscal_doc_unique, save_receipt, get_monthly_balance, save_receipt_summary
from utils import parse_qr_from_photo
from googleapiclient.errors import HttpError
import logging
from datetime import datetime
import re

logger = logging.getLogger("AccountingBot")
router = Router()

class AddReceiptQR(StatesGroup):
    UPLOAD_QR = State()
    CUSTOMER = State()
    SELECT_TYPE = State()
    CONFIRM_DELIVERY_DATE = State()
    CONFIRM_ACTION = State()

class ConfirmDelivery(StatesGroup):
    SELECT_RECEIPT = State()
    UPLOAD_FULL_QR = State()
    CONFIRM_ACTION = State()

class ReturnReceipt(StatesGroup):
    SELECT_ITEM = State()
    UPLOAD_RETURN_QR = State()
    CONFIRM_ACTION = State()

@router.message(Command("add"))
async def start_add_receipt(message: Message, state: FSMContext):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("Доступ запрещен.")
        logger.info(f"Доступ запрещен для /add: user_id={message.from_user.id}")
        return
    await state.update_data(username=message.from_user.username or str(message.from_user.id))  # Сохраняем username или id как запасной вариант
    await message.answer("Отправьте фото QR-кода чека.")
    await state.set_state(AddReceiptQR.UPLOAD_QR)
    logger.info(f"Начало добавления чека по QR: user_id={message.from_user.id}")

@router.message(AddReceiptQR.UPLOAD_QR)
async def process_qr_upload(message: Message, state: FSMContext, bot: Bot):
    if not message.photo:
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer("Пожалуйста, отправьте фото QR-кода чека.", reply_markup=keyboard)
        logger.info(f"Фото отсутствует для QR: user_id={message.from_user.id}")
        return
    parsed_data = await parse_qr_from_photo(bot, message.photo[-1].file_id)
    if not parsed_data:
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer("Ошибка обработки QR-кода. Убедитесь, что QR-код четкий, или используйте /add_manual для ручного ввода.", reply_markup=keyboard)
        logger.error(f"Ошибка обработки QR-кода: user_id={message.from_user.id}")
        await state.clear()
        return
    if not await is_fiscal_doc_unique(parsed_data["fiscal_doc"]):
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer(f"Чек с фискальным номером {parsed_data['fiscal_doc']} уже существует.", reply_markup=keyboard)
        logger.info(f"Дубликат фискального номера: {parsed_data['fiscal_doc']}, user_id={message.from_user.id}")
        await state.clear()
        return
    await state.update_data(parsed_data=parsed_data)
    keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
    await message.answer("Введите заказчика (или /skip):", reply_markup=keyboard)
    await state.set_state(AddReceiptQR.CUSTOMER)
    logger.info(f"QR-код обработан: fiscal_doc={parsed_data['fiscal_doc']}, user_id={message.from_user.id}")

@router.message(AddReceiptQR.CUSTOMER)
async def process_customer(message: Message, state: FSMContext):
    customer = message.text if message.text != "/skip" else "Неизвестно"
    await state.update_data(customer=customer)
    inline_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Доставка", callback_data="type_delivery")],
        [InlineKeyboardButton(text="Покупка в магазине", callback_data="type_store")]
    ])
    reply_keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
    await message.answer("Это доставка или покупка в магазине?", reply_markup=inline_keyboard)
    await message.answer("Или сбросьте действие:", reply_markup=reply_keyboard)
    await state.set_state(AddReceiptQR.SELECT_TYPE)
    logger.info(f"Заказчик принят: {customer}, user_id={message.from_user.id}")

@router.callback_query(AddReceiptQR.SELECT_TYPE)
async def process_receipt_type(callback, state: FSMContext):
    data = await state.get_data()
    parsed_data = data["parsed_data"]
    total_sum = sum(item["sum"] for item in parsed_data["items"])
    items_list = "\n".join([f"- {item['name']} (Сумма: {item['sum']:.2f} RUB)" for item in parsed_data["items"]])
    if callback.data == "type_store":
        receipt_type = "Полный"
        delivery_date = ""
        status = "Доставлено"
        receipt = {
            "date": parsed_data["date"],
            "store": parsed_data.get("store", "Неизвестно"),
            "items": [{"name": item["name"], "sum": item["sum"]} for item in parsed_data["items"]],  # Положительная сумма
            "receipt_type": receipt_type,
            "fiscal_doc": parsed_data["fiscal_doc"],
            "qr_string": parsed_data["qr_string"],
            "delivery_date": delivery_date,
            "status": status,
            "customer": data["customer"]
        }
        details = (
            f"Детали чека:\n"
            f"Магазин: {receipt['store']}\n"
            f"Заказчик: {receipt['customer']}\n"
            f"Сумма: {total_sum:.2f} RUB\n"
            f"Товары:\n{items_list}\n"
            f"Фискальный номер: {parsed_data['fiscal_doc']}"
        )
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Подтвердить", callback_data="confirm_add")],
            [InlineKeyboardButton(text="Отменить", callback_data="cancel_add")]
        ])
        await callback.message.answer(details, reply_markup=keyboard)
        await state.update_data(receipt=receipt)
        await state.set_state(AddReceiptQR.CONFIRM_ACTION)
    elif callback.data == "type_delivery":
        receipt_type = "Предоплата"
        await state.update_data(receipt_type=receipt_type)
        items = parsed_data["items"]
        if len(items) == 1:
            await callback.message.answer(f"Введите дату доставки для {items[0]['name']} Используйте ддммгг (6 цифр, например 110825 для 11.08.2025) или /skip.")
            await state.set_state(AddReceiptQR.CONFIRM_DELIVERY_DATE)
        else:
            await state.update_data(current_item_index=0, delivery_dates=[])
            await callback.message.answer(f"Введите дату доставки для {items[0]['name']} Используйте ддммгг (6 цифр, например 110825 для 11.08.2025) или /skip.")
            await state.set_state(AddReceiptQR.CONFIRM_DELIVERY_DATE)
    await callback.answer()

@router.message(AddReceiptQR.CONFIRM_DELIVERY_DATE)
async def process_delivery_date(message: Message, state: FSMContext):
    data = await state.get_data()
    parsed_data = data["parsed_data"]
    receipt_type = data["receipt_type"]
    items = parsed_data["items"]
    current_item_index = data.get("current_item_index", 0)
    delivery_dates = data.get("delivery_dates", [])

    if message.text == "/skip":
        delivery_date = ""
    else:
        date_pattern = r"^\d{6}$"  # Проверяем, что введено ровно 6 цифр
        if not re.match(date_pattern, message.text):
            keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
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
            keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
            await message.answer("Неверный формат даты. Используйте ддммгг (6 цифр, например 110825 для 11.08.2025) или /skip.", reply_markup=keyboard)
            return

    delivery_dates.append(delivery_date)
    await state.update_data(delivery_dates=delivery_dates)

    if current_item_index + 1 < len(items):
        await state.update_data(current_item_index=current_item_index + 1)
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer(f"Введите дату доставки для {items[current_item_index + 1]['name']} (ддммгг, например 110825 для 11.08.2025) или /skip:", reply_markup=keyboard)
        return

    total_sum = sum(item["sum"] for item in items)
    items_list = "\n".join([f"- {item['name']} (Сумма: {item['sum']:.2f} RUB, Дата доставки: {delivery_dates[i] or 'Не указана'})" for i, item in enumerate(items)])
    receipt = {
        "date": parsed_data["date"],
        "store": parsed_data.get("store", "Неизвестно"),
        "items": [{"name": item["name"], "sum": item["sum"]} for item in parsed_data["items"]],
        "receipt_type": receipt_type,
        "fiscal_doc": parsed_data["fiscal_doc"],
        "qr_string": parsed_data["qr_string"],
        "delivery_dates": delivery_dates,
        "status": "Ожидает",
        "customer": data["customer"]
    }
    details = (
        f"Детали чека:\n"
        f"Магазин: {receipt['store']}\n"
        f"Заказчик: {receipt['customer']}\n"
        f"Сумма: {total_sum:.2f} RUB\n"
        f"Товары:\n{items_list}\n"
        f"Фискальный номер: {parsed_data['fiscal_doc']}"
    )
    inline_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Подтвердить", callback_data="confirm_add")],
        [InlineKeyboardButton(text="Отменить", callback_data="cancel_add")]
    ])
    reply_keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
    await message.answer(details, reply_markup=inline_keyboard)
    await message.answer("Или сбросьте действие:", reply_markup=reply_keyboard)
    await state.update_data(receipt=receipt)
    await state.set_state(AddReceiptQR.CONFIRM_ACTION)

@router.callback_query(AddReceiptQR.CONFIRM_ACTION, lambda c: c.data == "confirm_add")
async def confirm_add_action(callback, state: FSMContext):
    data = await state.get_data()
    receipt = data["receipt"]
    parsed_data = data["parsed_data"]
    username = callback.from_user.username or str(callback.from_user.id)
    delivery_dates = receipt.get("delivery_dates", [""] * len(receipt["items"]))

    is_delivery = receipt.get("receipt_type") == "Предоплата"
    receipt_type_for_save = "Доставка" if is_delivery else "Покупка"

    for i, item in enumerate(receipt["items"]):
        logger.info(f"Товар {i}: {item['name']}, дата: {delivery_dates[i]}")
        customer = receipt.get("customer", "Неизвестно")
        await save_receipt(
            parsed_data=parsed_data,
            username=username,
            user_id=callback.from_user.id,
            customer=customer,
            receipt_type=receipt_type_for_save,
            delivery_date=delivery_dates[i] if i < len(delivery_dates) else ""
        )

    await callback.message.answer(f"Чек с фискальным номером {receipt['fiscal_doc']} успешно добавлен.")
    await state.clear()
    await callback.answer()

@router.callback_query(AddReceiptQR.CONFIRM_ACTION, lambda c: c.data == "cancel_add")
async def cancel_add_action(callback, state: FSMContext):
    await callback.message.answer("Добавление чека отменено. Начать заново: /add")
    logger.info(f"Добавление чека отменено: user_id={callback.from_user.id}")
    await state.clear()
    await callback.answer()
    
@router.callback_query(ConfirmDelivery.SELECT_RECEIPT)
async def confirm_delivery_action(callback, state: FSMContext):
    try:
        fiscal_doc, index = callback.data.split("_", 1)
        index = int(index)
        data = await state.get_data()
        item_map = data.get("item_map", {})
        row_index, item_name = item_map.get(f"{fiscal_doc}_{index}", (None, None))
        if not row_index or not item_name:
            keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
            await callback.message.answer("Ошибка: товар не найден.", reply_markup=keyboard)
            logger.error(f"Товар не найден: fiscal_doc={fiscal_doc}, index={index}, user_id={callback.from_user.id}")
            await state.clear()
            await callback.answer()
            return
        await state.update_data(row_index=row_index, item_name=item_name, old_fiscal_doc=fiscal_doc)
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await callback.message.answer("Пожалуйста, отправьте QR-код чека полного расчета.", reply_markup=keyboard)
        await state.set_state(ConfirmDelivery.UPLOAD_FULL_QR)
        logger.info(f"Запрос QR-кода полного расчета: fiscal_doc={fiscal_doc}, item={item_name}, user_id={callback.from_user.id}")
        await callback.answer()
    except Exception as e:
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await callback.message.answer(f"Неожиданная ошибка: {str(e)}. Проверьте /debug.", reply_markup=keyboard)
        logger.error(f"Неожиданная ошибка подтверждения доставки: {str(e)}, user_id={callback.from_user.id}")
        await state.clear()
        await callback.answer()
        
@router.message(ConfirmDelivery.UPLOAD_FULL_QR)
async def process_full_qr_upload(message: Message, state: FSMContext, bot: Bot):
    if not message.photo:
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer("Пожалуйста, отправьте фото QR-кода чека полного расчета.", reply_markup=keyboard)
        logger.info(f"Фото отсутствует для QR полного расчета: user_id={message.from_user.id}")
        return
    parsed_data = await parse_qr_from_photo(bot, message.photo[-1].file_id)
    if not parsed_data:
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer("Ошибка обработки QR-кода. Убедитесь, что QR-код четкий.", reply_markup=keyboard)
        logger.error(f"Ошибка обработки QR-кода полного расчета: user_id={message.from_user.id}")
        return
    if parsed_data["operation_type"] != 1:
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer("Чек должен быть полным расчетом (operationType == 1).", reply_markup=keyboard)
        logger.info(f"Некорректный чек для полного расчета: operation_type={parsed_data['operation_type']}, user_id={message.from_user.id}")
        return
    data = await state.get_data()
    item_name = data.get("item_name")
    row_index = data.get("row_index")
    old_fiscal_doc = data.get("old_fiscal_doc")
    new_fiscal_doc = parsed_data["fiscal_doc"]
    if not await is_fiscal_doc_unique(new_fiscal_doc):
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer(f"Чек с фискальным номером {new_fiscal_doc} уже существует.", reply_markup=keyboard)
        logger.info(f"Дубликат фискального номера: new_fiscal_doc={new_fiscal_doc}, user_id={message.from_user.id}")
        return
    # Проверка совпадения товара
    item_found = False
    for item in parsed_data["items"]:
        if item["name"].lower() == item_name.lower():
            item_found = True
            break
    if not item_found:
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer(f"Товар {item_name} не найден в чеке полного расчета.", reply_markup=keyboard)
        logger.info(f"Товар не найден в чеке полного расчета: item={item_name}, fiscal_doc={new_fiscal_doc}, user_id={message.from_user.id}")
        return
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range=f"Чеки!A{row_index}:M{row_index}"
        ).execute()
        row = result.get("values", [[]])[0]
        if len(row) < 11:
            keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
            await message.answer("Ошибка: данные чека некорректны.", reply_markup=keyboard)
            logger.error(f"Некорректные данные чека: row_index={row_index}, user_id={message.from_user.id}")
            await state.clear()
            return
        # Обновляем данные: статус, тип чека, фискальный номер, QR-строка
        row[6] = "Доставлено"  # Статус
        row[9] = "Полный"     # Тип чека
        row[10] = new_fiscal_doc  # Новый фискальный номер
        row[11] = parsed_data["qr_string"]  # Новая QR-строка
        sheets_service.spreadsheets().values().update(
            spreadsheetId=SHEET_NAME,
            range=f"Чеки!A{row_index}:M{row_index}",
            valueInputOption="RAW",
            body={"values": [row]}
        ).execute()
        await message.answer(
            f"Доставка товара {item_name} подтверждена.\n"
            f"Фискальный номер обновлен на {new_fiscal_doc}.\n"
            f"Тип чека: Полный.",
            reply_markup=ReplyKeyboardRemove()
        )
        logger.info(f"Доставка подтверждена: old_fiscal_doc={old_fiscal_doc}, new_fiscal_doc={new_fiscal_doc}, item={item_name}, user_id={message.from_user.id}")
        await state.clear()
    except HttpError as e:
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer(f"Ошибка обновления данных в Google Sheets: {e.status_code} - {e.reason}. Проверьте /debug.", reply_markup=keyboard)
        logger.error(f"Ошибка подтверждения доставки: {e.status_code} - {e.reason}, user_id={message.from_user.id}")
        await state.clear()
    except Exception as e:
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer(f"Неожиданная ошибка: {str(e)}. Проверьте /debug.", reply_markup=keyboard)
        logger.error(f"Неожиданная ошибка подтверждения доставки: {str(e)}, user_id={message.from_user.id}")
        await state.clear()
        
@router.message(Command("expenses"))
async def list_pending_receipts(message: Message, state: FSMContext):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("Доступ запрещен.")
        logger.info(f"Доступ запрещен для /expenses: user_id={message.from_user.id}")
        return
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Чеки!A:M"
        ).execute()
        receipts = result.get("values", [])[1:]  # Пропускаем заголовок
        today = datetime.now().strftime("%d.%m.%Y")
        pending_receipts = []
        item_map = {}
        for i, row in enumerate(receipts, start=2):
            status = row[6].lower() if len(row) > 6 and row[6] else ""
            delivery_date = row[5] if len(row) > 5 and row[5] else ""
            if status == "ожидает":
                fiscal_doc = row[10] if len(row) > 10 and row[10] else ""
                item_name = row[8] if len(row) > 8 and row[8] else ""
                if fiscal_doc and item_name:  # Проверяем, что данные корректны
                    index = len(pending_receipts)
                    pending_receipts.append({
                        "fiscal_doc": fiscal_doc,
                        "item_name": item_name,
                        "delivery_date": delivery_date
                    })
                    item_map[f"{fiscal_doc}_{index}"] = (i, item_name)
        if not pending_receipts:
            keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
            await message.answer("Нет ожидающих доставки чеков.", reply_markup=keyboard)
            logger.info(f"Нет ожидающих чеков: user_id={message.from_user.id}")
            return
        await state.update_data(item_map=item_map)
        inline_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"{r['fiscal_doc']} - {r['item_name']}", callback_data=f"{r['fiscal_doc']}_{i}")]
            for i, r in enumerate(pending_receipts)
        ])
        reply_keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer("Выберите товар для подтверждения доставки:", reply_markup=inline_keyboard)
        await message.answer("Или сбросьте действие:", reply_markup=reply_keyboard)
        await state.set_state(ConfirmDelivery.SELECT_RECEIPT)
        logger.info(f"Список ожидающих чеков выведен: user_id={message.from_user.id}")
    except HttpError as e:
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer(f"Ошибка получения данных из Google Sheets: {e.status_code} - {e.reason}. Проверьте /debug.", reply_markup=keyboard)
        logger.error(f"Ошибка /expenses: {e.status_code} - {e.reason}, user_id={message.from_user.id}")
    except Exception as e:
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer(f"Неожиданная ошибка: {str(e)}. Проверьте /debug.", reply_markup=keyboard)
        logger.error(f"Неожиданная ошибка /expenses: {str(e)}, user_id={message.from_user.id}")

@router.message(Command("return"))
async def return_receipt(message: Message, state: FSMContext):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("Доступ запрещен.")
        logger.info(f"Доступ запрещен для /return: user_id={message.from_user.id}")
        return
    try:
        fiscal_doc = message.text.split()[1]
        if not fiscal_doc.isdigit() or len(fiscal_doc) > 20:
            await message.answer("Фискальный номер должен содержать только цифры и быть не длиннее 20 символов.")
            logger.info(f"Некорректный фискальный номер для /return: {fiscal_doc}, user_id={message.from_user.id}")
            return
    except IndexError:
        await message.answer("Укажите фискальный номер: /return [fiscal_doc]")
        logger.info(f"Фискальный номер не указан для /return: user_id={message.from_user.id}")
        return
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Чеки!A:M"
        ).execute()
        receipts = [row for row in result.get("values", [])[1:] if len(row) > 10 and row[10] == fiscal_doc and row[6] != "Возвращен"]
        if not receipts:
            await message.answer("Чеки не найдены или уже возвращены.")
            logger.info(f"Чеки не найдены для /return: fiscal_doc={fiscal_doc}, user_id={message.from_user.id}")
            return
        item_map = {}
        for i, row in enumerate(receipts):
            item_map[i] = row[8] if len(row) > 8 else "Неизвестно"
        await state.update_data(fiscal_doc=fiscal_doc, item_map=item_map)
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=row[8] if len(row) > 8 else "Неизвестно", callback_data=f"товар_{fiscal_doc}_{i}")]
            for i, row in enumerate(receipts)
        ])
        await message.answer("Выберите товар для возврата:", reply_markup=keyboard)
        await state.set_state(ReturnReceipt.SELECT_ITEM)
        logger.info(f"Чек для возврата найден: fiscal_doc={fiscal_doc}, user_id={message.from_user.id}")
    except HttpError as e:
        await message.answer(f"Ошибка получения данных из Google Sheets: {e.status_code} - {e.reason}. Проверьте /debug.")
        logger.error(f"Ошибка /return: {e.status_code} - {e.reason}, user_id={message.from_user.id}")
    except Exception as e:
        await message.answer(f"Неожиданная ошибка: {str(e)}. Проверьте /debug.")
        logger.error(f"Неожиданная ошибка /return: {str(e)}, user_id={message.from_user.id}")

@router.callback_query(ReturnReceipt.SELECT_ITEM)
async def process_return_item(callback, state: FSMContext):
    try:
        _, fiscal_doc, index = callback.data.split("_")
        index = int(index)
        data = await state.get_data()
        item_name = data["item_map"].get(index, "")
        if not item_name:
            await callback.message.answer("Ошибка: товар не найден.")
            logger.error(f"Товар не найден в item_map: index={index}, user_id={callback.from_user.id}")
            await state.clear()
            await callback.answer()
            return
        await state.update_data(fiscal_doc=fiscal_doc, item_name=item_name)
        await callback.message.answer("Отправьте QR-код чека возврата.")
        await state.set_state(ReturnReceipt.UPLOAD_RETURN_QR)
        await callback.answer()
        logger.info(f"Товар для возврата выбран: fiscal_doc={fiscal_doc}, item={item_name}, user_id={callback.from_user.id}")
    except ValueError:
        await callback.message.answer("Ошибка выбора товара.")
        logger.error(f"Ошибка выбора товара: callback_data={callback.data}, user_id={callback.from_user.id}")
        await state.clear()
        await callback.answer()

@router.message(ReturnReceipt.UPLOAD_RETURN_QR)
async def process_return_qr(message: Message, state: FSMContext, bot: Bot):
    if not message.photo:
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer("Пожалуйста, отправьте фото QR-кода.", reply_markup=keyboard)
        logger.info(f"Фото отсутствует для возврата: user_id={message.from_user.id}")
        return
    parsed_data = await parse_qr_from_photo(bot, message.photo[-1].file_id)
    if not parsed_data:
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer("Ошибка обработки QR-кода. Убедитесь, что QR-код четкий.", reply_markup=keyboard)
        logger.info(f"Ошибка обработки QR-кода для возврата: user_id={message.from_user.id}")
        return
    if parsed_data["operation_type"] != 2:
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer("Чек должен быть возвратом (operationType == 2).", reply_markup=keyboard)
        logger.info(f"Некорректный чек для возврата: operation_type={parsed_data['operation_type']}, user_id={message.from_user.id}")
        return
    new_fiscal_doc = parsed_data["fiscal_doc"]
    if not await is_fiscal_doc_unique(new_fiscal_doc):
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer(f"Чек с фискальным номером {new_fiscal_doc} уже существует.", reply_markup=keyboard)
        logger.info(f"Дубликат фискального номера: new_fiscal_doc={new_fiscal_doc}, user_id={message.from_user.id}")
        return
    data = await state.get_data()
    fiscal_doc = data["fiscal_doc"]
    item_name = data["item_name"]
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Чеки!A:M"
        ).execute()
        rows = result.get("values", [])[1:]
        row_updated = False
        for i, row in enumerate(rows, start=2):
            if len(row) > 10 and row[10] == fiscal_doc and row[8] == item_name:
                while len(row) < 13:
                    row.append("")
                row[6] = "Возвращен"  # Меняем статус на "Возвращен"
                row[12] = parsed_data["qr_string"]  # Добавляем QR-строку возврата
                sheets_service.spreadsheets().values().update(
                    spreadsheetId=SHEET_NAME,
                    range=f"Чеки!A{i}:M{i}",
                    valueInputOption="RAW",
                    body={"values": [row]}
                ).execute()
                row_updated = True
                total_sum = float(row[2]) if row[2] else 0.0  # Используем сумму из оригинальной строки
                note = f"{new_fiscal_doc} - {item_name}"
                # Записываем возврат в "Сводка"
                await save_receipt_summary(parsed_data["date"], "Возврат", total_sum, note)
                details = (
                    f"Магазин: {row[4]}\n"
                    f"Заказчик: {row[7] if len(row) > 7 else 'Неизвестно'}\n"
                    f"Сумма: {total_sum:.2f} RUB\n"
                    f"Товар: {item_name}\n"
                    f"Новый фискальный номер: {new_fiscal_doc}"
                )
                inline_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="Подтвердить", callback_data="confirm_return")]
                ])
                reply_keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
                await message.answer(f"Возврат товара {item_name} обработан. Детали:\n{details}\nПодтвердите действие кнопкой ниже:", reply_markup=inline_keyboard)
                await message.answer("Или сбросьте действие:", reply_markup=reply_keyboard)
                await state.update_data(new_fiscal_doc=new_fiscal_doc)
                await state.set_state(ReturnReceipt.CONFIRM_ACTION)
                logger.info(f"Возврат обработан: old_fiscal_doc={fiscal_doc}, new_fiscal_doc={new_fiscal_doc}, item={item_name}, user_id={message.from_user.id}")
                return
        if not row_updated:
            keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
            await message.answer("Товар возврата не найден в оригинальном чеке.", reply_markup=keyboard)
            logger.info(f"Товар не найден для возврата: fiscal_doc={fiscal_doc}, item={item_name}, user_id={message.from_user.id}")
    except HttpError as e:
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer(f"Ошибка обновления данных в Google Sheets: {e.status_code} - {e.reason}. Проверьте /debug.", reply_markup=keyboard)
        logger.error(f"Ошибка обработки возврата: {e.status_code} - {e.reason}, user_id={message.from_user.id}")
    except Exception as e:
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
        await message.answer(f"Неожиданная ошибка: {str(e)}. Проверьте /debug.", reply_markup=keyboard)
        logger.error(f"Неожиданная ошибка обработки возврата: {str(e)}, user_id={message.from_user.id}")

@router.callback_query(ReturnReceipt.CONFIRM_ACTION)
async def confirm_return_action(callback, state: FSMContext):
    data = await state.get_data()
    new_fiscal_doc = data["new_fiscal_doc"]
    await callback.message.answer(f"Возврат подтвержден с новым фискальным номером {new_fiscal_doc}.")
    logger.info(f"Возврат подтвержден пользователем: new_fiscal_doc={new_fiscal_doc}, user_id={callback.from_user.id}")
    await state.clear()
    await callback.answer()

@router.message(Command("balance"))
async def get_balance(message: Message):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("Доступ запрещен.")
        logger.info(f"Доступ запрещен для /balance: user_id={message.from_user.id}")
        return
    try:
        balance_data = await get_monthly_balance()
        if balance_data:
            await message.answer(
                f"Текущий баланс:\n"
                f"Потрачено: {abs(balance_data['spent']):.2f} RUB\n"
                f"Возвращено: {balance_data['returned']:.2f} RUB\n"
                f"Обновление баланса: {balance_data['balance_update']:.2f} RUB\n"
                f"Остаток: {balance_data['balance']:.2f} RUB"
            )
            logger.info(f"Баланс выдан: spent={abs(balance_data['spent'])}, returned={balance_data['returned']}, balance={balance_data['balance']}, user_id={message.from_user.id}")
        else:
            await message.answer("Ошибка получения данных о балансе.")
            logger.error(f"Ошибка получения баланса: user_id={message.from_user.id}")
    except Exception as e:
        await message.answer(f"Неожиданная ошибка: {str(e)}. Проверьте /debug.")
        logger.error(f"Неожиданная ошибка /balance: {str(e)}, user_id={message.from_user.id}")