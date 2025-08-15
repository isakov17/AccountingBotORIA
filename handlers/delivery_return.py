from config import SHEET_NAME, PROVERKACHEKA_TOKEN
from aiogram import Router, Bot
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, CallbackQuery
from sheets import sheets_service, is_user_allowed, is_fiscal_doc_unique, save_receipt, get_monthly_balance, save_receipt_summary
from utils import parse_qr_from_photo
from googleapiclient.errors import HttpError
import logging
from datetime import datetime
import re
import asyncio

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
    ENTER_FISCAL_DOC = State()
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
    loading_message = await message.answer("⌛ Обработка запроса... Пожалуйста, подождите.")
    await state.update_data(parsed_data=parsed_data)
    keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)
    await message.answer("Введите заказчика (или /skip):", reply_markup=keyboard)
    await state.set_state(AddReceiptQR.CUSTOMER)
    await loading_message.edit_text("QR-код обработан.")
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
async def confirm_add_action(callback: CallbackQuery, state: FSMContext):
    loading_message = await callback.message.answer("⌛ Обработка запроса... Пожалуйста, подождите.")
    data = await state.get_data()
    receipt = data["receipt"]
    parsed_data = data["parsed_data"]  # нужен для даты/магазина/qr/fiscalDoc/excluded_sum/excluded_items
    username = callback.from_user.username or str(callback.from_user.id)
    delivery_dates = receipt.get("delivery_dates", [])

    is_delivery = receipt.get("receipt_type") == "Предоплата"
    receipt_type_for_save = "Доставка" if is_delivery else "Покупка"

    ok, fail = 0, 0
    for i, item in enumerate(receipt["items"]):
        one = {
            "date": parsed_data["date"],
            "store": parsed_data.get("store", "Неизвестно"),
            "items": [ {"name": item["name"], "sum": item["sum"]} ],
            "receipt_type": "Полный" if not is_delivery else "Доставка",
            "fiscal_doc": parsed_data["fiscal_doc"],
            "qr_string": parsed_data["qr_string"],
            "delivery_date": delivery_dates[i] if i < len(delivery_dates) else "",
            "status": "Ожидает" if is_delivery else "Доставлено",
            "customer": receipt.get("customer", data.get("customer", "Неизвестно")),
        }

        saved = await save_receipt(one, username, callback.from_user.id, receipt_type=receipt_type_for_save)
        if saved:
            ok += 1
        else:
            fail += 1

    # Добавление записи для excluded_sum в Сводка как расход на услуги
    excluded_sum = parsed_data.get("excluded_sum", 0.0)
    if excluded_sum > 0:
        excluded_items_list = parsed_data.get("excluded_items", [])
        note = f"{parsed_data['fiscal_doc']} - Услуги ({', '.join(excluded_items_list)})"
        await save_receipt_summary(parsed_data["date"], "Услуга", excluded_sum, note)  # Расход, поэтому сумма положительная в расходах
        logger.info(f"Учёт услуг в Сводка: сумма={excluded_sum}, note={note}, user_id={callback.from_user.id}")

    # Редактируем сообщение загрузки на результат
    if ok and not fail:
        await loading_message.edit_text(f"Чек {receipt['fiscal_doc']} добавлен. Позиции: {ok}/{ok}. Услуги учтены в балансе.")
    elif ok and fail:
        await loading_message.edit_text(f"Чек {receipt['fiscal_doc']} добавлен частично. Удалось: {ok}, ошибок: {fail}. Смотри /debug для деталей. Услуги учтены в балансе.")
    else:
        await loading_message.edit_text(f"Не удалось сохранить чек {receipt['fiscal_doc']}. Попробуй ещё раз или /add_manual.")

    logger.info(
        "Чек подтвержден: fiscal_doc=%s, saved=%d, failed=%d, excluded_sum=%f, user_id=%s",
        receipt['fiscal_doc'], ok, fail, excluded_sum, callback.from_user.id
    )
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
    # Отправляем сообщение о загрузке
    loading_message = await message.answer("⌛ Обработка запроса... Пожалуйста, подождите.")

    if not message.photo:
        await loading_message.edit_text("Пожалуйста, отправьте фото QR-кода чека полного расчета.", reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True))
        logger.info(f"Фото отсутствует для QR полного расчета: user_id={message.from_user.id}")
        return

    parsed_data = await parse_qr_from_photo(bot, message.photo[-1].file_id)
    if not parsed_data:
        await loading_message.edit_text("Ошибка обработки QR-кода. Убедитесь, что QR-код четкий.", reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True))
        logger.error(f"Ошибка обработки QR-кода полного расчета: user_id={message.from_user.id}")
        return

    if parsed_data["operation_type"] != 1:
        await loading_message.edit_text("Чек должен быть полным расчетом (operationType == 1).", reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True))
        logger.info(f"Некорректный чек для полного расчета: operation_type={parsed_data['operation_type']}, user_id={message.from_user.id}")
        return

    data = await state.get_data()
    item_name = data.get("item_name")
    row_index = data.get("row_index")
    old_fiscal_doc = data.get("old_fiscal_doc")
    new_fiscal_doc = parsed_data["fiscal_doc"]
    if not await is_fiscal_doc_unique(new_fiscal_doc):
        await loading_message.edit_text(f"Чек с фискальным номером {new_fiscal_doc} уже существует.", reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True))
        logger.info(f"Дубликат фискального номера: new_fiscal_doc={new_fiscal_doc}, user_id={message.from_user.id}")
        return

    # Проверка совпадения товара
    item_found = False
    for item in parsed_data["items"]:
        if item["name"].lower() == item_name.lower():
            item_found = True
            break
    if not item_found:
        await loading_message.edit_text(f"Товар {item_name} не найден в чеке полного расчета.", reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True))
        logger.info(f"Товар не найден в чеке полного расчета: item={item_name}, fiscal_doc={new_fiscal_doc}, user_id={message.from_user.id}")
        return

    # Сохраняем данные для последующего подтверждения
    details = (
        f"Магазин: {data.get('store', 'Неизвестно')}\n"
        f"Заказчик: {data.get('customer', 'Неизвестно')}\n"
        f"Сумма: {parsed_data.get('total_sum', 0.0) / 100:.2f} RUB\n"  # Предполагаем, что сумма в копейках
        f"Товар: {item_name}\n"
        f"Новый фискальный номер: {new_fiscal_doc}"
    )
    inline_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Подтвердить", callback_data="confirm_delivery")],
        [InlineKeyboardButton(text="Отмена", callback_data="cancel_delivery")]
    ])
    await loading_message.edit_text(f"Доставка товара {item_name} обработана. Детали:\n{details}\nПодтвердите или отмените действие:", reply_markup=inline_keyboard)
    await state.update_data(
        new_fiscal_doc=new_fiscal_doc,
        parsed_data=parsed_data,
        row_index=row_index,
        item_name=item_name,
        old_fiscal_doc=old_fiscal_doc
    )
    await state.set_state(ConfirmDelivery.CONFIRM_ACTION)
    logger.info(f"Доставка подготовлена к подтверждению: old_fiscal_doc={old_fiscal_doc}, new_fiscal_doc={new_fiscal_doc}, item={item_name}, user_id={message.from_user.id}")

# Обработчик подтверждения/отмены доставки
@router.callback_query(ConfirmDelivery.CONFIRM_ACTION, lambda c: c.data in ["confirm_delivery", "cancel_delivery"])
async def handle_delivery_confirmation(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    old_fiscal_doc = data.get("old_fiscal_doc")
    new_fiscal_doc = data.get("new_fiscal_doc")
    item_name = data.get("item_name")
    row_index = data.get("row_index")
    parsed_data = data.get("parsed_data")

    if callback.data == "confirm_delivery":
        try:
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=SHEET_NAME, range=f"Чеки!A{row_index}:M{row_index}"
            ).execute()
            row = result.get("values", [[]])[0]
            if len(row) < 11:
                await callback.message.edit_text("Ошибка: данные чека некорректны.")
                logger.error(f"Некорректные данные чека: row_index={row_index}, user_id={callback.from_user.id}")
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
            await callback.message.edit_text(
                f"Доставка товара {item_name} подтверждена.\n"
                f"Фискальный номер обновлен на {new_fiscal_doc}.\n"
                f"Тип чека: Полный."
            )
            logger.info(f"Доставка подтверждена: old_fiscal_doc={old_fiscal_doc}, new_fiscal_doc={new_fiscal_doc}, item={item_name}, user_id={callback.from_user.id}")
        except HttpError as e:
            await callback.message.edit_text(f"Ошибка обновления данных в Google Sheets: {e.status_code} - {e.reason}. Проверьте /debug.")
            logger.error(f"Ошибка подтверждения доставки: {e.status_code} - {e.reason}, user_id={callback.from_user.id}")
        except Exception as e:
            await callback.message.edit_text(f"Неожиданная ошибка: {str(e)}. Проверьте /debug.")
            logger.error(f"Неожиданная ошибка подтверждения доставки: {str(e)}, user_id={callback.from_user.id}")
    elif callback.data == "cancel_delivery":
        await callback.message.edit_text(f"Доставка товара {item_name} отменена. Фискальный номер: {new_fiscal_doc} не сохранен.")
        logger.info(f"Доставка отменена: old_fiscal_doc={old_fiscal_doc}, new_fiscal_doc={new_fiscal_doc}, item={item_name}, user_id={callback.from_user.id}")

    await state.clear()
    await callback.answer()
        
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
    
    # Запрашиваем у пользователя фискальный номер
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Сброс")]],
        resize_keyboard=True
    )
    await message.answer("Пожалуйста, введите фискальный номер чека для возврата:", reply_markup=keyboard)
    await state.set_state(ReturnReceipt.ENTER_FISCAL_DOC)
    logger.info(f"Запрос фискального номера для /return: user_id={message.from_user.id}")

@router.message(ReturnReceipt.ENTER_FISCAL_DOC)
async def process_fiscal_doc(message: Message, state: FSMContext):
    fiscal_doc = message.text.strip()
    if fiscal_doc == "Сброс":
        await message.answer("Действие сброшено.", reply_markup=ReplyKeyboardRemove())
        await state.clear()
        logger.info(f"Сброс действия для /return: user_id={message.from_user.id}")
        return

    if not fiscal_doc.isdigit() or len(fiscal_doc) > 20:
        keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Сброс")]],
            resize_keyboard=True
        )
        await message.answer("Фискальный номер должен содержать только цифры и быть не длиннее 20 символов.", reply_markup=keyboard)
        logger.info(f"Некорректный фискальный номер для /return: {fiscal_doc}, user_id={message.from_user.id}")
        return

    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Чеки!A:M"
        ).execute()
        receipts = [row for row in result.get("values", [])[1:] if len(row) > 10 and row[10] == fiscal_doc and row[6] != "Возвращен"]
        if not receipts:
            keyboard = ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="Сброс")]],
                resize_keyboard=True
            )
            await message.answer("Чеки не найдены или уже возвращены.", reply_markup=keyboard)
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
        keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Сброс")]],
            resize_keyboard=True
        )
        await message.answer(f"Ошибка получения данных из Google Sheets: {e.status_code} - {e.reason}. Проверьте /debug.", reply_markup=keyboard)
        logger.error(f"Ошибка /return: {e.status_code} - {e.reason}, user_id={message.from_user.id}")
    except Exception as e:
        keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Сброс")]],
            resize_keyboard=True
        )
        await message.answer(f"Неожиданная ошибка: {str(e)}. Проверьте /debug.", reply_markup=keyboard)
        logger.error(f"Неожиданная ошибка /return: {str(e)}, user_id={message.from_user.id}")

# (Дополнительные обработчики, такие как SELECT_ITEM, можно оставить без изменений, если они уже определены)

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
    # Отправляем сообщение о загрузке
    loading_message = await message.answer("⌛ Обработка запроса... Пожалуйста, подождите.")

    if not message.photo:
        await loading_message.edit_text("Пожалуйста, отправьте фото QR-кода.", reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True))
        logger.info(f"Фото отсутствует для возврата: user_id={message.from_user.id}")
        return

    parsed_data = await parse_qr_from_photo(bot, message.photo[-1].file_id)
    if not parsed_data:
        await loading_message.edit_text("Ошибка обработки QR-кода. Убедитесь, что QR-код четкий.", reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True))
        logger.info(f"Ошибка обработки QR-кода для возврата: user_id={message.from_user.id}")
        return

    if parsed_data["operation_type"] != 2:
        await loading_message.edit_text("Чек должен быть возвратом (operationType == 2).", reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True))
        logger.info(f"Некорректный чек для возврата: operation_type={parsed_data['operation_type']}, user_id={message.from_user.id}")
        return

    # === Новый блок: проверяем, что в чеке возврата реально есть нужный товар ===
    data = await state.get_data()
    expected_item = (data or {}).get("item_name", "")

    def norm(s: str) -> str:
        s = (s or "").lower()
        s = " ".join(s.split())
        return s

    tgt = norm(expected_item)
    found_match = False
    for it in parsed_data.get("items", []):
        name = norm(it.get("name", ""))
        # строгая проверка + «мягкая» (на случай различий артикулов/хвостов)
        if name == tgt or (tgt and (tgt in name or name in tgt)):
            found_match = True
            break

    if not found_match:
        await loading_message.edit_text(f"Товар «{expected_item}» не найден в чеке возврата.", reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True))
        logger.info(
            "Товар не найден в чеке возврата: need=%s, got_items=%s, user_id=%s",
            expected_item,
            [x.get('name') for x in parsed_data.get('items', [])],
            message.from_user.id
        )
        return
    # === конец нового блока ===

    new_fiscal_doc = parsed_data["fiscal_doc"]
    if not await is_fiscal_doc_unique(new_fiscal_doc):
        await loading_message.edit_text(f"Чек с фискальным номером {new_fiscal_doc} уже существует.", reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True))
        logger.info(f"Дубликат фискального номера: new_fiscal_doc={new_fiscal_doc}, user_id={message.from_user.id}")
        return

    # Сохраняем данные для последующего подтверждения
    data = await state.get_data()
    fiscal_doc = data["fiscal_doc"]
    item_name = data["item_name"]
    total_sum = 0.0  # Будет обновлено при подтверждении
    details = (
        f"Магазин: {data.get('store', 'Неизвестно')}\n"
        f"Заказчик: {data.get('customer', 'Неизвестно')}\n"
        f"Сумма: {total_sum:.2f} RUB\n"
        f"Товар: {item_name}\n"
        f"Новый фискальный номер: {new_fiscal_doc}"
    )
    inline_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Подтвердить", callback_data="confirm_return")],
        [InlineKeyboardButton(text="Отмена", callback_data="cancel_return")]
    ])
    await loading_message.edit_text(f"Возврат товара {item_name} обработан. Детали:\n{details}\nПодтвердите или отмените действие:", reply_markup=inline_keyboard)
    await state.update_data(
        new_fiscal_doc=new_fiscal_doc,
        parsed_data=parsed_data,
        fiscal_doc=fiscal_doc,
        item_name=item_name
    )
    await state.set_state(ReturnReceipt.CONFIRM_ACTION)
    logger.info(f"Возврат подготовлен к подтверждению: old_fiscal_doc={fiscal_doc}, new_fiscal_doc={new_fiscal_doc}, item={item_name}, user_id={message.from_user.id}")

# Обработчик подтверждения/отмены возврата
@router.callback_query(ReturnReceipt.CONFIRM_ACTION, lambda c: c.data in ["confirm_return", "cancel_return"])
async def handle_return_confirmation(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    fiscal_doc = data.get("fiscal_doc")
    new_fiscal_doc = data.get("new_fiscal_doc")
    item_name = data.get("item_name")
    parsed_data = data.get("parsed_data")

    if callback.data == "confirm_return":
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
                    await callback.message.edit_text(f"Возврат товара {item_name} подтвержден. Фискальный номер: {new_fiscal_doc}")
                    logger.info(f"Возврат подтвержден: old_fiscal_doc={fiscal_doc}, new_fiscal_doc={new_fiscal_doc}, item={item_name}, user_id={callback.from_user.id}")
                    break
            if not row_updated:
                await callback.message.edit_text(f"Товар {item_name} не найден для подтверждения возврата.")
                logger.info(f"Товар не найден для возврата: fiscal_doc={fiscal_doc}, item={item_name}, user_id={callback.from_user.id}")
        except HttpError as e:
            await callback.message.edit_text(f"Ошибка обновления данных в Google Sheets: {e.status_code} - {e.reason}. Проверьте /debug.")
            logger.error(f"Ошибка обработки возврата: {e.status_code} - {e.reason}, user_id={callback.from_user.id}")
        except Exception as e:
            await callback.message.edit_text(f"Неожиданная ошибка: {str(e)}. Проверьте /debug.")
            logger.error(f"Неожиданная ошибка обработки возврата: {str(e)}, user_id={callback.from_user.id}")
    elif callback.data == "cancel_return":
        await callback.message.edit_text(f"Возврат товара {item_name} отменен. Фискальный номер: {new_fiscal_doc} не сохранен.")
        logger.info(f"Возврат отменен: old_fiscal_doc={fiscal_doc}, new_fiscal_doc={new_fiscal_doc}, item={item_name}, user_id={callback.from_user.id}")

    await state.clear()
    await callback.answer()



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
        await message.answer("🚫 Доступ запрещен.")
        logger.info(f"Доступ запрещен для /balance: user_id={message.from_user.id}")
        return

    loading_message = await message.answer("⌛ Обработка запроса... Пожалуйста, подождите.")
    try:
        balance_data = await get_monthly_balance()
        if balance_data:
            initial_balance = balance_data.get("initial_balance", 0.0)
            spent = abs(balance_data.get("spent", 0.0))
            returned = balance_data.get("returned", 0.0)
            balance = balance_data.get("balance", 0.0)

            # Получаем дату обновления из A1 (опционально)
            try:
                date_result = sheets_service.spreadsheets().values().get(
                    spreadsheetId=SHEET_NAME, range="Сводка!A1"
                ).execute()
                update_date = date_result.get("values", [[datetime.now().strftime("%d.%m.%Y")]])[0][0]
            except Exception:
                update_date = datetime.now().strftime("%d.%m.%Y")
                logger.warning("Не удалось получить дату обновления из A1, используется текущая дата")

            await loading_message.edit_text(
                f"💸 Текущий баланс:\n"
                f"💰 Начальный баланс: {initial_balance:.2f} RUB\n"
                f"➖ Потрачено: {spent:.2f} RUB\n"
                f"➕ Возвращено: {returned:.2f} RUB\n"
                f"🟰 Остаток: {balance:.2f} RUB",
                parse_mode="Markdown"
            )
            logger.info(
                f"Баланс выдан: initial_balance={initial_balance}, spent={spent}, returned={returned}, balance={balance}, user_id={message.from_user.id}"
            )
        else:
            await loading_message.edit_text("❌ Ошибка получения данных о балансе.")
            logger.error(f"Ошибка получения баланса: user_id={message.from_user.id}")
    except Exception as e:
        await loading_message.edit_text(f"❌ Неожиданная ошибка: {str(e)}. Проверьте /debug.")
        logger.error(f"Неожиданная ошибка /balance: {str(e)}, user_id={message.from_user.id}")
