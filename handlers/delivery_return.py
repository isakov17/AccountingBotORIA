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
    SELECT_RECEIPT = State()   # выбор чека (по fiscal_doc)
    SELECT_ITEMS = State()     # мультивыбор позиций в чеке
    UPLOAD_FULL_QR = State()   # загрузка QR полного расчёта
    CONFIRM_ACTION = State()   # финальное подтверждение
class ReturnReceipt(StatesGroup):
    ENTER_FISCAL_DOC = State()
    SELECT_ITEM = State()
    UPLOAD_RETURN_QR = State()
    CONFIRM_ACTION = State()

# 🔽 ДОБАВЬ К ИМПОРТАМ ВВЕРХУ ФАЙЛА
from aiogram import F
from aiogram.filters import StateFilter
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

# 🔽 ГЛОБАЛЬНЫЙ ПЕРЕХВАТ ФОТО QR, ЕСЛИ ПОЛЬЗОВАТЕЛЬ НЕ ВОШЕЛ В /add
@router.message(StateFilter(None), F.photo)
async def catch_qr_photo_without_command(message: Message, state: FSMContext, bot: Bot):
    """
    Пользователь прислал фото с QR не заходя в /add — запускаем тот же флоу,
    что и @router.message(AddReceiptQR.UPLOAD_QR).
    """
    # Проверяем доступ
    if not await is_user_allowed(message.from_user.id):
        await message.answer("Доступ запрещен.")
        logger.info(f"Доступ запрещен для авто-обработки QR: user_id={message.from_user.id}")
        return

    # Пробуем распарсить QR
    parsed_data = await parse_qr_from_photo(bot, message.photo[-1].file_id)
    keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сброс")]], resize_keyboard=True)

    if not parsed_data:
        await message.answer(
            "Ошибка обработки QR-кода. Убедитесь, что QR-код четкий, "
            "или используйте /add для ручного старта.",
            reply_markup=keyboard
        )
        logger.error(f"Авто-QR: распознавание не удалось, user_id={message.from_user.id}")
        # На всякий случай чистим состояние (вдруг было что-то «битое»)
        await state.clear()
        return

    # Проверяем уникальность фискального номера (точно как в обычном /add)
    # см. process_qr_upload: проверка is_fiscal_doc_unique и ветвление ответов
    if not await is_fiscal_doc_unique(parsed_data["fiscal_doc"]):
        await message.answer(
            f"Чек с фискальным номером {parsed_data['fiscal_doc']} уже существует.",
            reply_markup=keyboard
        )
        logger.info(f"Авто-QR: дубликат фискального номера: {parsed_data['fiscal_doc']}, user_id={message.from_user.id}")
        await state.clear()
        return

    # Сохраняем данные и переводим пользователя в тот же шаг, что и после /add -> UPLOAD_QR
    await state.update_data(
        username=message.from_user.username or str(message.from_user.id),
        parsed_data=parsed_data
    )

    # Дальше — в точности как в твоём process_qr_upload: спрашиваем заказчика и ставим состояние CUSTOMER
    await message.answer("Введите заказчика (или /skip):", reply_markup=keyboard)
    await state.set_state(AddReceiptQR.CUSTOMER)

    logger.info(
        "Авто-старт /add по фото QR: fiscal_doc=%s, user_id=%s",
        parsed_data['fiscal_doc'], message.from_user.id
    )


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
    parsed_data = data["parsed_data"]
    user_name = await is_user_allowed(callback.from_user.id)
    if not user_name:
        await loading_message.edit_text("🚫 Доступ запрещен.")
        logger.info(f"Доступ запрещен для confirm_add: user_id={callback.from_user.id}")
        await state.clear()
        await callback.answer()
        return
    delivery_dates = receipt.get("delivery_dates", [])

    is_delivery = receipt.get("receipt_type") == "Предоплата"
    receipt_type_for_save = "Доставка" if is_delivery else "Покупка"

    ok, fail = 0, 0
    for i, item in enumerate(receipt["items"]):
        one = {
            "date": parsed_data["date"],
            "store": parsed_data.get("store", "Неизвестно"),
            "items": [{"name": item["name"], "sum": item["sum"]}],
            "receipt_type": "Полный" if not is_delivery else "Доставка",
            "fiscal_doc": parsed_data["fiscal_doc"],
            "qr_string": parsed_data["qr_string"],
            "delivery_date": delivery_dates[i] if i < len(delivery_dates) else "",
            "status": "Ожидает" if is_delivery else "Доставлено",
            "customer": receipt.get("customer", data.get("customer", "Неизвестно")),
        }

        saved = await save_receipt(one, user_name, callback.from_user.id, receipt_type=receipt_type_for_save)
        if saved:
            ok += 1
        else:
            fail += 1

    # Добавление записи для excluded_sum в Сводка как расход на услуги
    excluded_sum = parsed_data.get("excluded_sum", 0.0)
    if excluded_sum > 0:
        excluded_items_list = parsed_data.get("excluded_items", [])
        note = f"{parsed_data['fiscal_doc']} - Услуги ({', '.join(excluded_items_list)})"
        await save_receipt_summary(parsed_data["date"], "Услуга", excluded_sum, note)
        logger.info(f"Учёт услуг в Сводка: сумма={excluded_sum}, note={note}, user_id={callback.from_user.id}")

    # Получаем текущий баланс
    balance_data = await get_monthly_balance()
    balance = balance_data.get("balance", 0.0) if balance_data else 0.0

    # Редактируем сообщение загрузки на результат
    if ok and not fail:
        await loading_message.edit_text(
            f"✅ Чек {receipt['fiscal_doc']} добавлен (пользователь: {user_name}).\n"
            f"Позиции: {ok}/{ok}. Услуги учтены в балансе.\n"
            f"🟰 Текущий остаток: {balance:.2f} RUB",
            parse_mode="Markdown"
        )
    elif ok and fail:
        await loading_message.edit_text(
            f"⚠️ Чек {receipt['fiscal_doc']} добавлен частично (пользователь: {user_name}).\n"
            f"Удалось: {ok}, ошибок: {fail}. Смотри /debug для деталей. Услуги учтены в балансе.\n"
            f"🟰 Текущий остаток: {balance:.2f} RUB",
            parse_mode="Markdown"
        )
    else:
        await loading_message.edit_text(
            f"❌ Не удалось сохранить чек {receipt['fiscal_doc']}. Попробуй ещё раз или /add_manual."
        )

    logger.info(
        f"Чек подтвержден: fiscal_doc={receipt['fiscal_doc']}, saved={ok}, failed={fail}, excluded_sum={excluded_sum}, balance={balance}, user_id={callback.from_user.id}, user_name={user_name}"
    )
    await state.clear()
    await callback.answer()


@router.callback_query(AddReceiptQR.CONFIRM_ACTION, lambda c: c.data == "cancel_add")
async def cancel_add_action(callback, state: FSMContext):
    await callback.message.answer("Добавление чека отменено. Начать заново: /add")
    logger.info(f"Добавление чека отменено: user_id={callback.from_user.id}")
    await state.clear()
    await callback.answer()
    
        

# === МУЛЬТИВЫБОР ПОДТВЕРЖДЕНИЯ ДОСТАВКИ /expenses ===
from aiogram import F

# Состояния потока подтверждения доставки (мультивыбор)

def _norm_name(s: str) -> str:
    s = (s or "").lower().strip()
    return " ".join(s.split())

def _rub(val) -> float:
    """
    Аккуратно приводим сумму из QR к рублям.
    Поддерживаем: sum (в рублях/копейках) или price*quantity.
    """
    if val is None:
        return 0.0
    try:
        v = float(val)
        # простая эвристика: если очень большое целое — вероятно, копейки
        return v/100.0 if (v > 500 and float(v).is_integer()) else v
    except Exception:
        return 0.0

def _item_sum_from_qr(item: dict) -> float:
    if "sum" in item and item["sum"] is not None:
        return _rub(item["sum"])
    price = _rub(item.get("price", 0))
    qty = float(item.get("quantity", 1) or 1)
    return price * qty

# 1) /expenses — список чеков с позициями, ожидающими доставки
@router.message(Command("expenses"))
async def list_pending_receipts(message: Message, state: FSMContext):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("Доступ запрещен.")
        return

    try:
        res = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Чеки!A:M"
        ).execute()
        rows = res.get("values", [])[1:]  # пропускаем заголовок

        groups = {}  # fiscal_doc -> list[{row_index,name,sum,date,user,store}]
        for i, row in enumerate(rows, start=2):
            status = (row[6] if len(row) > 6 else "").strip().lower()
            if status != "ожидает":
                continue
            fiscal_doc = (row[10] if len(row) > 10 else "").strip()
            item_name  = (row[8] if len(row) > 8 else "").strip()
            if not fiscal_doc or not item_name:
                continue
            try:
                item_sum = float((row[2] if len(row) > 2 else "0").replace(",", "."))
            except Exception:
                item_sum = 0.0
            groups.setdefault(fiscal_doc, []).append({
                "row_index": i,
                "name": item_name,
                "sum": item_sum,
                "date": row[1] if len(row) > 1 else "",
                "user": row[3] if len(row) > 3 else "",
                "store": row[4] if len(row) > 4 else ""
            })

        if not groups:
            await message.answer("Нет чеков со статусом «Ожидает».")
            return

        kb_rows = []
        for fd, items in groups.items():
            kb_rows.append([
                InlineKeyboardButton(text=f"{fd} — позиций: {len(items)}",
                                     callback_data=f"choose_fd:{fd}")
            ])

        await state.update_data(pending_groups=groups)
        await message.answer(
            "Выберите чек (fiscal_doc), в котором хотите подтвердить доставку:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows)
        )
        await state.set_state(ConfirmDelivery.SELECT_RECEIPT)
    except HttpError as e:
        await message.answer(f"Ошибка Google Sheets: {e.status_code} - {e.reason}")
    except Exception as e:
        await message.answer(f"Неожиданная ошибка: {str(e)}")

# 2) Выбор конкретного чека → мультивыбор позиций
@router.callback_query(ConfirmDelivery.SELECT_RECEIPT, F.data.startswith("choose_fd:"))
async def choose_receipt(callback: CallbackQuery, state: FSMContext):
    fiscal_doc = callback.data.split(":", 1)[-1]  # БЕЗ int(...), чистая строка
    data = await state.get_data()
    groups = data.get("pending_groups", {})
    items = groups.get(fiscal_doc, [])
    if not items:
        await callback.message.edit_text("Позиции не найдены.")
        await callback.answer()
        return

    # сохраняем в состоянии список позиций этого чека
    await state.update_data(items=items, selected=set(), fd=fiscal_doc)

    # строим клавиатуру с переключателями
    def build_kb(items, selected_idxs):
        rows = []
        for idx, it in enumerate(items):
            checked = "☑️" if idx in selected_idxs else "⬜️"
            rows.append([
                InlineKeyboardButton(
                    text=f"{checked} {it['name']} — {it['sum']:.2f} RUB (стр. {it['row_index']})",
                    callback_data=f"sel:toggle:{idx}"
                )
            ])
        rows.append([InlineKeyboardButton(text="Далее ▶️", callback_data="sel:done")])
        rows.append([InlineKeyboardButton(text="Отмена", callback_data="sel:cancel")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    kb = build_kb(items, set())
    await callback.message.edit_text(
        f"Чек {fiscal_doc}. Выберите позиции для подтверждения:",
        reply_markup=kb
    )
    await state.set_state(ConfirmDelivery.SELECT_ITEMS)
    await callback.answer()

# 3) Тоггл/готово/отмена для мультивыбора
@router.callback_query(ConfirmDelivery.SELECT_ITEMS, F.data.startswith("sel:"))
async def select_items_toggle(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    items = data.get("items", [])
    selected = set(data.get("selected", set()))

    cmd = callback.data
    if cmd == "sel:cancel":
        await callback.message.edit_text("Отменено.")
        await state.clear()
        await callback.answer()
        return

    if cmd == "sel:done":
        if not selected:
            await callback.answer("Ничего не выбрано.", show_alert=True)
            return
        await state.update_data(selected=selected)
        await callback.message.edit_text("Отправьте фото QR-кода ЧЕКА ПОЛНОГО РАСЧЁТА (operationType=1).")
        await state.set_state(ConfirmDelivery.UPLOAD_FULL_QR)
        await callback.answer()
        return

    # sel:toggle:{idx}
    try:
        _, _, sidx = cmd.split(":", 2)
        idx = int(sidx)
        if idx < 0 or idx >= len(items):
            raise ValueError("bad index")
        if idx in selected:
            selected.remove(idx)
        else:
            selected.add(idx)
        await state.update_data(selected=selected)
    except Exception:
        await callback.answer("Некорректный индекс.", show_alert=True)
        return

    # перестраиваем клавиатуру
    def build_kb(items, selected_idxs):
        rows = []
        for i, it in enumerate(items):
            checked = "☑️" if i in selected_idxs else "⬜️"
            rows.append([
                InlineKeyboardButton(
                    text=f"{checked} {it['name']} — {it['sum']:.2f} RUB (стр. {it['row_index']})",
                    callback_data=f"sel:toggle:{i}"
                )
            ])
        rows.append([InlineKeyboardButton(text="Далее ▶️", callback_data="sel:done")])
        rows.append([InlineKeyboardButton(text="Отмена", callback_data="sel:cancel")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    kb = build_kb(items, selected)
    await callback.message.edit_reply_markup(reply_markup=kb)
    await callback.answer()

# 4) Загрузка QR полного расчёта и проверка соответствия выбранных позиций
@router.message(ConfirmDelivery.UPLOAD_FULL_QR)
async def upload_full_qr(message: Message, state: FSMContext, bot: Bot):
    loading = await message.answer("⌛ Проверяю чек...")

    if not message.photo:
        await loading.edit_text("Пожалуйста, пришлите фото QR-кода чека полного расчёта.")
        return

    parsed = await parse_qr_from_photo(bot, message.photo[-1].file_id)
    if not parsed:
        await loading.edit_text("Не удалось распознать QR. Проверьте качество фото.")
        return

    # Только ПОЛНЫЙ расчёт
    if parsed.get("operation_type") != 1:
        await loading.edit_text("Это не чек полного расчёта (operationType должен быть 1).")
        return

    data = await state.get_data()
    items = data.get("items", [])
    selected = sorted(list(data.get("selected", set())))
    sel_items = [items[i] for i in selected]

    # Сверяем названия и суммы (строгий матч по имени, допускаем «вхождения»; сумма ±2 коп.)
    qr_items = parsed.get("items", [])
    missing = []
    for it in sel_items:
        need_name = _norm_name(it["name"])
        need_sum  = float(it["sum"])
        matched = False
        for q in qr_items:
            q_name = _norm_name(q.get("name", ""))
            if not q_name:
                continue
            if q_name == need_name or (need_name in q_name or q_name in need_name):
                q_sum = _item_sum_from_qr(q)
                if abs(q_sum - need_sum) <= 0.02:
                    matched = True
                    break
        if not matched:
            missing.append(f"{it['name']} ({it['sum']:.2f})")

    if missing:
        await loading.edit_text(
            "❌ Проверка провалена. Не найдены в QR (или суммы не совпали):\n• " + "\n• ".join(missing)
        )
        return

    # Успех — сохраняем распарсенный чек и показываем подтверждение
    await state.update_data(qr_parsed=parsed)
    total = sum(it["sum"] for it in sel_items)
    details = [
        f"Чек (fiscal_doc): {parsed.get('fiscal_doc')}",
        f"Позиции ({len(sel_items)} шт., итого {total:.2f} RUB):"
    ] + [f"• {it['name']} — {it['sum']:.2f} RUB (строка {it['row_index']})" for it in sel_items]

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Подтвердить запись", callback_data="confirm:delivery_many")],
        [InlineKeyboardButton(text="Отмена", callback_data="confirm:cancel")]
    ])
    await loading.edit_text("✅ Проверка пройдена.\n" + "\n".join(details), reply_markup=kb)
    await state.set_state(ConfirmDelivery.CONFIRM_ACTION)

# 5) Финальное подтверждение — обновляем строки «Чеки» и пишем «Сводка»
@router.callback_query(ConfirmDelivery.CONFIRM_ACTION, F.data.in_(["confirm:delivery_many", "confirm:cancel"]))
async def confirm_delivery_many(callback: CallbackQuery, state: FSMContext):
    if callback.data == "confirm:cancel":
        await callback.message.edit_text("Отменено.")
        await state.clear()
        await callback.answer()
        return

    data = await state.get_data()
    items    = data.get("items", [])
    selected = sorted(list(data.get("selected", set())))
    sel_items = [items[i] for i in selected]
    parsed   = data.get("qr_parsed", {})

    new_fd  = parsed.get("fiscal_doc", "")
    qr_str  = parsed.get("qr_string", "")

    ok, fail = 0, 0
    errors = []

    # ВАЖНО: для подтверждения доставки мы НЕ требуем уникальности fiscal_doc —
    # один и тот же номер у нескольких строк допустим.
    for it in sel_items:
        row_index = it["row_index"]
        try:
            # читаем текущую строку
            res = sheets_service.spreadsheets().values().get(
                spreadsheetId=SHEET_NAME, range=f"Чеки!A{row_index}:M{row_index}"
            ).execute()
            row = res.get("values", [[]])[0] if res.get("values") else []
            while len(row) < 13:
                row.append("")

            # обновляем поля — РОВНО КАК В ТВОЕЙ ЛОГИКЕ:
            row[6]  = "Доставлено"    # G: статус
            row[9]  = "Полный"        # J: тип чека
            row[10] = str(new_fd)     # K: fiscal_doc полного чека
            row[11] = qr_str          # L: QR-строка полного расчёта

            sheets_service.spreadsheets().values().update(
                spreadsheetId=SHEET_NAME,
                range=f"Чеки!A{row_index}:M{row_index}",
                valueInputOption="RAW",
                body={"values": [row]}
            ).execute()

            # запись в «Сводка»: Покупка, отрицательная сумма, примечание "{fd} - {item_name}"
            date_for_summary = row[1] if len(row) > 1 and row[1] else row[0]
            await save_receipt_summary(
                date=date_for_summary,
                operation_type="Покупка",
                sum_value=-abs(float(it["sum"])),
                note=f"{new_fd} - {it['name']}"
            )

            ok += 1
        except HttpError as e:
            fail += 1
            errors.append(f"Строка {row_index}: {e.status_code} - {e.reason}")
        except Exception as e:
            fail += 1
            errors.append(f"Строка {row_index}: {str(e)}")

    # баланс после записей
    try:
        balance_data = await get_monthly_balance()
        balance = balance_data.get("balance", 0.0) if balance_data else 0.0
    except Exception:
        balance = 0.0

    if fail == 0:
        await callback.message.edit_text(
            f"✅ Подтверждено: {ok}/{ok}. Чек {new_fd}.\n🟰 Текущий остаток: {balance:.2f} RUB"
        )
    else:
        details = "\n".join(errors[:10])
        more = f"\n…и ещё {len(errors)-10}" if len(errors) > 10 else ""
        await callback.message.edit_text(
            f"⚠️ Частично: успешно {ok}, ошибок {fail}.\n{details}{more}\n🟰 Остаток: {balance:.2f} RUB"
        )

    await state.clear()
    await callback.answer()
# === КОНЕЦ БЛОКА /expenses ===




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
                f"💸 Баланс на {update_date}:\n"
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
