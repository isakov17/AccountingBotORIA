from config import SHEET_NAME, PROVERKACHEKA_TOKEN
from aiogram import Router, Bot
from aiogram.filters import Command
# 🔽 ДОБАВЬ К ИМПОРТАМ ВВЕРХУ ФАЙЛА
from aiogram import F
from aiogram.filters import StateFilter
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, CallbackQuery
from sheets import sheets_service, is_user_allowed, is_fiscal_doc_unique, save_receipt, get_monthly_balance, save_receipt_summary
from utils import parse_qr_from_photo, confirm_manual_api, safe_float
from handlers.notifications import send_group_notification, send_user_notification
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
    CONFIRM_DELIVERY_DATE = State()  # ввод даты для текущего товара
    WAIT_LINK = State()              # ввод ссылки для текущего товара
    WAIT_COMMENT = State()
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

class AddManualAPI(StatesGroup):
    FN = State()
    FD = State()
    FP = State()
    SUM = State()
    DATE = State()
    TIME = State()
    TYPE = State()
    CONFIRM = State()

def reset_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Сброс")]],
        resize_keyboard=True
    )

@router.message(F.text.casefold() == "сброс")
async def reset_action(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("🔄 Действие сброшено. Вы можете начать заново.", reply_markup=ReplyKeyboardRemove())
    logger.info(f"Сброс состояний: user_id={message.from_user.id}")


# 🔽 ГЛОБАЛЬНЫЙ ПЕРЕХВАТ ФОТО QR, ЕСЛИ ПОЛЬЗОВАТЕЛЬ НЕ ВОШЕЛ В /add
@router.message(StateFilter(None), F.photo)
async def catch_qr_photo_without_command(message: Message, state: FSMContext, bot: Bot):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("🚫 Доступ запрещен.")
        logger.info(f"Доступ запрещен для авто-обработки QR: user_id={message.from_user.id}")
        return

    loading = await message.answer("⌛ Обрабатываю фото чека...")

    try:
        parsed_data = await asyncio.wait_for(
            parse_qr_from_photo(bot, message.photo[-1].file_id),
            timeout=10.0
        )

        if not parsed_data:
            inline_keyboard = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="✍️ Ввести вручную", callback_data="goto_add_manual")]]
            )
            await loading.edit_text(
                "❌ QR-код не удалось распознать. Возможно, превышено количество обращений по чеку.\n"
                "Вы можете попробовать снова или добавить чек вручную:",
                reply_markup=inline_keyboard
            )
            logger.error(f"Не удалось распознать QR-код: user_id={message.from_user.id}")
            await state.clear()
            return

        if not await is_fiscal_doc_unique(parsed_data["fiscal_doc"]):
            await loading.edit_text(
                f"❌ Чек с фискальным номером {parsed_data['fiscal_doc']} уже существует."
            )
            logger.info(
                f"Авто-QR: дубликат фискального номера {parsed_data['fiscal_doc']}, user_id={message.from_user.id}"
            )
            await state.clear()
            return

        await loading.edit_text("✅ QR-код распознан.")
        await message.answer("Введите заказчика (или /skip):", reply_markup=reset_keyboard())
        await state.update_data(
            username=message.from_user.username or str(message.from_user.id),
            parsed_data=parsed_data
        )
        await state.set_state(AddReceiptQR.CUSTOMER)
        logger.info(
            f"Авто-старт /add по фото QR: fiscal_doc={parsed_data['fiscal_doc']}, "
            f"qr_string={parsed_data['qr_string']}, user_id={message.from_user.id}"
        )

    except asyncio.TimeoutError:
        inline_keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="✍️ Ввести вручную", callback_data="goto_add_manual")]]
        )
        await loading.edit_text(
            "❌ Превышено время обработки QR-кода. Попробуйте снова или добавьте чек вручную:",
            reply_markup=inline_keyboard
        )
        logger.error(f"Таймаут при обработке QR-кода: user_id={message.from_user.id}")
        await state.clear()
    except Exception as e:
        inline_keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="✍️ Ввести вручную", callback_data="goto_add_manual")]]
        )
        await loading.edit_text(
            f"⚠️ Ошибка при обработке фото: {str(e)}. Возможно, превышено количество обращений по чеку.\n"
            "Попробуйте снова или добавьте чек вручную:",
            reply_markup=inline_keyboard
        )
        logger.error(f"Ошибка обработки фото чека: {str(e)}, user_id={message.from_user.id}")
        await state.clear()



@router.callback_query(lambda c: c.data == "goto_add_manual")
async def goto_add_manual(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await add_manual_start(callback.message, state)  # запускаем как если бы /add_manual
    await callback.answer()


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

@router.message(Command("add_manual"))
async def add_manual_start(message: Message, state: FSMContext):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("🚫 Доступ запрещен.")
        return
    await message.answer("Введите *ФН* (номер фискального накопителя):", reply_markup=reset_keyboard())
    await state.set_state(AddManualAPI.FN)


@router.message(AddReceiptQR.UPLOAD_QR)
async def process_qr_upload(message: Message, state: FSMContext, bot: Bot):
    if not message.photo:
        await message.answer("Пожалуйста, отправьте фото QR-кода чека.", reply_markup=reset_keyboard())
        logger.info(f"Фото отсутствует для QR: user_id={message.from_user.id}")
        return
    parsed_data = await parse_qr_from_photo(bot, message.photo[-1].file_id)
    if not parsed_data:
        await message.answer("Ошибка обработки QR-кода. Убедитесь, что QR-код четкий, или используйте /add_manual для ручного ввода.", reply_markup=reset_keyboard())
        logger.error(f"Ошибка обработки QR-кода: user_id={message.from_user.id}")
        await state.clear()
        return
    if not await is_fiscal_doc_unique(parsed_data["fiscal_doc"]):
        await message.answer(f"Чек с фискальным номером {parsed_data['fiscal_doc']} уже существует.", reply_markup=reset_keyboard())
        logger.info(f"Дубликат фискального номера: {parsed_data['fiscal_doc']}, user_id={message.from_user.id}")
        await state.clear()
        return
    loading_message = await message.answer("⌛ Обработка запроса... Пожалуйста, подождите.")
    await state.update_data(parsed_data=parsed_data)
    await message.answer("Введите заказчика (или /skip):", reply_markup=reset_keyboard())
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
    await message.answer("Это доставка или покупка в магазине?", reply_markup=inline_keyboard)
    await message.answer("Или сбросьте действие:", reply_markup=reset_keyboard())
    await state.set_state(AddReceiptQR.SELECT_TYPE)
    logger.info(f"Заказчик принят: {customer}, user_id={message.from_user.id}")

@router.callback_query(AddReceiptQR.SELECT_TYPE)
async def process_receipt_type(callback: CallbackQuery, state: FSMContext):
    await callback.answer()

    data = await state.get_data()
    parsed_data = data.get("parsed_data", {})
    items = parsed_data.get("items", [])

    if not items:
        await callback.message.answer("⚠️ Нет товаров в чеке. Попробуйте снова или используйте /add_manual.", reply_markup=reset_keyboard())
        await state.clear()
        logger.error(f"Нет товаров в чеке: fiscal_doc={parsed_data.get('fiscal_doc', '')}, user_id={callback.from_user.id}")
        return

    total_sum = sum(safe_float(item.get("sum", 0)) for item in items)
    items_list = "\n".join([
        f"- {item.get('name', '—')} "
        f"(Сумма: {safe_float(item.get('sum', 0)):.2f} RUB, "
        f"Цена: {safe_float(item.get('price', 0)):.2f} RUB, "
        f"Кол-во: {item.get('quantity', 1)})"
        for item in items
    ])

    if callback.data == "type_store":
        receipt_type = "Полный"
        await state.update_data(receipt_type=receipt_type, delivery_dates=[], links=[], comments=[])
        if items:
            await callback.message.answer(
                f"💬 Введите комментарий для «{items[0].get('name', '—')}» или /skip:",
                reply_markup=reset_keyboard()
            )
            await state.update_data(current_item_index=0)
            await state.set_state(AddReceiptQR.WAIT_COMMENT)
        else:
            receipt = {
                "date": parsed_data.get("date"),
                "store": parsed_data.get("store", "Неизвестно"),
                # В блоке else (если items пустые, но для полноты)
                "items": [
                    {
                        "name": item.get("name", "—"),
                        "sum": safe_float(item.get("sum", 0)),
                        "price": safe_float(item.get("price", 0)),
                        "quantity": item.get("quantity", 1),
                        "link": "",  # Для store links=[] , так что ""
                        "comment": ""  # Аналогично
                    }
                    for item in items
                ],
                "receipt_type": receipt_type,
                "fiscal_doc": parsed_data.get("fiscal_doc", ""),
                "qr_string": parsed_data.get("qr_string", ""),
                "delivery_dates": [],
                "links": [],
                "comments": [],
                "status": "Доставлено",
                "customer": data.get("customer", "Неизвестно")
            }
            details = (
                f"Детали чека:\n"
                f"Магазин: {receipt['store']}\n"
                f"Заказчик: {receipt['customer']}\n"
                f"Сумма: {total_sum:.2f} RUB\n"
                f"Товары:\n{items_list}\n"
                f"Фискальный номер: {receipt['fiscal_doc']}"
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
        await state.update_data(receipt_type=receipt_type, delivery_dates=[], links=[], comments=[])
        await state.update_data(current_item_index=0)
        await callback.message.answer(
            f"📅 Введите дату доставки для «{items[0].get('name', '—')}» "
            f"(ддммгг, например 110825) или /skip:",
            reply_markup=reset_keyboard()
        )
        await state.set_state(AddReceiptQR.CONFIRM_DELIVERY_DATE)
        logger.info(f"Выбрана доставка: fiscal_doc={parsed_data.get('fiscal_doc', '')}, user_id={callback.from_user.id}")

@router.message(AddReceiptQR.CONFIRM_DELIVERY_DATE)
async def process_delivery_date(message: Message, state: FSMContext):
    data = await state.get_data()
    parsed_data = data["parsed_data"]
    items = parsed_data["items"]
    receipt_type = data["receipt_type"]

    # индекс текущего товара (по умолчанию 0 — первый)
    current_item_index = data.get("current_item_index", 0)
    delivery_dates = data.get("delivery_dates", [])
    links = data.get("links", [])

    # --- валидация даты/скипа ---
    if message.text == "/skip":
        delivery_date = ""
    else:
        date_pattern = r"^\d{6}$"
        if not re.match(date_pattern, message.text or ""):
            await message.answer(
                "Неверный формат даты. Используйте ддммгг (например 110825) или /skip.",
                reply_markup=reset_keyboard()
            )
            return
        try:
            day, month, year = message.text[:2], message.text[2:4], message.text[4:6]
            full_year = f"20{year}"
            normalized_date = f"{day}.{month}.{full_year}"
            datetime.strptime(normalized_date, "%d.%m.%Y")
            delivery_date = normalized_date
        except ValueError:
            await message.answer(
                "Неверный формат даты. Используйте ддммгг (например 110825) или /skip.",
                reply_markup=reset_keyboard()
            )
            return

    # гарантируем длину списка дат до текущего индекса
    while len(delivery_dates) < current_item_index:
        delivery_dates.append("")
    if len(delivery_dates) == current_item_index:
        delivery_dates.append(delivery_date)
    else:
        delivery_dates[current_item_index] = delivery_date

    # сохраняем и переходим за ссылкой для ЭТОГО же товара
    await state.update_data(delivery_dates=delivery_dates)

    item_name = items[current_item_index]['name']
    await message.answer(
        f"📎 Пришлите ссылку на «{item_name}» (например: https://www.ozon.ru/...).",
        reply_markup=reset_keyboard()
    )
    await state.set_state(AddReceiptQR.WAIT_LINK)


@router.message(AddReceiptQR.WAIT_LINK)
async def process_receipt_link(message: Message, state: FSMContext):
    link = (message.text or "").strip()

    if link != "/skip" and not (link.startswith("http://") or link.startswith("https://")):
        await message.answer(
            "⚠️ Пожалуйста, отправьте корректную ссылку (http/https) или /skip.",
            reply_markup=reset_keyboard()
        )
        return

    data = await state.get_data()
    parsed_data = data.get("parsed_data", {})
    items = parsed_data.get("items", [])
    current_item_index = data.get("current_item_index", 0)
    links = data.get("links", [])

    # Сохраняем ссылку
    link = "" if link == "/skip" else link
    while len(links) < current_item_index:
        links.append("")
    if len(links) == current_item_index:
        links.append(link)
    else:
        links[current_item_index] = link

    await state.update_data(links=links)

    # Переходим к вводу комментария
    item_name = items[current_item_index]['name']
    await message.answer(
        f"💬 Введите комментарий для «{item_name}» или /skip:",
        reply_markup=reset_keyboard()
    )
    await state.set_state(AddReceiptQR.WAIT_COMMENT)

@router.message(AddReceiptQR.WAIT_COMMENT)
async def process_receipt_comment(message: Message, state: FSMContext):
    comment = (message.text or "").strip()
    if comment == "/skip":
        comment = ""

    data = await state.get_data()
    parsed_data = data.get("parsed_data", {})
    items = parsed_data.get("items", [])
    receipt_type = data.get("receipt_type", "Покупка")
    current_item_index = data.get("current_item_index", 0)
    comments = data.get("comments", [])

    # Сохраняем комментарий
    while len(comments) < current_item_index:
        comments.append("")
    if len(comments) == current_item_index:
        comments.append(comment)
    else:
        comments[current_item_index] = comment

    await state.update_data(comments=comments)

    # Если есть следующий товар
    if current_item_index + 1 < len(items):
        next_index = current_item_index + 1
        await state.update_data(current_item_index=next_index)
        if receipt_type == "Полный":
            await message.answer(
                f"💬 Введите комментарий для «{items[next_index].get('name', '—')}» или /skip:",
                reply_markup=reset_keyboard()
            )
            await state.set_state(AddReceiptQR.WAIT_COMMENT)
        else:
            await message.answer(
                f"📅 Введите дату доставки для «{items[next_index].get('name', '—')}» "
                f"(ддммгг, например 110825) или /skip:",
                reply_markup=reset_keyboard()
            )
            await state.set_state(AddReceiptQR.CONFIRM_DELIVERY_DATE)
        return

    # Все товары обработаны
    total_sum = sum(safe_float(item.get("sum", 0)) for item in items)
    delivery_dates = data.get("delivery_dates", [])
    links = data.get("links", [])
    comments = data.get("comments", [])

    rows = []
    for i, item in enumerate(items):
        d = delivery_dates[i] if i < len(delivery_dates) else ""
        l = links[i] if i < len(links) else ""
        c = comments[i] if i < len(comments) else ""
        rows.append(
            f"- {item.get('name', '—')} "
            f"(Сумма: {safe_float(item.get('sum', 0)):.2f} RUB, "
            f"Цена: {safe_float(item.get('price', 0)):.2f} RUB, "
            f"Кол-во: {item.get('quantity', 1)}, "
            f"Доставка: {d or '—'}, "
            f"Ссылка: {l or '—'}, "
            f"Комментарий: {c or '—'})"
        )

    receipt = {
        "date": parsed_data.get("date"),
        "store": parsed_data.get("store", "Неизвестно"),
        "items": [
            {
                "name": item.get("name", "—"),
                "sum": safe_float(item.get("sum", 0)),
                "price": safe_float(item.get("price", 0)),
                "quantity": item.get("quantity", 1),
                "link": links[i] if i < len(links) else "",
                "comment": comments[i] if i < len(comments) else ""
            }
            for i, item in enumerate(items)  # ИСПРАВЛЕНИЕ: добавлен enumerate для правильного i
        ],
        "receipt_type": receipt_type,
        "fiscal_doc": parsed_data.get("fiscal_doc", ""),
        "qr_string": parsed_data.get("qr_string", ""),
        "delivery_dates": delivery_dates,
        "links": links,
        "comments": comments,
        "status": "Ожидает" if receipt_type == "Предоплата" else "Доставлено",
        "customer": data.get("customer", "Неизвестно")
    }

    details = (
        f"Детали чека:\n"
        f"Магазин: {receipt['store']}\n"
        f"Заказчик: {receipt['customer']}\n"
        f"Сумма: {total_sum:.2f} RUB\n"
        f"Товары:\n" + "\n".join(rows) + "\n"
        f"Фискальный номер: {receipt['fiscal_doc']}"
    )

    inline_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Подтвердить", callback_data="confirm_add")],
        [InlineKeyboardButton(text="Отменить", callback_data="cancel_add")]
    ])
    await message.answer(details, reply_markup=inline_keyboard)
    await message.answer("Или сбросьте действие:", reply_markup=reset_keyboard())
    await state.update_data(receipt=receipt)
    await state.set_state(AddReceiptQR.CONFIRM_ACTION)

@router.callback_query(AddReceiptQR.CONFIRM_ACTION, lambda c: c.data == "confirm_add")
async def confirm_add_action(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    loading_message = await callback.message.answer("⌛ Обработка запроса... Пожалуйста, подождите.")

    data = await state.get_data()
    receipt: dict = data.get("receipt", {})
    parsed_data: dict = data.get("parsed_data", {})
    user_name = await is_user_allowed(callback.from_user.id)

    if not user_name:
        await loading_message.edit_text("🚫 Доступ запрещен.")
        await state.clear()
        return

    # Логируем товары до сохранения
    logger.info(
        f"Перед сохранением чека: fiscal_doc={parsed_data.get('fiscal_doc', '')}, "
        f"items={receipt.get('items', [])}, user_id={callback.from_user.id}"
    )

    saved = await save_receipt(receipt, user_name=user_name)

    if saved:
        balance_data = await get_monthly_balance()
        balance = safe_float(balance_data.get("balance", 0.0)) if balance_data else 0.0

        # ИСПРАВЛЕНИЕ: Получаем delivery_dates из receipt
        delivery_dates = receipt.get("delivery_dates", [])
        # Для заголовка: первая дата или "Не указана"
        delivery_date_header = delivery_dates[0] if delivery_dates else "Не указана"

        # ИСПРАВЛЕНИЕ: Нормализуем товары с per-item данными, включая delivery_date
        items = []
        for i, item in enumerate(receipt.get("items", [])):
            deliv_date = delivery_dates[i] if i < len(delivery_dates) else ""
            items.append({
                "name": item.get("name", "—"),
                "sum": safe_float(item.get("sum", 0)),
                "price": safe_float(item.get("price", 0)),
                "quantity": int(item.get("quantity", 1) or 1),
                "link": item.get("link", ""),
                "comment": item.get("comment", ""),
                "delivery_date": deliv_date  # Per-item дата
            })

        # Отправляем уведомления
        await send_group_notification(
            bot=callback.bot,
            action="🆕 Добавлен чек",
            items=items,
            user_name=user_name,
            fiscal_doc=parsed_data.get("fiscal_doc", ""),
            delivery_date=delivery_date_header,
            balance=balance
        )

        await send_user_notification(
            bot=callback.bot,
            chat_id=callback.message.chat.id,
            action="🆕 Чек добавлен",
            items=items,
            user_name=user_name,
            fiscal_doc=parsed_data.get("fiscal_doc", ""),
            delivery_date=delivery_date_header,
            balance=balance
        )

        await loading_message.delete()
    else:
        await loading_message.edit_text(
            f"❌ Не удалось сохранить чек {parsed_data.get('fiscal_doc', '')}."
        )

    logger.info(
        f"Чек подтвержден: fiscal_doc={parsed_data.get('fiscal_doc', '')}, "
        f"positions={len(receipt.get('items', []))}, balance={balance}, "
        f"user_id={callback.from_user.id}, user_name={user_name}"
    )
    await state.clear()


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
    if val is None:
        return 0.0
    try:
        v = float(val)
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
            spreadsheetId=SHEET_NAME, range="Чеки!A:P"
        ).execute()
        rows = res.get("values", [])[1:]

        groups = {}
        for i, row in enumerate(rows, start=2):
            status = (row[8] if len(row) > 8 else "").strip().lower()   # I: статус
            if status != "ожидает":
                continue
            fiscal_doc = (row[12] if len(row) > 12 else "").strip()     # M: fiscal_doc
            item_name  = (row[10] if len(row) > 10 else "").strip()     # K: товар
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
                "date": row[1] if len(row) > 1 else "",   # B: дата покупки
                "user": row[5] if len(row) > 5 else "",   # F: пользователь
                "store": row[6] if len(row) > 6 else ""   # G: магазин
            })

        if not groups:
            await message.answer("Нет чеков со статусом «Ожидает».")
            return

        kb_rows = [
            [InlineKeyboardButton(text=f"{fd} — позиций: {len(items)}", callback_data=f"choose_fd:{fd}")]
            for fd, items in groups.items()
        ]

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
    fiscal_doc = callback.data.split(":", 1)[-1]
    data = await state.get_data()
    groups = data.get("pending_groups", {})
    items = groups.get(fiscal_doc, [])
    if not items:
        await callback.message.edit_text("Позиции не найдены.")
        await callback.answer()
        return

    await state.update_data(items=items, selected=set(), fd=fiscal_doc)

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

# 3) Тоггл/готово/отмена
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

# 4) Загрузка QR и проверка
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

    if parsed.get("operation_type") != 1:
        await loading.edit_text("Это не чек полного расчёта (operationType должен быть 1).")
        return

    data = await state.get_data()
    items = data.get("items", [])
    selected = sorted(list(data.get("selected", set())))
    sel_items = [items[i] for i in selected]

    qr_items = parsed.get("items", [])
    missing = []
    for it in sel_items:
        need_name = _norm_name(it["name"])
        matched = any(
            q_name and (q_name == need_name or need_name in q_name or q_name in need_name)
            for q in qr_items
            for q_name in [_norm_name(q.get("name", ""))]
        )
        if not matched:
            missing.append(it["name"])

    if missing:
        await loading.edit_text(
            "❌ Проверка провалена. Не найдены в QR:\n• " + "\n• ".join(missing),
            reply_markup=reset_keyboard()
        )
        return

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

# 5) Финальное подтверждение
@router.callback_query(ConfirmDelivery.CONFIRM_ACTION, F.data.in_(["confirm:delivery_many", "confirm:cancel"]))
async def confirm_delivery_many(callback: CallbackQuery, state: FSMContext):
    if callback.data == "confirm:cancel":
        await callback.message.edit_text("Отменено.")
        await state.clear()
        await callback.answer()
        return

    data = await state.get_data()
    items = data.get("items", [])
    selected = sorted(list(data.get("selected", set())))
    sel_items = [items[i] for i in selected]
    parsed = data.get("qr_parsed", {})
    new_fd = parsed.get("fiscal_doc", "")
    qr_str = parsed.get("qr_string", "")

    ok, fail, errors = 0, 0, []
    updated_items = []  # ИСПРАВЛЕНИЕ: Инициализируем список

    for it in sel_items:
        row_index = it["row_index"]
        try:
            res = sheets_service.spreadsheets().values().get(
                spreadsheetId=SHEET_NAME, range=f"Чеки!A{row_index}:Q{row_index}"
            ).execute()
            row = res.get("values", [[]])[0] if res.get("values") else []
            while len(row) < 17:
                row.append("")

            row[8] = "Доставлено"  # I: статус
            row[11] = "Полный"     # L: тип чека
            row[12] = str(new_fd)  # M: fiscal_doc
            row[13] = qr_str       # N: QR строка

            link = row[15].strip() if len(row) > 15 and row[15] else ""
            comment = row[16].strip() if len(row) > 16 and row[16] else ""
            delivery_date = row[7].strip() if row[7] else ""  # H: Дата доставки (per-item)

            sheets_service.spreadsheets().values().update(
                spreadsheetId=SHEET_NAME,
                range=f"Чеки!A{row_index}:Q{row_index}",
                valueInputOption="RAW",
                body={"values": [row]}
            ).execute()

            # ИСПРАВЛЕНИЕ: Добавляем per-item данные, включая delivery_date
            updated_items.append({
                "name": it.get("name", "—"),
                "sum": safe_float(it.get("sum", 0)),
                "quantity": int(it.get("quantity", 1)),
                "link": link,
                "comment": comment,
                "delivery_date": delivery_date
            })

            logger.info(f"Обновлена строка в Чеки: row={row_index}, fiscal_doc={new_fd}, link={link}, comment={comment}, delivery_date={delivery_date}")
            ok += 1
        except HttpError as e:
            fail += 1
            errors.append(f"Строка {row_index}: {e.status_code} - {e.reason}")
        except Exception as e:
            fail += 1
            errors.append(f"Строка {row_index}: {str(e)}")

    try:
        balance_data = await get_monthly_balance()
        balance = safe_float(balance_data.get("balance", 0.0)) if balance_data else 0.0
    except Exception as e:
        logger.error(f"Ошибка получения баланса: {str(e)}")
        balance = 0.0

    user_name = await is_user_allowed(callback.from_user.id) or callback.from_user.full_name

    # ИСПРАВЛЕНИЕ: Для заголовка delivery_date — первая из updated_items или текущая дата
    delivery_date_header = updated_items[0].get("delivery_date", datetime.now().strftime("%d.%m.%Y")) if updated_items else datetime.now().strftime("%d.%m.%Y")

    if fail == 0:
        await send_group_notification(
            bot=callback.bot,
            action="📦 Подтверждена доставка",
            items=updated_items,
            user_name=user_name,
            fiscal_doc=new_fd,
            delivery_date=delivery_date_header,
            balance=balance
        )

        await send_user_notification(
            bot=callback.bot,
            chat_id=callback.message.chat.id,
            action="📦 Доставка подтверждена",
            items=updated_items,
            user_name=user_name,
            fiscal_doc=new_fd,
            delivery_date=delivery_date_header,
            balance=balance
        )
    else:
        details = "\n".join(errors[:10])
        more = f"\n…и ещё {len(errors)-10}" if len(errors) > 10 else ""
        await callback.message.edit_text(
            f"⚠️ Частично: успешно {ok}, ошибок {fail}.\n{details}{more}\n🟰 Остаток: {balance:.2f} RUB"
        )

    logger.info(f"Доставка подтверждена: fiscal_doc={new_fd}, ok={ok}, fail={fail}, user_id={callback.from_user.id}")
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
    await message.answer("Пожалуйста, введите фискальный номер чека для возврата:", reply_markup=reset_keyboard())
    await state.set_state(ReturnReceipt.ENTER_FISCAL_DOC)
    logger.info(f"Запрос фискального номера для /return: user_id={message.from_user.id}")


@router.message(ReturnReceipt.ENTER_FISCAL_DOC)
async def process_fiscal_doc(message: Message, state: FSMContext):
    if not message.text:
        await message.answer("Пожалуйста, введите фискальный номер текстом.", reply_markup=reset_keyboard())
        logger.warning(f"Получен update без текста для /return: user_id={message.from_user.id}")
        return

    fiscal_doc = message.text.strip()
    if not fiscal_doc.isdigit() or len(fiscal_doc) > 20:
        await message.answer("Фискальный номер должен содержать только цифры и быть не длиннее 20 символов.", reply_markup=reset_keyboard())
        logger.info(f"Некорректный фискальный номер для /return: {fiscal_doc}, user_id={message.from_user.id}")
        return

    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Чеки!A:P"
        ).execute()
        receipts = [
            row for row in result.get("values", [])[1:]
            if len(row) > 12 and row[12] == fiscal_doc and row[8] != "Возвращен"  # M=fiscal_doc, I=статус
        ]
        if not receipts:
            await message.answer("Чеки не найдены или уже возвращены.", reply_markup=reset_keyboard())
            logger.info(f"Чеки не найдены для /return: fiscal_doc={fiscal_doc}, user_id={message.from_user.id}")
            return

        # Сохраняем карту товаров
        item_map = {i: (row[10] if len(row) > 10 else "Неизвестно") for i, row in enumerate(receipts)}  # K=товар
        await state.update_data(fiscal_doc=fiscal_doc, item_map=item_map)

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=row[10] if len(row) > 10 else "Неизвестно", callback_data=f"товар_{fiscal_doc}_{i}")]
            for i, row in enumerate(receipts)
        ])
        await message.answer("Выберите товар для возврата:", reply_markup=keyboard)
        await state.set_state(ReturnReceipt.SELECT_ITEM)
        logger.info(f"Чек для возврата найден: fiscal_doc={fiscal_doc}, user_id={message.from_user.id}")
    except HttpError as e:
        await message.answer(f"Ошибка получения данных из Google Sheets: {e.status_code} - {e.reason}. Проверьте /debug.", reply_markup=reset_keyboard())
        logger.error(f"Ошибка /return: {e.status_code} - {e.reason}, user_id={message.from_user.id}")
    except Exception as e:
        await message.answer(f"Неожиданная ошибка: {str(e)}. Проверьте /debug.", reply_markup=reset_keyboard())
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
    loading_message = await message.answer("⌛ Обработка запроса... Пожалуйста, подождите.")

    if not message.photo:
        await loading_message.edit_text("Пожалуйста, отправьте фото QR-кода.", reply_markup=reset_keyboard())
        return

    parsed_data = await parse_qr_from_photo(bot, message.photo[-1].file_id)
    if not parsed_data:
        await loading_message.edit_text("Ошибка обработки QR-кода.", reply_markup=reset_keyboard())
        return

    if parsed_data.get("operation_type") != 2:
        await loading_message.edit_text("Чек должен быть возвратным (operationType=2).", reply_markup=reset_keyboard())
        return

    # Проверяем, что нужный товар есть
    data = await state.get_data()
    expected_item = (data or {}).get("item_name", "")

    def norm(s: str) -> str:
        return " ".join((s or "").lower().split())

    tgt = norm(expected_item)
    found_item = next(
        (it for it in parsed_data.get("items", []) if tgt in norm(it.get("name", "")) or norm(it.get("name", "")) in tgt),
        None
    )

    if not found_item:
        await loading_message.edit_text(f"Товар «{expected_item}» не найден в чеке возврата.", reply_markup=reset_keyboard())
        return

    new_fiscal_doc = parsed_data.get("fiscal_doc", "")
    if not await is_fiscal_doc_unique(new_fiscal_doc):
        await loading_message.edit_text(f"Чек с фискальным номером {new_fiscal_doc} уже существует.", reply_markup=reset_keyboard())
        return

    # Детали возврата
    total_sum = float(found_item.get("sum", 0))
    item_price = float(found_item.get("price", 0))
    item_qty = float(found_item.get("quantity", 1))

    details = (
        f"Магазин: {parsed_data.get('store', 'Неизвестно')}\n"
        f"Заказчик: {data.get('customer', 'Неизвестно')}\n"
        f"Сумма возврата: {total_sum:.2f} RUB\n"
        f"Товар: {found_item.get('name', '—')}\n"
        f"Цена за ед.: {item_price:.2f} RUB\n"
        f"Количество: {item_qty}\n"
        f"Фискальный номер (новый): {new_fiscal_doc}"
    )

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Подтвердить", callback_data="confirm_return")],
        [InlineKeyboardButton(text="Отмена", callback_data="cancel_return")]
    ])

    await loading_message.edit_text(f"Возврат обработан. Детали:\n{details}", reply_markup=keyboard)

    await state.update_data(
        new_fiscal_doc=new_fiscal_doc,
        parsed_data=parsed_data,
        fiscal_doc=data.get("fiscal_doc"),
        item_name=expected_item
    )
    await state.set_state(ReturnReceipt.CONFIRM_ACTION)



# Обработчик подтверждения/отмены возврата
@router.callback_query(ReturnReceipt.CONFIRM_ACTION, lambda c: c.data in ["confirm_return", "cancel_return"])
async def handle_return_confirmation(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    fiscal_doc = data.get("fiscal_doc")
    new_fiscal_doc = data.get("new_fiscal_doc")
    item_name = data.get("item_name")
    parsed_data = data.get("parsed_data")
    user_name = await is_user_allowed(callback.from_user.id) or callback.from_user.full_name

    if callback.data == "confirm_return":
        try:
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=SHEET_NAME, range="Чеки!A:Q"
            ).execute()
            rows = result.get("values", [])[1:]

            row_updated = False
            for i, row in enumerate(rows, start=2):
                if len(row) > 12 and row[12] == fiscal_doc and row[10] == item_name:
                    while len(row) < 17:
                        row.append("")
                    row[8] = "Возвращен"
                    row[14] = parsed_data["qr_string"]
                    link = row[15].strip() if len(row) > 15 and row[15] else ""
                    comment = row[16].strip() if len(row) > 16 and row[16] else ""
                    delivery_date = row[7].strip() if row[7] else ""

                    sheets_service.spreadsheets().values().update(
                        spreadsheetId=SHEET_NAME,
                        range=f"Чеки!A{i}:Q{i}",
                        valueInputOption="RAW",
                        body={"values": [row]}
                    ).execute()
                    row_updated = True

                    total_sum = safe_float(row[2]) if row[2] else 0.0

                    # ИСПРАВЛЕНИЕ: Сначала сохраняем возврат в "Сводка"
                    await save_receipt_summary(
                        parsed_data["date"],
                        "Возврат",
                        total_sum,
                        f"{new_fiscal_doc} - {item_name}"
                    )

                    # ИСПРАВЛЕНИЕ: Затем получаем обновлённый баланс
                    try:
                        balance_data = await get_monthly_balance()
                        balance = safe_float(balance_data.get("balance", 0.0)) if balance_data else 0.0
                    except Exception as e:
                        logger.error(f"Ошибка получения баланса: {str(e)}")
                        balance = 0.0

                    items = [{
                        "name": item_name,
                        "sum": total_sum,
                        "quantity": 1,
                        "link": link,
                        "comment": comment,
                        "delivery_date": delivery_date
                    }]

                    delivery_date_header = delivery_date or datetime.now().strftime("%d.%m.%Y")

                    await send_group_notification(
                        bot=callback.bot,
                        action="↩️ Возврат товара",
                        items=items,
                        user_name=user_name,
                        fiscal_doc=new_fiscal_doc,
                        delivery_date=delivery_date_header,
                        balance=balance  # Передаём обновлённый баланс
                    )

                    await send_user_notification(
                        bot=callback.bot,
                        chat_id=callback.message.chat.id,
                        action="↩️ Возврат подтверждён",
                        items=items,
                        user_name=user_name,
                        fiscal_doc=new_fiscal_doc,
                        delivery_date=delivery_date_header,
                        balance=balance  # Передаём обновлённый баланс
                    )

                    break

            if not row_updated:
                await callback.message.edit_text(f"Товар {item_name} не найден для возврата.")
                logger.info(
                    f"Товар не найден для возврата: fiscal_doc={fiscal_doc}, item={item_name}, user_id={callback.from_user.id}"
                )
        except HttpError as e:
            await callback.message.edit_text(f"Ошибка обновления данных в Google Sheets: {e.status_code} - {e.reason}")
            logger.error(f"Ошибка обработки возврата: {e.status_code} - {e.reason}, user_id={callback.from_user.id}")
        except Exception as e:
            await callback.message.edit_text(f"Неожиданная ошибка: {str(e)}")
            logger.error(f"Неожиданная ошибка обработки возврата: {str(e)}, user_id={callback.from_user.id}")
    else:
        await callback.message.edit_text(
            f"Возврат товара {item_name} отменен.\nФискальный номер {new_fiscal_doc} не сохранен."
        )
        logger.info(
            f"Возврат отменен: old_fiscal_doc={fiscal_doc}, new_fiscal_doc={new_fiscal_doc}, "
            f"item={item_name}, user_id={callback.from_user.id}"
        )

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


@router.message(AddManualAPI.FN)
async def add_manual_fn(message: Message, state: FSMContext):
    await state.update_data(fn=message.text.strip())
    await state.set_state(AddManualAPI.FD)
    await message.answer("Введите номер ФД:", reply_markup=reset_keyboard())

@router.message(AddManualAPI.FD)
async def add_manual_fd(message: Message, state: FSMContext):
    await state.update_data(fd=message.text.strip())
    await state.set_state(AddManualAPI.FP)
    await message.answer("Введите ФП (фискальный признак):", reply_markup=reset_keyboard())

@router.message(AddManualAPI.FP)
async def add_manual_fp(message: Message, state: FSMContext):
    await state.update_data(fp=message.text.strip())
    await state.set_state(AddManualAPI.SUM)
    await message.answer("Введите сумму чека (например: 123.45):", reply_markup=reset_keyboard())

@router.message(AddManualAPI.SUM)
async def add_manual_sum(message: Message, state: FSMContext):
    try:
        await state.update_data(s=float(message.text.replace(",", ".")))
        await state.set_state(AddManualAPI.DATE)
        await message.answer("Введите дату (в формате ДДММГГ):", reply_markup=reset_keyboard())
    except ValueError:
        await message.answer("Неверный формат суммы. Попробуйте ещё раз.")

@router.message(AddManualAPI.DATE)
async def add_manual_date(message: Message, state: FSMContext):
    await state.update_data(date=message.text.strip())
    await state.set_state(AddManualAPI.TIME)
    await message.answer("Введите время (в формате ЧЧ:ММ):", reply_markup=reset_keyboard())


@router.message(AddManualAPI.TIME)
async def add_manual_time(message: Message, state: FSMContext):
    await state.update_data(time=message.text.strip())
    await state.set_state(AddManualAPI.TYPE)
    await message.answer("Введите тип операции (1=приход, 2=возврат прихода, 3=расход, 4=возврат расхода):", reply_markup=reset_keyboard())

@router.message(AddManualAPI.TYPE)
async def add_manual_type(message: Message, state: FSMContext):
    await state.update_data(op_type=message.text.strip())
    data = await state.get_data()

    details = (
        f"Проверьте данные чека:\n"
        f"ФН: {data['fn']}\n"
        f"ФД: {data['fd']}\n"
        f"ФП: {data['fp']}\n"
        f"Сумма: {data['s']}\n"
        f"Дата: {data['date']}\n"
        f"Время: {data['time']}\n"
        f"Тип: {data['op_type']}\n\n"
        f"Подтвердить запрос к proverkacheka.com?"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да", callback_data="confirm_manual_api")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_manual_api")]
    ])
    await message.answer(details, reply_markup=kb)
    await state.set_state(AddManualAPI.CONFIRM)


# === Обработчик подтверждения чека через ручной ввод (API) ===
@router.callback_query(AddManualAPI.CONFIRM, lambda c: c.data == "confirm_manual_api")
async def confirm_manual_api_callback(callback: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    loading = await callback.message.answer("⌛ Запрашиваю данные чека через API...")

    try:
        # Добавляем таймаут для confirm_manual_api
        success, msg, parsed_data = await asyncio.wait_for(
            confirm_manual_api(data, callback.from_user),
            timeout=10.0
        )

        if not success or not parsed_data:
            await loading.edit_text(msg)
            await state.clear()
            await callback.answer()
            return

        await loading.edit_text("✅ Чек получен.")
        await callback.message.answer("Введите заказчика (или /skip):", reply_markup=reset_keyboard())

        await state.update_data(
            username=callback.from_user.username or str(callback.from_user.id),
            parsed_data=parsed_data
        )
        await state.set_state(AddReceiptQR.CUSTOMER)

        logger.info(
            f"Manual API чек подтверждён: fiscal_doc={parsed_data['fiscal_doc']}, "
            f"qr_string={parsed_data['qr_string']}, user_id={callback.from_user.id}"
        )
        await callback.answer()

    except asyncio.TimeoutError:
        await loading.edit_text(
            "❌ Превышено время запроса к API. Попробуйте снова или добавьте чек вручную: /add_manual"
        )
        logger.error(f"Таймаут при запросе к API: user_id={callback.from_user.id}")
        await state.clear()
        await callback.answer()
    except Exception as e:
        await loading.edit_text(
            f"⚠️ Ошибка при запросе к API: {str(e)}. Проверьте /debug."
        )
        logger.error(f"Ошибка при запросе к API: {str(e)}, user_id={callback.from_user.id}")
        await state.clear()
        await callback.answer()



@router.callback_query(AddManualAPI.CONFIRM, lambda c: c.data == "cancel_manual_api")
async def cancel_manual_api_callback(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("Добавление чека отменено. Начать заново: /add_manual")
    await state.clear()
    await callback.answer()

