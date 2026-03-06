from aiogram import Router, F, Bot
from aiogram.filters import Command, StateFilter
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, CallbackQuery, ReplyKeyboardRemove
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from sheets import (
    is_user_allowed, 
    save_receipt, 
    is_fiscal_doc_unique,
    async_sheets_call,
    sheets_service,  # Если используется
    SHEET_NAME,  # Если используется
    get_monthly_balance,  # Для других частей, если нужно
    # NOVOYE: Импорт delta helpers из sheets.py
    compute_delta_balance,
    update_balance_cache_with_delta
)

from utils import parse_qr_from_photo, confirm_manual_api, safe_float, reset_keyboard, normalize_date
from handlers.notifications import send_notification
from googleapiclient.errors import HttpError
import logging
import asyncio
from datetime import datetime
import re
import calendar


logger = logging.getLogger("AccountingBot")
add_router = Router()

class AddReceiptQR(StatesGroup):
    UPLOAD_QR = State()
    CUSTOMER = State()
    SELECT_TYPE = State()
    CONFIRM_DELIVERY_DATE = State()
    WAIT_LINK = State()
    WAIT_COMMENT = State()
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

@add_router.message(F.text.casefold() == "сброс")
async def reset_action(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("🔄 Действие сброшено. Вы можете начать заново.", reply_markup=ReplyKeyboardRemove())
    logger.info(f"Сброс состояний: user_id={message.from_user.id}")

@add_router.message(StateFilter(None), F.photo)
async def catch_qr_photo_without_command(message: Message, state: FSMContext, bot: Bot) -> None:
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

@add_router.callback_query(lambda c: c.data == "goto_add_manual")
async def goto_add_manual(callback: CallbackQuery, state: FSMContext) -> None:
    user_id = callback.from_user.id  # Правильный user ID (1059161513)
    
    # Проверка доступа перед вызовом (fallback)
    if not await is_user_allowed(user_id):
        await callback.message.answer("🚫 Доступ запрещен.")
        logger.info(f"Доступ запрещен для goto_add_manual: user_id={user_id}")
        await callback.answer()
        return
    
    await state.clear()
    # Вызываем add_manual_start с user_id (для проверки) и callback.message (для chat_id/answer)
    await add_manual_start(callback.message, state, user_id=user_id)
    await callback.answer("Переход к ручному вводу чека...")

@add_router.message(Command("add"))
async def start_add_receipt(message: Message, state: FSMContext) -> None:
    if not await is_user_allowed(message.from_user.id):
        await message.answer("Доступ запрещен.")
        logger.info(f"Доступ запрещен для /add: user_id={message.from_user.id}")
        return
    await state.update_data(username=message.from_user.username or str(message.from_user.id))
    await message.answer("Отправьте фото QR-кода чека.", reply_markup=reset_keyboard())
    await state.set_state(AddReceiptQR.UPLOAD_QR)
    logger.info(f"Начало добавления чека по QR: user_id={message.from_user.id}")

@add_router.message(Command("add_manual"))
async def add_manual_start(message: Message, state: FSMContext, user_id: int | None = None) -> None:
    """
    Старт /add_manual — с optional user_id для callback (из goto_add_manual).
    Если user_id передан — используй его для проверки доступа.
    """
    check_id = user_id if user_id is not None else message.from_user.id
    if not await is_user_allowed(check_id):
        await message.answer("🚫 Доступ запрещен.")
        logger.info(f"Доступ запрещен для /add_manual: user_id={check_id}")
        return
    
    # Отправляем сообщение в тот же чат (message.chat.id)
    await message.answer("Введите *ФН* (номер фискального накопителя):", reply_markup=reset_keyboard())
    await state.set_state(AddManualAPI.FN)
    logger.info(f"Начало /add_manual: user_id={check_id}")

@add_router.message(AddReceiptQR.UPLOAD_QR)
async def process_qr_upload(message: Message, state: FSMContext, bot: Bot) -> None:
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

@add_router.message(AddReceiptQR.CUSTOMER)
async def process_customer(message: Message, state: FSMContext) -> None:
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

@add_router.callback_query(AddReceiptQR.SELECT_TYPE)
async def process_receipt_type(callback: CallbackQuery, state: FSMContext) -> None:
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
                f"📎 Пришлите ссылку на «{items[0].get('name', '—')}» "
                f"(например: https://www.ozon.ru/...).",
                reply_markup=reset_keyboard()
            )
            await state.update_data(current_item_index=0)
            await state.set_state(AddReceiptQR.WAIT_LINK)

        # Удалено else: (no items already returned)

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

@add_router.message(AddReceiptQR.CONFIRM_DELIVERY_DATE)
async def process_delivery_date(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    parsed_data = data["parsed_data"]
    items = parsed_data["items"]
    receipt_type = data["receipt_type"]

    current_item_index = data.get("current_item_index", 0)
    delivery_dates = data.get("delivery_dates", [])
    links = data.get("links", [])

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
            month_int, day_int = int(month), int(day)
            if not (1 <= month_int <= 12 and 1 <= day_int <= calendar.monthrange(int(full_year), month_int)[1]):
                raise ValueError("Invalid date")
            datetime.strptime(normalized_date, "%d.%m.%Y")
            delivery_date = normalized_date
        except ValueError as e:
            await message.answer(
                f"Неверный формат даты: {e}. Используйте ддммгг (например 110825) или /skip.",
                reply_markup=reset_keyboard()
            )
            return

    while len(delivery_dates) < current_item_index:
        delivery_dates.append("")
    if len(delivery_dates) == current_item_index:
        delivery_dates.append(delivery_date)
    else:
        delivery_dates[current_item_index] = delivery_date

    await state.update_data(delivery_dates=delivery_dates)

    item_name = items[current_item_index]['name']
    await message.answer(
        f"📎 Пришлите ссылку на «{item_name}» (например: https://www.ozon.ru/...).",
        reply_markup=reset_keyboard()
    )
    await state.set_state(AddReceiptQR.WAIT_LINK)

@add_router.message(AddReceiptQR.WAIT_LINK)
async def process_receipt_link(message: Message, state: FSMContext) -> None:
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

    link = "" if link == "/skip" else link
    while len(links) < current_item_index:
        links.append("")
    if len(links) == current_item_index:
        links.append(link)
    else:
        links[current_item_index] = link

    await state.update_data(links=links)

    item_name = items[current_item_index]['name']
    await message.answer(
        f"💬 Введите комментарий для «{item_name}» или /skip:",
        reply_markup=reset_keyboard()
    )
    await state.set_state(AddReceiptQR.WAIT_COMMENT)

@add_router.message(AddReceiptQR.WAIT_COMMENT)
async def process_receipt_comment(message: Message, state: FSMContext) -> None:
    comment = (message.text or "").strip()
    if comment == "/skip":
        comment = ""

    data = await state.get_data()
    parsed_data = data.get("parsed_data", {})
    items = parsed_data.get("items", [])
    receipt_type = data.get("receipt_type", "Покупка")
    current_item_index = data.get("current_item_index", 0)
    comments = data.get("comments", [])

    while len(comments) < current_item_index:
        comments.append("")
    if len(comments) == current_item_index:
        comments.append(comment)
    else:
        comments[current_item_index] = comment

    await state.update_data(comments=comments)

    if current_item_index + 1 < len(items):
        next_index = current_item_index + 1
        await state.update_data(current_item_index=next_index)

        if receipt_type == "Полный":
            # Сначала спрашиваем ссылку для следующего товара
            await message.answer(
                f"📎 Пришлите ссылку на «{items[next_index].get('name', '—')}» "
                f"(например: https://www.ozon.ru/...).",
                reply_markup=reset_keyboard()
            )
            await state.set_state(AddReceiptQR.WAIT_LINK)

        else:  # Предоплата (доставка)
            await message.answer(
                f"📅 Введите дату доставки для «{items[next_index].get('name', '—')}» "
                f"(ддммгг, например 110825) или /skip:",
                reply_markup=reset_keyboard()
            )
            await state.set_state(AddReceiptQR.CONFIRM_DELIVERY_DATE)
        return


    # Все обработано
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
            for i, item in enumerate(items)
        ],
        "receipt_type": receipt_type,
        "fiscal_doc": parsed_data.get("fiscal_doc", ""),
        "qr_string": parsed_data.get("qr_string", ""),
        "pdf_url": parsed_data.get("pdf_url", ""),
        "delivery_dates": delivery_dates,
        "links": links,
        "comments": comments,
        "status": "Ожидает" if receipt_type == "Предоплата" else "Доставлено",
        "customer": data.get("customer", "Неизвестно")
    }

    # ✅ НОВОЕ: Копируем excluded_sum и excluded_items из parsed_data в receipt
    # Это позволит save_receipt в sheets.py правильно добавить строку "Услуга" в Сводку
    receipt["excluded_sum"] = safe_float(parsed_data.get("excluded_sum", 0))
    receipt["excluded_items"] = parsed_data.get("excluded_items", [])

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

@add_router.callback_query(AddReceiptQR.CONFIRM_ACTION, lambda c: c.data == "confirm_add")
async def confirm_add_action(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    loading_message = await callback.message.answer("⌛ Сохранение чека...")

    data = await state.get_data()
    receipt: dict = data.get("receipt", {})

    user_name = await is_user_allowed(callback.from_user.id)
    if not user_name:
        await loading_message.edit_text("🚫 Доступ запрещен.")
        await state.clear()
        return

    items = receipt.get("items", [])
    total_sum = sum(safe_float(item.get("sum", 0)) for item in items)
    excluded_sum = safe_float(receipt.get("excluded_sum", 0))
    total_sum += excluded_sum

    logger.info(f"✅ Подтверждение добавления чека: fiscal_doc={receipt.get('fiscal_doc', '')}, total_sum={total_sum:.2f}, user={user_name}")

    saved = await save_receipt(receipt, user_name=user_name)

    if saved:
        balance_data = await get_monthly_balance(force_refresh=True)
        balance = balance_data.get("balance", 0.0) if balance_data else 0.0

        delivery_dates = receipt.get("delivery_dates", [])
        delivery_date_header = delivery_dates[0] if delivery_dates else "—"
        operation_date = datetime.now().strftime("%d.%m.%Y")

        # ✅ ДОБАВИТЬ ЭТУ СТРОЧКУ СЮДА:
        pdf_link = receipt.get("pdf_url", "")
        
        items_list = []
        for i, item in enumerate(items):
            deliv_date = delivery_dates[i] if i < len(delivery_dates) else ""
            items_list.append({
                "name": item.get("name", "—"),
                "sum": safe_float(item.get("sum", 0)),
                "price": safe_float(item.get("price", 0)),
                "quantity": int(item.get("quantity", 1) or 1),
                "link": receipt.get("links", [None])[i] if i < len(receipt.get("links", [])) else "",
                "comment": receipt.get("comments", [None])[i] if i < len(receipt.get("comments", [])) else "",
                "delivery_date": deliv_date
            })

        # 🔔 Уведомление в группу
        await send_notification(
            bot=callback.bot,
            action="🧾 Добавлен новый чек",
            items=items_list,
            user_name=user_name,
            fiscal_doc=receipt.get("fiscal_doc", ""),
            operation_date=operation_date,
            balance=balance,
            is_group=True,
            pdf_url=pdf_link
        )

        # 🔔 Личное уведомление пользователю
        await send_notification(
            bot=callback.bot,
            action="🧾 Чек успешно сохранён",
            items=items_list,
            user_name=user_name,
            fiscal_doc=receipt.get("fiscal_doc", ""),
            operation_date=operation_date,
            balance=balance,
            is_group=False,
            chat_id=callback.message.chat.id,
            pdf_url=pdf_link
        )

        await loading_message.delete()
        await callback.message.answer(f"✅ Чек сохранён! Баланс: {balance:.2f} ₽")
    else:
        await loading_message.edit_text(f"❌ Ошибка при сохранении чека {receipt.get('fiscal_doc', '')}.")

    await state.clear()

@add_router.callback_query(AddReceiptQR.CONFIRM_ACTION, lambda c: c.data == "cancel_add")
async def cancel_add_action(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.message.answer("Добавление чека отменено. Начать заново: /add")
    logger.info(f"Добавление чека отменено: user_id={callback.from_user.id}")
    await state.clear()
    await callback.answer()

# Manual API handlers (остальные без изменений, как в предыдущем)
@add_router.message(AddManualAPI.FN)
async def add_manual_fn(message: Message, state: FSMContext) -> None:
    await state.update_data(fn=message.text.strip())
    await state.set_state(AddManualAPI.FD)
    await message.answer("Введите номер ФД:", reply_markup=reset_keyboard())

@add_router.message(AddManualAPI.FD)
async def add_manual_fd(message: Message, state: FSMContext) -> None:
    await state.update_data(fd=message.text.strip())
    await state.set_state(AddManualAPI.FP)
    await message.answer("Введите ФП (фискальный признак):", reply_markup=reset_keyboard())

@add_router.message(AddManualAPI.FP)
async def add_manual_fp(message: Message, state: FSMContext) -> None:
    await state.update_data(fp=message.text.strip())
    await state.set_state(AddManualAPI.SUM)
    await message.answer("Введите сумму чека (например: 123.45):", reply_markup=reset_keyboard())

@add_router.message(AddManualAPI.SUM)
async def add_manual_sum(message: Message, state: FSMContext) -> None:
    try:
        s = safe_float(message.text)
        if s <= 0:
            await message.answer("⚠️ Сумма должна быть положительной. Попробуйте ещё раз.", reply_markup=reset_keyboard())
            return
        await state.update_data(s=s)
        await state.set_state(AddManualAPI.DATE)
        await message.answer("Введите дату (в формате ДДММГГ):", reply_markup=reset_keyboard())
    except ValueError:
        await message.answer("Неверный формат суммы. Попробуйте ещё раз.", reply_markup=reset_keyboard())

@add_router.message(AddManualAPI.DATE)
async def add_manual_date(message: Message, state: FSMContext) -> None:
    await state.update_data(date=message.text.strip())
    await state.set_state(AddManualAPI.TIME)
    await message.answer("Введите время (в формате ЧЧ:ММ):", reply_markup=reset_keyboard())

@add_router.message(AddManualAPI.TIME)
async def add_manual_time(message: Message, state: FSMContext) -> None:
    await state.update_data(time=message.text.strip())
    await state.set_state(AddManualAPI.TYPE)
    await message.answer("Введите тип операции (1=приход, 2=возврат прихода, 3=расход, 4=возврат расхода):", reply_markup=reset_keyboard())

@add_router.message(AddManualAPI.TYPE)
async def add_manual_type(message: Message, state: FSMContext) -> None:
    await state.update_data(op_type=message.text.strip())
    data = await state.get_data()

    details = (
        f"Проверьте данные чека:\n"
        f"ФН: {data['fn']}\n"
        f"ФД: {data['fd']}\n"
        f"ФП: {data['fp']}\n"
        f"Сумма: {data['s']:.2f}\n"
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

@add_router.callback_query(AddManualAPI.CONFIRM, lambda c: c.data == "confirm_manual_api")
async def confirm_manual_api_callback(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    loading = await callback.message.answer("⌛ Запрашиваю данные чека...")

    try:
        success, msg, parsed_data = await confirm_manual_api(data, callback.from_user)

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

        logger.info(f"Manual API success: fiscal={parsed_data.get('fiscal_doc', 'N/A')}, user={callback.from_user.id}")
        await callback.answer()

    except asyncio.TimeoutError as timeout_exc:
        await loading.edit_text("❌ Таймаут API. Попробуйте позже.")
        logger.error(f"Timeout in handler: {str(timeout_exc)}")
        await state.clear()
        await callback.answer()
    except Exception as exc:
        error_type = type(exc).__name__
        await loading.edit_text(f"⚠️ Ошибка: {error_type}: {str(exc)}.")
        logger.error(f"Handler error: {error_type}: {str(exc)}, user={callback.from_user.id}")
        await state.clear()
        await callback.answer()

@add_router.callback_query(AddManualAPI.CONFIRM, lambda c: c.data == "cancel_manual_api")
async def cancel_manual_api_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.message.answer("Добавление чека отменено. Начать заново: /add_manual")
    await state.clear()
    await callback.answer()