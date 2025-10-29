import logging
import asyncio
from aiogram import F, Router, Bot  # Bot для type hint
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import Command
from sheets import (
    is_user_allowed, 
    save_receipt_summary,  # Только summary для возврата
    is_fiscal_doc_unique,
    async_sheets_call,
    sheets_service,
    SHEET_NAME,
    get_monthly_balance,
)
from utils import parse_qr_from_photo, safe_float, reset_keyboard, build_qr_from_manual, process_check_from_qrraw
from config import SHEET_NAME
from handlers.notifications import send_notification
from googleapiclient.errors import HttpError
from datetime import datetime

logger = logging.getLogger("AccountingBot")
return_router = Router()

class ReturnReceipt(StatesGroup):
    ENTER_SEARCH_TERM = State()
    SELECT_ITEM = State()
    UPLOAD_RETURN_QR = State()
    MANUAL_ENTRY = State()   # ✅ новое состояние
    CONFIRM_ACTION = State()


@return_router.message(Command("return"))
async def return_receipt(message: Message, state: FSMContext):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("Доступ запрещен.", reply_markup=reset_keyboard())  # OK: answer
        logger.info(f"Доступ запрещен для /return: user_id={message.from_user.id}")
        return
    
    await message.answer(
        "Пожалуйста, введите фискальный номер чека **или название/часть названия товара** для возврата:\n"
        "• Пример: '191208' (номер)\n"
        "• Пример: 'антенна угл' (имя)\n",
        reply_markup=reset_keyboard()  # OK: answer
    )
    await state.set_state(ReturnReceipt.ENTER_SEARCH_TERM)
    logger.info(f"Запрос поиска для /return: user_id={message.from_user.id}")

@return_router.message(ReturnReceipt.ENTER_SEARCH_TERM)
async def process_search_term(message: Message, state: FSMContext):
    search_term = message.text.strip().lower()  # Нормализация

    if not search_term:
        await message.answer("Запрос не может быть пустым. Попробуйте снова.", reply_markup=reset_keyboard())  # OK: answer
        return

    if len(search_term) > 50:  # Разумный лимит
        await message.answer("Запрос слишком длинный. Укоротите.", reply_markup=reset_keyboard())  # OK: answer
        return

    try:
        # 1 запрос на все данные
        result = await async_sheets_call(
            sheets_service.spreadsheets().values().get,
            spreadsheetId=SHEET_NAME, range="Чеки!A:Q"
        )
        rows = result.get("values", [])[1:]  # Пропуск заголовка
        logger.info(f"Поиск по '{search_term}': загружено {len(rows)} строк из Чеки!A:Q")

        matches = []
        is_fiscal_search = search_term.isdigit()  # Цифры → поиск по fiscal

        for row in rows:
            if len(row) < 13:  # Минимум до M=fiscal
                continue
            if row[8] == "Возвращен":  # I=8: Уже возвращён
                continue

            fiscal_doc = str(row[12] or "").strip()  # M=12
            item_name = (row[10] or "").strip() if len(row) > 10 else "Неизвестно"  # K=10
            date_purchase = (row[1] or "").strip() if len(row) > 1 else "—"  # B=1: Дата покупки

            if is_fiscal_search:
                if fiscal_doc == search_term:
                    matches.append({"fiscal": fiscal_doc, "item": item_name, "date": date_purchase, "row_index": len(matches)})
            else:
                # Частичное по имени (case-insensitive)
                if search_term in item_name.lower():
                    matches.append({"fiscal": fiscal_doc, "item": item_name, "date": date_purchase, "row_index": len(matches)})

        count = len(matches)
        logger.info(f"Поиск по '{search_term}' ({'fiscal' if is_fiscal_search else 'имя'}): найдено {count} совпадений")

        if count == 0:
            await message.answer(
                f"Чеки с номером '{search_term}' или товаром, содержащим '{search_term}', не найдены "
                f"(или уже возвращены). Уточните запрос и попробуйте снова.",
                reply_markup=reset_keyboard()  # OK: answer
            )
            return

        if count > 10:
            await message.answer(
                f"Слишком много совпадений ({count}). Уточните запрос (больше деталей по товару или fiscal).",
                reply_markup=reset_keyboard()  # OK: answer
            )
            return

        # Подготовка data
        item_map = {m["row_index"]: m for m in matches}  # {index: {"fiscal": ..., "item": ..., "date": ...}}
        await state.update_data(item_map=item_map, search_term=search_term)

        if count == 1:
            # ✅ Авто-переход: Единственное совпадение
            match = matches[0]
            await state.update_data(
                fiscal_doc=match["fiscal"],
                item_name=match["item"],
                date_purchase=match["date"]
            )
            await message.answer(
                f"✅ Найден единственный вариант:\n"
                f"• Товар: {match['item']}\n"
                f"• Fiscal: {match['fiscal']}\n"
                f"• Дата покупки: {match['date']}\n\n"
                f"Отправьте QR-код чека возврата.",
                reply_markup=reset_keyboard()  # OK: answer
            )
            await state.set_state(ReturnReceipt.UPLOAD_RETURN_QR)
            logger.info(f"Авто-переход для '{search_term}': fiscal={match['fiscal']}, item={match['item']}, user_id={message.from_user.id}")
            return

        # ✅ ФИКС: Компактные кнопки (1 строка, <50 символов) + нумерованный список в сообщении для контекста
        button_texts = []
        for i, m in enumerate(matches, 1):
            short_item = m['item'][:20] + '...' if len(m['item']) > 20 else m['item']
            button_text = f"{short_item} (f {m['fiscal']}, d {m['date']})"
            button_texts.append(f"{i}. {short_item} (fiscal: {m['fiscal']}, дата: {m['date']})")
        inline_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=button_text,  # Компактно: "74-61-06 Гнездо пит.... (f 22713, d 04.09.2025)"
                    callback_data=f"select_return_{m['fiscal']}_{m['row_index']}"
                )
            ] for m in matches
        ])
        list_text = "\n".join(button_texts)  # Нумерованный список для деталей
        await message.answer(
            f"✅ Найдено {count} совпадений по '{search_term}'. Выберите товар:\n"
            f"{list_text}\n\n"
            f"(Кнопки с краткими деталями; полный список выше)",
            reply_markup=inline_keyboard
        )
        await state.set_state(ReturnReceipt.SELECT_ITEM)
        logger.info(f"Список для выбора: {count} вариантов по '{search_term}', user_id={message.from_user.id}")

    except HttpError as e:
        await message.answer(f"Ошибка получения данных из Google Sheets: {e.status_code} - {e.reason}. Проверьте /debug.", reply_markup=reset_keyboard())  # OK: answer
        logger.error(f"Ошибка поиска /return: {e.status_code} - {e.reason}, term={search_term}, user_id={message.from_user.id}")
    except Exception as e:
        await message.answer(f"Неожиданная ошибка: {str(e)}. Проверьте /debug.", reply_markup=reset_keyboard())  # OK: answer
        logger.error(f"Неожиданная ошибка поиска /return: {str(e)}, term={search_term}, user_id={message.from_user.id}")

@return_router.callback_query(ReturnReceipt.SELECT_ITEM)
async def process_return_item(callback: CallbackQuery, state: FSMContext):
    try:
        data_parts = callback.data.split("_", 2)  # select_return_{fiscal}_{index}
        if len(data_parts) != 4 or data_parts[0] != "select" or data_parts[1] != "return":
            raise ValueError("Неверный callback")

        fiscal_doc = data_parts[2]
        index = int(data_parts[3])
        state_data = await state.get_data()
        item_map = state_data.get("item_map", {})
        match = item_map.get(index, None)

        if not match:
            await callback.message.answer("Ошибка: вариант не найден.", reply_markup=reset_keyboard())  # OK: answer
            logger.error(f"Вариант не найден в item_map: index={index}, user_id={callback.from_user.id}")
            await state.clear()
            await callback.answer()
            return

        await state.update_data(
            fiscal_doc=fiscal_doc,
            item_name=match["item"],
            date_purchase=match["date"]
        )
        await callback.message.answer("Отправьте QR-код чека возврата.", reply_markup=reset_keyboard())  # OK: answer
        await state.set_state(ReturnReceipt.UPLOAD_RETURN_QR)
        await callback.answer()
        logger.info(f"Товар выбран из списка: fiscal={fiscal_doc}, item={match['item']}, user_id={callback.from_user.id}")
    except (ValueError, KeyError) as e:
        await callback.message.answer("Ошибка выбора. Попробуйте /return заново.", reply_markup=reset_keyboard())  # OK: answer
        logger.error(f"Ошибка выбора: callback_data={callback.data}, error={str(e)}, user_id={callback.from_user.id}")
        await state.clear()
        await callback.answer()

@return_router.message(ReturnReceipt.UPLOAD_RETURN_QR)
async def process_return_qr(message: Message, state: FSMContext, bot: Bot):
    loading_message = await message.answer("⌛ Обработка QR-кода возврата... Пожалуйста, подождите.")

    if not message.photo:
        await loading_message.edit_text("Пожалуйста, отправьте фото QR-кода.", reply_markup=None)  # ✅ ФИКС: None
        logger.info(f"Фото отсутствует для возврата: user_id={message.from_user.id}")
        return

    data = await state.get_data()
    expected_item = data.get("item_name", "")

    parsed_data = await parse_qr_from_photo(bot, message.photo[-1].file_id)
    if not parsed_data:
        await loading_message.delete()
        await message.answer(
            "❌ Не удалось распознать QR-код.\n"
            "Введите данные вручную:\n\n"
            "📄 Формат:\n"
            "<code>FN FD FP СУММА ДД.ММ.ГГГГ ЧЧ:ММ</code>\n\n"
            "Пример:\n"
            "<code>9960440302201159 12345 6789012345 1500.00 28.10.2025 14:30</code>\n\n"
            "Или /cancel чтобы выйти.",
            parse_mode="HTML"
        )
        await state.set_state(ReturnReceipt.MANUAL_ENTRY)
        logger.info(f"Переход в ручной ввод возврата: user_id={message.from_user.id}")
        return


    if parsed_data.get("operation_type") != 2:
        await loading_message.edit_text("Чек должен быть возвратом (operationType == 2).", reply_markup=None)  # ✅ ФИКС
        logger.info(f"Некорректный чек для возврата: operation_type={parsed_data.get('operation_type')}, user_id={message.from_user.id}")
        return

    # ✅ Проверка: Товар в items (filtered, для валидации)
    def norm(s: str) -> str:
        return (s or "").lower().strip()

    tgt = norm(expected_item)
    found_match = False
    for it in parsed_data.get("items", []):
        name = norm(it.get("name", ""))
        if tgt in name or name in tgt or name == tgt:
            found_match = True
            break

    if not found_match:
        await loading_message.edit_text(f"Товар «{expected_item}» не найден в items чека возврата.", reply_markup=None)  # ✅ ФИКС
        logger.info(
            f"Товар не найден в QR возврата: expected='{expected_item}', qr_items={[it.get('name') for it in parsed_data.get('items', [])]}, user_id={message.from_user.id}"
        )
        return

    # ✅ НОВОЕ: Полная сумма возврата из QR (totalSum, включая всё)
    total_return_sum = safe_float(parsed_data.get("totalSum", 0))

    if total_return_sum <= 0:
        items_total = sum(safe_float(it.get("sum", 0)) for it in parsed_data.get("items", []))
        total_return_sum = items_total + safe_float(parsed_data.get("excluded_sum", 0))
        logger.warning(f"Fallback total_return_sum: {total_return_sum:.2f} (totalSum был 0, used items + excluded)")

    logger.info(f"QR возврата: totalSum={total_return_sum}, items_count={len(parsed_data.get('items', []))}, excluded_sum={parsed_data.get('excluded_sum', 0)}")  # ✅ ЛОГ ДЛЯ ДИАГНОСТИКИ
    if total_return_sum <= 0:
        await loading_message.edit_text("Сумма возврата в QR нулевая или некорректная.", reply_markup=None)  # ✅ ФИКС
        logger.info(f"Нулевая сумма в QR возврата: totalSum={total_return_sum}, user_id={message.from_user.id}")
        return

    new_fiscal_doc = parsed_data.get("fiscal_doc", "")
    if not await is_fiscal_doc_unique(new_fiscal_doc):
        await loading_message.edit_text(f"Чек возврата с фискальным номером {new_fiscal_doc} уже существует.", reply_markup=None)  # ✅ ФИКС
        logger.info(f"Дубликат фискального номера в QR возврата: {new_fiscal_doc}, user_id={message.from_user.id}")
        return

    # Сохраняем data
    fiscal_doc = data["fiscal_doc"]
    item_name = data["item_name"]
    date_purchase = data.get("date_purchase", "—")
    details = (
        f"Товар: {item_name}\n"
        f"Дата покупки: {date_purchase}\n"
        f"Оригинальный fiscal: {fiscal_doc}\n"
        f"Новый fiscal (возврат): {new_fiscal_doc}\n"
        f"Сумма возврата: {total_return_sum:.2f} RUB (полная из чека возврата)\n"
        f"Магазин: {parsed_data.get('store', 'Неизвестно')}\n"
        f"Дата возврата: {parsed_data.get('date', '—')}"
    )
    inline_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Подтвердить", callback_data="confirm_return")],
        [InlineKeyboardButton(text="Отмена", callback_data="cancel_return")]
    ])
    await loading_message.edit_text(f"✅ QR возврата обработан. Детали:\n{details}\n\nПодтвердите или отмените:", reply_markup=inline_keyboard)  # Inline OK
    await state.update_data(
        new_fiscal_doc=new_fiscal_doc,
        parsed_data=parsed_data,
        total_return_sum=total_return_sum,
        fiscal_doc=fiscal_doc,
        item_name=item_name,
        date_purchase=date_purchase
    )
    await state.set_state(ReturnReceipt.CONFIRM_ACTION)
    logger.info(f"Возврат готов к подтверждению: old_fiscal={fiscal_doc}, new_fiscal={new_fiscal_doc}, item={item_name}, total_return_sum={total_return_sum}, user_id={message.from_user.id}")


@return_router.message(ReturnReceipt.MANUAL_ENTRY)
async def handle_manual_return_entry(message: Message, state: FSMContext):
    """
    Обработка ручного ввода QR для возврата (если фото не распознано)
    """
    text = message.text.strip()
    parts = text.split()

    if len(parts) != 6:
        await message.answer("⚠️ Формат неверный. Используй пример:\n<code>FN FD FP СУММА ДД.ММ.ГГГГ ЧЧ:ММ</code>", parse_mode="HTML")
        return

    fn, fd, fp, s, date_str, time_str = parts
    data = {
        "fn": fn,
        "fd": fd,
        "fp": fp,
        "s": s,
        "date": date_str,
        "time": time_str,
        "op_type": 2  # ✅ Возврат
    }

    qr_raw = await build_qr_from_manual(data)
    if not qr_raw:
        await message.answer("❌ Не удалось сформировать QR. Проверьте ввод.")
        return

    await message.answer("⌛ Проверяю чек через API...")
    success, msg, parsed_data = await process_check_from_qrraw(qr_raw)

    if not success or not parsed_data:
        await message.answer(f"❌ Ошибка: {msg}")
        await state.clear()
        return

    await message.answer("✅ Чек возврата получен успешно.")
    await state.update_data(parsed_data=parsed_data)
    await state.set_state(ReturnReceipt.CONFIRM_ACTION)
    logger.info(f"Ручной ввод возврата успешен: fiscal={parsed_data.get('fiscal_doc')}, user={message.from_user.id}")


@return_router.callback_query(ReturnReceipt.CONFIRM_ACTION, lambda c: c.data in ["confirm_return", "cancel_return"])
async def handle_return_confirmation(callback: CallbackQuery, state: FSMContext):
    await callback.answer()

    data = await state.get_data()
    fiscal_doc = data.get("fiscal_doc")
    new_fiscal_doc = data.get("new_fiscal_doc")
    item_name = data.get("item_name")
    total_return_sum = data.get("total_return_sum", 0.0)
    parsed_data = data.get("parsed_data")
    date_purchase = data.get("date_purchase", "—")

    if callback.data == "cancel_return":
        await callback.message.edit_text(f"🚫 Возврат {item_name} отменён.")
        await state.clear()
        return

    try:
        result = await async_sheets_call(
            sheets_service.spreadsheets().values().get,
            spreadsheetId=SHEET_NAME,
            range="Чеки!A:Q"
        )
        rows = result.get("values", [])[1:]
        updated_items, found = [], False

        for i, row in enumerate(rows, start=2):
            if len(row) < 13:
                continue
            if str(row[12] or "").strip() == fiscal_doc and (row[10] or "").strip() == item_name:
                while len(row) < 17:
                    row.append("")
                row[8] = "Возвращен"
                row[14] = parsed_data.get("qr_string", "")
                await async_sheets_call(
                    sheets_service.spreadsheets().values().update,
                    spreadsheetId=SHEET_NAME,
                    range=f"Чеки!A{i}:Q{i}",
                    valueInputOption="RAW",
                    body={"values": [row]}
                )

                updated_items.append({
                    "name": item_name,
                    "sum": safe_float(row[2]),
                    "quantity": int(row[4] or 1),
                    "price": safe_float(row[3]) if row[3] else safe_float(row[2]) / int(row[4] or 1),
                    "link": (row[15] or "").strip() if len(row) > 15 else "",
                    "comment": (row[16] or "").strip() if len(row) > 16 else "",
                    "delivery_date": (row[7] or "").strip() if len(row) > 7 else ""
                })

                await save_receipt_summary(
                    parsed_data.get("date", datetime.now().strftime("%d.%m.%Y")),
                    "Возврат",
                    total_return_sum,
                    f"{new_fiscal_doc} - {item_name}"
                )
                found = True
                break

        balance_data = await get_monthly_balance(force_refresh=True)
        balance = safe_float(balance_data.get("balance", 0.0)) if balance_data else 0.0
        user_name = await is_user_allowed(callback.from_user.id) or callback.from_user.full_name
        operation_date = datetime.now().strftime("%d.%m.%Y")

        if found:
            await send_notification(
                bot=callback.bot,
                action=f"↩️ Возврат подтверждён ({total_return_sum:.2f} ₽)",
                items=updated_items,
                user_name=user_name,
                fiscal_doc=new_fiscal_doc,
                operation_date=operation_date,
                balance=balance,
                is_group=True
            )
            await send_notification(
                bot=callback.bot,
                action=f"↩️ Возврат подтверждён ({total_return_sum:.2f} ₽)",
                items=updated_items,
                user_name=user_name,
                fiscal_doc=new_fiscal_doc,
                operation_date=operation_date,
                balance=balance,
                is_group=False,
                chat_id=callback.message.chat.id
            )
            await callback.message.edit_text(
                f"✅ Возврат {item_name} подтверждён.\n"
                f"Фискальный номер: {new_fiscal_doc}\n"
                f"Сумма: {total_return_sum:.2f} ₽\n"
                f"Баланс: {balance:.2f} ₽"
            )
        else:
            await callback.message.edit_text(f"⚠️ Не удалось найти товар {item_name} для обновления.")
    except HttpError as e:
        await callback.message.edit_text(f"Ошибка Google Sheets: {e.status_code} - {e.reason}")
    except Exception as e:
        await callback.message.edit_text(f"Ошибка подтверждения возврата: {e}")

    await state.clear()

# Отмена ("Сброс")
@return_router.message(F.text == "Сброс", ReturnReceipt)
async def cancel_return(message: Message, state: FSMContext):
    await message.answer("Все действия по возврату отменены. /start", reply_markup=reset_keyboard())  # OK: answer
    await state.clear()
    logger.info(f"/return отменён: user_id={message.from_user.id}")