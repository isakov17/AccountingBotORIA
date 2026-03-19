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
from utils import parse_qr_from_photo, safe_float, reset_keyboard
from config import SHEET_NAME
from handlers.notifications import send_notification
from googleapiclient.errors import HttpError
from datetime import datetime
import urllib.parse

logger = logging.getLogger("AccountingBot")
return_router = Router()

class ReturnReceipt(StatesGroup):
    ENTER_SEARCH_TERM = State()  # ✅ НОВОЕ: Гибкий поиск (fiscal или имя)
    SELECT_ITEM = State()
    UPLOAD_RETURN_QR = State()
    CONFIRM_ACTION = State()

@return_router.message(Command("return"))
async def return_receipt(message: Message, state: FSMContext):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("Доступ запрещен.", reply_markup=reset_keyboard())  
        logger.info(f"Доступ запрещен для /return: user_id={message.from_user.id}")
        return
    
    await message.answer(
        "Пожалуйста, введите фискальный номер чека **или название/часть названия товара** для возврата:\n"
        "• Пример: '191208' (номер)\n"
        "• Пример: 'антенна угл' (имя)\n",
        reply_markup=reset_keyboard()  
    )
    await state.set_state(ReturnReceipt.ENTER_SEARCH_TERM)
    logger.info(f"Запрос поиска для /return: user_id={message.from_user.id}")

@return_router.message(ReturnReceipt.ENTER_SEARCH_TERM)
async def process_search_term(message: Message, state: FSMContext):
    search_term = message.text.strip().lower()  

    if not search_term:
        await message.answer("Запрос не может быть пустым. Попробуйте снова.", reply_markup=reset_keyboard())  
        return

    if len(search_term) > 50:  
        await message.answer("Запрос слишком длинный. Укоротите.", reply_markup=reset_keyboard())  
        return

    try:
        result = await async_sheets_call(
            sheets_service.spreadsheets().values().get,
            spreadsheetId=SHEET_NAME, range="Чеки!A:Q"
        )
        rows = result.get("values", [])[1:]  
        logger.info(f"Поиск по '{search_term}': загружено {len(rows)} строк из Чеки!A:Q")

        matches = []
        is_fiscal_search = search_term.isdigit()  

        for i, row in enumerate(rows): # Добавил enumerate для надежности, если нужен row_index
            if len(row) < 13:  
                continue
            if len(row) > 8 and row[8] == "Возвращен":  
                continue

            fiscal_doc = str(row[12] or "").strip()  # M=12
            item_name = (row[10] or "").strip() if len(row) > 10 else "Неизвестно"  # K=10
            date_purchase = (row[1] or "").strip() if len(row) > 1 else "—"  # B=1
            
            # ✅ НОВОЕ: Извлекаем цену товара (Столбец D = Индекс 3)
            # Так как в таблице могут быть пробелы или запятые, чистим строку
            raw_price = str(row[3]).replace(" ", "").replace(",", ".") if len(row) > 3 else "0"
            try:
                item_price = float(raw_price)
            except ValueError:
                item_price = 0.0

            # Формируем словарь с данными о товаре
            match_data = {
                "fiscal": fiscal_doc, 
                "item": item_name, 
                "date": date_purchase, 
                "price": item_price, # <-- Сохраняем цену
                "row_index": len(matches) # Используем длину списка как уникальный ID для inline кнопок
            }

            if is_fiscal_search:
                if fiscal_doc == search_term:
                    matches.append(match_data)
            else:
                if search_term in item_name.lower():
                    matches.append(match_data)

        count = len(matches)
        logger.info(f"Поиск по '{search_term}': найдено {count} совпадений")

        if count == 0:
            await message.answer(
                f"Чеки с номером '{search_term}' или товаром, содержащим '{search_term}', не найдены "
                f"(или уже возвращены). Уточните запрос и попробуйте снова.",
                reply_markup=reset_keyboard()  
            )
            return

        if count > 10:
            await message.answer(
                f"Слишком много совпадений ({count}). Уточните запрос (больше деталей по товару или fiscal).",
                reply_markup=reset_keyboard()  
            )
            return

        # Подготовка data
        item_map = {m["row_index"]: m for m in matches}  
        await state.update_data(item_map=item_map, search_term=search_term)

        if count == 1:
            # ✅ Авто-переход: Единственное совпадение
            match = matches[0]
            await state.update_data(
                fiscal_doc=match["fiscal"],
                item_name=match["item"],
                date_purchase=match["date"],
                item_price=match["price"] # <-- Передаем цену в state
            )
            await message.answer(
                f"✅ Найден единственный вариант:\n"
                f"• Товар: {match['item']}\n"
                f"• Цена: {match['price']:.2f} ₽\n" # <-- Показываем цену пользователю
                f"• Fiscal: {match['fiscal']}\n"
                f"• Дата: {match['date']}\n\n"
                f"Отправьте QR-код чека возврата.",
                reply_markup=reset_keyboard()  
            )
            await state.set_state(ReturnReceipt.UPLOAD_RETURN_QR)
            logger.info(f"Авто-переход: fiscal={match['fiscal']}, item={match['item']}, price={match['price']}")
            return

        # Если вариантов несколько - создаем кнопки
        button_texts = []
        inline_keyboard_buttons = []
        
        for i, m in enumerate(matches, 1):
            short_item = m['item'][:20] + '...' if len(m['item']) > 20 else m['item']
            # В текст сообщения добавляем цену для ясности
            button_texts.append(f"{i}. {short_item} ({m['price']:.2f}₽, f: {m['fiscal']}, d: {m['date']})")
            
            inline_keyboard_buttons.append(
                [
                    InlineKeyboardButton(
                        text=f"{i}. {short_item} ({m['price']:.2f}₽)", 
                        callback_data=f"select_return_{m['fiscal']}_{m['row_index']}"
                    )
                ]
            )

        list_text = "\n".join(button_texts)  
        await message.answer(
            f"✅ Найдено {count} совпадений по '{search_term}'. Выберите товар:\n\n"
            f"{list_text}\n\n",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=inline_keyboard_buttons)
        )
        await state.set_state(ReturnReceipt.SELECT_ITEM)

    except HttpError as e:
        await message.answer(f"Ошибка получения данных из Google Sheets: {e.status_code} - {e.reason}.", reply_markup=reset_keyboard())  
    except Exception as e:
        await message.answer(f"Неожиданная ошибка: {str(e)}.", reply_markup=reset_keyboard())


@return_router.callback_query(ReturnReceipt.SELECT_ITEM)
async def process_return_item(callback: CallbackQuery, state: FSMContext):
    try:
        data_parts = callback.data.split("_")   
        if len(data_parts) != 4 or data_parts[0] != "select" or data_parts[1] != "return":
            raise ValueError("Неверный callback")

        fiscal_doc = data_parts[2]
        index = int(data_parts[3])
        state_data = await state.get_data()
        item_map = state_data.get("item_map", {})
        # Преобразуем ключи item_map обратно в int, так как state мог сохранить их как строки (json)
        match = item_map.get(str(index)) or item_map.get(index) 

        if not match:
            await callback.message.answer("Ошибка: вариант не найден.", reply_markup=reset_keyboard())  
            await state.clear()
            await callback.answer()
            return

        # ✅ НОВОЕ: Сохраняем цену в state при выборе товара из списка
        await state.update_data(
            fiscal_doc=fiscal_doc,
            item_name=match["item"],
            date_purchase=match["date"],
            item_price=match["price"] # <-- ПЕРЕДАЕМ ЦЕНУ
        )
        
        await callback.message.edit_text(
            f"Выбран товар: {match['item']} ({match['price']:.2f} ₽).\n\n"
            f"Отправьте QR-код чека возврата."
        )
        await state.set_state(ReturnReceipt.UPLOAD_RETURN_QR)
        await callback.answer()
        
    except Exception as e:
        await callback.message.answer("Ошибка выбора. Попробуйте /return заново.", reply_markup=reset_keyboard())  
        logger.error(f"Ошибка выбора: {str(e)}")
        await state.clear()
        await callback.answer()

@return_router.message(ReturnReceipt.UPLOAD_RETURN_QR)
async def process_return_qr(message: Message, state: FSMContext, bot: Bot):
    loading_message = await message.answer("⌛ Обработка QR-кода возврата... Пожалуйста, подождите.")

    if not message.photo:
        await loading_message.edit_text("Пожалуйста, отправьте фото QR-кода.", reply_markup=None)
        logger.info(f"Фото отсутствует для возврата: user_id={message.from_user.id}")
        return

    data = await state.get_data()
    expected_item = data.get("item_name", "")
    
    # Достаем цену товара из состояния (убедись, что она сохраняется на этапе SELECT_ITEM)
    expected_price = safe_float(data.get("item_price", 0))

    parsed_data = await parse_qr_from_photo(bot, message.photo[-1].file_id)
    if not parsed_data:
        await loading_message.edit_text("Ошибка обработки QR-кода. Убедитесь, что QR-код четкий.", reply_markup=None)
        logger.info(f"Ошибка обработки QR-кода для возврата: user_id={message.from_user.id}")
        return

    if parsed_data.get("operation_type") != 2:
        await loading_message.edit_text("Чек должен быть возвратом (operationType == 2).", reply_markup=None)
        logger.info(f"Некорректный чек для возврата: operation_type={parsed_data.get('operation_type')}, user_id={message.from_user.id}")
        return

    # ✅ НОВОЕ: Сначала вычисляем полную сумму возврата из QR (totalSum), чтобы использовать в валидации
    total_return_sum = safe_float(parsed_data.get("totalSum", 0))

    if total_return_sum <= 0:
        items_total = sum(safe_float(it.get("sum", 0)) for it in parsed_data.get("items", []))
        total_return_sum = items_total + safe_float(parsed_data.get("excluded_sum", 0))
        logger.warning(f"Fallback total_return_sum: {total_return_sum:.2f} (totalSum был 0, used items + excluded)")

    logger.info(f"QR возврата: totalSum={total_return_sum}, items_count={len(parsed_data.get('items', []))}, excluded_sum={parsed_data.get('excluded_sum', 0)}")
    
    if total_return_sum <= 0:
        await loading_message.edit_text("Сумма возврата в QR нулевая или некорректная.", reply_markup=None)
        logger.info(f"Нулевая сумма в QR возврата: totalSum={total_return_sum}, user_id={message.from_user.id}")
        return

    # ✅ Проверка: Валидация по имени ИЛИ по цене
    def norm(s: str) -> str:
        return (s or "").lower().strip()

    # 1. Проверяем совпадение по имени
    tgt = norm(expected_item)
    name_match = False
    for it in parsed_data.get("items", []):
        name = norm(it.get("name", ""))
        if tgt in name or name in tgt or name == tgt:
            name_match = True
            break

    # 2. Проверяем совпадение по сумме (допускаем погрешность в пару копеек из-за float)
    price_match = (expected_price > 0) and abs(total_return_sum - expected_price) < 0.05

    # Если ни имя не подошло, ни цена не совпала — отклоняем
    if not (name_match or price_match):
        error_msg = (
            f"⚠️ Ошибка валидации чека возврата.\n\n"
            f"Товар «{expected_item}» не найден по названию, "
            f"а сумма чека возврата ({total_return_sum:.2f} ₽) "
        )
        if expected_price > 0:
            error_msg += f"не совпадает с изначальной ценой товара ({expected_price:.2f} ₽)."
        else:
            error_msg += f"и цену товара в базе не удалось определить."

        await loading_message.edit_text(error_msg, reply_markup=None)
        logger.info(
            f"Товар не найден в QR возврата: Name match={name_match}, Price match={price_match} "
            f"(Total={total_return_sum}, Expected={expected_price}), user_id={message.from_user.id}"
        )
        return

    new_fiscal_doc = parsed_data.get("fiscal_doc", "")
    if not await is_fiscal_doc_unique(new_fiscal_doc):
        await loading_message.edit_text(f"Чек возврата с фискальным номером {new_fiscal_doc} уже существует.", reply_markup=None)
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
    await loading_message.edit_text(f"✅ QR возврата обработан. Детали:\n{details}\n\nПодтвердите или отмените:", reply_markup=inline_keyboard)
    
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

        # ✅ НОВОЕ: Извлекаем ссылку на PDF возврата и готовим кнопку
        pdf_url = parsed_data.get("pdf_url", "")
        qr_string = parsed_data.get("qr_string", "")
        
        if pdf_url:
            qr_cell_value = f'=HYPERLINK("{pdf_url}"; "📄 PDF Возврата")'
        else:
            safe_qr = urllib.parse.quote(qr_string)
            fallback_link = f"https://proverkacheka.com/qrcode/generate?text={safe_qr}"
            qr_cell_value = f'=HYPERLINK("{fallback_link}"; "⏳ PDF готовится (QR)")'

        for i, row in enumerate(rows, start=2):
            if len(row) < 13:
                continue
            if str(row[12] or "").strip() == fiscal_doc and (row[10] or "").strip() == item_name:
                while len(row) < 17:
                    row.append("")
                row[8] = "Возвращен"
                
                # ✅ ИЗМЕНЕНО: Записываем готовую формулу гиперссылки в столбец O (индекс 14)
                row[14] = qr_cell_value 
                
                await async_sheets_call(
                    sheets_service.spreadsheets().values().update,
                    spreadsheetId=SHEET_NAME,
                    range=f"Чеки!A{i}:Q{i}",
                    valueInputOption="USER_ENTERED", # ✅ ИЗМЕНЕНО: Чтобы формула сработала
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
                    date_purchase,
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
                is_group=True,
                pdf_url=pdf_url  # ✅ НОВОЕ: Передаем ссылку на чек возврата в группу
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
                chat_id=callback.message.chat.id,
                pdf_url=pdf_url  # ✅ НОВОЕ: Передаем ссылку пользователю
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