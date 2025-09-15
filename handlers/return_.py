import logging
import asyncio
from aiogram import F, Router, Bot  # Bot для type hint
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import Command
from sheets import (
    is_user_allowed, 
    save_receipt, 
    save_receipt_summary,
    is_fiscal_doc_unique,
    async_sheets_call,
    sheets_service,  # Если используется
    SHEET_NAME,  # Если используется
    get_monthly_balance,  # Для других частей, если нужно
    # NOVOYE: Импорт delta helpers из sheets.py
    compute_delta_balance,
    update_balance_cache_with_delta,
)
from utils import parse_qr_from_photo, safe_float, reset_keyboard  # safe_float для sum
from config import SHEET_NAME  # spreadsheetId
from handlers.notifications import send_notification  # Уведомления (как в expenses)
from googleapiclient.errors import HttpError
from datetime import datetime

logger = logging.getLogger("AccountingBot")
return_router = Router()

class ReturnReceipt(StatesGroup):
    ENTER_FISCAL_DOC = State()
    SELECT_ITEM = State()
    UPLOAD_RETURN_QR = State()
    CONFIRM_ACTION = State()

@return_router.message(Command("return"))
async def return_receipt(message: Message, state: FSMContext):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("Доступ запрещен.")
        logger.info(f"Доступ запрещен для /return: user_id={message.from_user.id}")
        return
    
    await message.answer("Пожалуйста, введите фискальный номер чека для возврата:", reply_markup=reset_keyboard())
    await state.set_state(ReturnReceipt.ENTER_FISCAL_DOC)
    logger.info(f"Запрос фискального номера для /return: user_id={message.from_user.id}")

@return_router.message(ReturnReceipt.ENTER_FISCAL_DOC)
async def process_fiscal_doc(message: Message, state: FSMContext):
    fiscal_doc = message.text.strip()

    if not fiscal_doc.isdigit() or len(fiscal_doc) > 20:
        await message.answer("Фискальный номер должен содержать только цифры и быть не длиннее 20 символов.", reply_markup=reset_keyboard())
        logger.info(f"Некорректный фискальный номер для /return: {fiscal_doc}, user_id={message.from_user.id}")
        return

    try:
        result = await async_sheets_call(
            sheets_service.spreadsheets().values().get,
            spreadsheetId=SHEET_NAME, range="Чеки!A:Q"
        )
        rows = result.get("values", [])
        logger.info(f"Loaded {len(rows)} rows from Чеки!A:Q (first 2: {rows[:2] if len(rows) >= 2 else rows})")  # Debug
        receipts = [row for row in rows[1:] if len(row) > 13 and row[12] == fiscal_doc and row[8] != "Возвращен"]  # M=12 fiscal, I=8 != "Возвращен"
        if not receipts:
            await message.answer(f"Чеки с номером {fiscal_doc} не найдены или уже возвращены.", reply_markup=reset_keyboard())
            logger.info(f"Чеки не найдены для /return: fiscal_doc={fiscal_doc}, user_id={message.from_user.id}")
            return
        item_map = {}
        for i, row in enumerate(receipts):
            item_map[i] = row[10] if len(row) > 10 else "Неизвестно"  # K=10 товар
        await state.update_data(fiscal_doc=fiscal_doc, item_map=item_map)
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=(row[10] if len(row) > 10 else "Неизвестно"), callback_data=f"товар_{fiscal_doc}_{i}")]
            for i, row in enumerate(receipts)
        ])
        await message.answer(f"✅ Чек {fiscal_doc} найден ({len(receipts)} позиций).\nВыберите товар для возврата:", reply_markup=keyboard)
        await state.set_state(ReturnReceipt.SELECT_ITEM)
        logger.info(f"Чек для возврата найден: fiscal_doc={fiscal_doc}, user_id={message.from_user.id}")
    except HttpError as e:
        await message.answer(f"Ошибка получения данных из Google Sheets: {e.status_code} - {e.reason}. Проверьте /debug.", reply_markup=reset_keyboard())
        logger.error(f"Ошибка /return: {e.status_code} - {e.reason}, user_id={message.from_user.id}")
    except Exception as e:
        await message.answer(f"Неожиданная ошибка: {str(e)}. Проверьте /debug.", reply_markup=reset_keyboard())
        logger.error(f"Неожиданная ошибка /return: {str(e)}, user_id={message.from_user.id}")

@return_router.callback_query(ReturnReceipt.SELECT_ITEM)
async def process_return_item(callback: CallbackQuery, state: FSMContext):
    try:
        _, fiscal_doc, index = callback.data.split("_")
        index = int(index)
        data = await state.get_data()
        item_map = data["item_map"]
        item_name = item_map.get(index, "")
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

@return_router.message(ReturnReceipt.UPLOAD_RETURN_QR)
async def process_return_qr(message: Message, state: FSMContext, bot: Bot):
    loading_message = await message.answer("⌛ Обработка запроса... Пожалуйста, подождите.")

    if not message.photo:
        await loading_message.edit_text("Пожалуйста, отправьте фото QR-кода.", reply_markup=reset_keyboard())
        logger.info(f"Фото отсутствует для возврата: user_id={message.from_user.id}")
        return

    parsed_data = await parse_qr_from_photo(bot, message.photo[-1].file_id)
    if not parsed_data:
        await loading_message.edit_text("Ошибка обработки QR-кода. Убедитесь, что QR-код четкий.", reply_markup=reset_keyboard())
        logger.info(f"Ошибка обработки QR-кода для возврата: user_id={message.from_user.id}")
        return

    if parsed_data["operation_type"] != 2:
        await loading_message.edit_text("Чек должен быть возвратом (operationType == 2).", reply_markup=reset_keyboard())
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
        if name == tgt or (tgt and (tgt in name or name in tgt)):
            found_match = True
            break

    if not found_match:
        await loading_message.edit_text(f"Товар «{expected_item}» не найден в чеке возврата.", reply_markup=reset_keyboard())
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
        await loading_message.edit_text(f"Чек с фискальным номером {new_fiscal_doc} уже существует.", reply_markup=reset_keyboard())
        logger.info(f"Дубликат фискального номера: new_fiscal_doc={new_fiscal_doc}, user_id={message.from_user.id}")
        return

    # Сохраняем данные для последующего подтверждения
    data = await state.get_data()
    fiscal_doc = data["fiscal_doc"]
    item_name = data["item_name"]
    total_sum = 0.0  # Будет обновлено при подтверждении
    details = (
        f"Магазин: {parsed_data.get('store', 'Неизвестно')}\n"
        f"Заказчик: {parsed_data.get('customer', 'Неизвестно')}\n"
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
# Обработчик подтверждения/отмены возврата
# Обработчик подтверждения/отмены возврата
@return_router.callback_query(ReturnReceipt.CONFIRM_ACTION, lambda c: c.data in ["confirm_return", "cancel_return"])
async def handle_return_confirmation(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    fiscal_doc = data.get("fiscal_doc")
    new_fiscal_doc = data.get("new_fiscal_doc")
    item_name = data.get("item_name")
    parsed_data = data.get("parsed_data")

    if callback.data == "cancel_return":
        await callback.message.edit_text(f"Возврат товара {item_name} отменен. Фискальный номер: {new_fiscal_doc} не сохранен.")
        logger.info(f"Возврат отменен: old_fiscal_doc={fiscal_doc}, new_fiscal_doc={new_fiscal_doc}, item={item_name}, user_id={callback.from_user.id}")
        await state.clear()
        await callback.answer()
        return

    if callback.data == "confirm_return":
        ok, fail, errors = 0, 0, []
        updated_items = []
        row_updated = False
        total_sum = 0.0

        try:
            # Direct get full A:Q (как в твоём коде, ~0.3с)
            result = await async_sheets_call(
                sheets_service.spreadsheets().values().get,
                spreadsheetId=SHEET_NAME, range="Чеки!A:Q"
            )
            rows = result.get("values", [])[1:]  # Skip header
            logger.debug(f"Return confirm: Loaded {len(rows)} rows from Чеки!A:Q")

            for i, row in enumerate(rows, start=2):
                if len(row) > 13 and row[12] == fiscal_doc and row[10] == item_name:
                    while len(row) < 17:
                        row.append("")
                    row[8] = "Возвращен"  # I=8
                    row[14] = parsed_data["qr_string"]  # O=14

                    await async_sheets_call(
                        sheets_service.spreadsheets().values().update,
                        spreadsheetId=SHEET_NAME,
                        range=f"Чеки!A{i}:Q{i}",
                        valueInputOption="RAW",
                        body={"values": [row]}
                    )
                    row_updated = True

                    total_sum = safe_float(row[2]) if row[2] else 0.0  # C=2
                    note = f"{new_fiscal_doc} - {item_name}"
                    await save_receipt_summary(parsed_data.get("date", datetime.now().strftime("%d.%m.%Y")), "Возврат", total_sum, note)

                    # Updated items
                    link = row[15].strip() if len(row) > 15 else ""
                    comment = row[16].strip() if len(row) > 16 else ""
                    delivery_date = row[7].strip() if row[7] else ""
                    updated_items.append({
                        "name": item_name,
                        "sum": total_sum,
                        "quantity": int(row[4] or 1),
                        "link": link,
                        "comment": comment,
                        "delivery_date": delivery_date
                    })

                    logger.info(f"Обновлена строка в Чеки: row={i}, fiscal_doc={new_fiscal_doc}")
                    ok += 1
                    break
                else:
                    fail += 1
                    errors.append(f"Строка {i}: Товар не найден")

            if row_updated:
                # Force fetch реального баланса (~0.3с)
                balance_data = await get_monthly_balance(force_refresh=True)
                balance = balance_data.get("balance", 0.0) if balance_data else 0.0

                user_name = await is_user_allowed(callback.from_user.id) or callback.from_user.full_name
                delivery_date_header = updated_items[0].get("delivery_date", datetime.now().strftime("%d.%m.%Y")) if updated_items else datetime.now().strftime("%d.%m.%Y")

                await send_notification(
                    bot=callback.bot,
                    action="↩️ Возврат подтверждён",
                    items=updated_items,
                    user_name=user_name,
                    fiscal_doc=new_fiscal_doc,
                    delivery_date=delivery_date_header,
                    balance=balance,
                    is_group=True
                )

                await send_notification(
                    bot=callback.bot,
                    action="↩️ Возврат подтверждён",
                    items=updated_items,
                    user_name=user_name,
                    fiscal_doc=new_fiscal_doc,
                    delivery_date=delivery_date_header,
                    balance=balance,
                    is_group=False,
                    chat_id=callback.message.chat.id
                )

                await callback.message.edit_text(f"✅ Возврат товара {item_name} подтверждён. Фискальный номер: {new_fiscal_doc}. Сумма: {total_sum:.2f} RUB.\n🟰 Остаток: {balance:.2f} RUB.")
                logger.info(f"Возврат подтверждён: old_fiscal_doc={fiscal_doc}, new_fiscal_doc={new_fiscal_doc}, item={item_name}, total_sum={total_sum}, balance={balance}, user_id={callback.from_user.id}")

            else:
                # Fetch даже при ошибках
                balance_data = await get_monthly_balance(force_refresh=True)
                balance = balance_data.get("balance", 0.0) if balance_data else 0.0

                details = "\n".join(errors[:10])
                more = f"\n…и ещё {len(errors)-10}" if len(errors) > 10 else ""
                await callback.message.edit_text(f"⚠️ Частично: успешно {ok}, ошибок {fail}.\n{details}{more}\nОстаток: {balance:.2f} RUB")
                logger.info(f"Товар не найден для возврата: fiscal_doc={fiscal_doc}, item={item_name}, user_id={callback.from_user.id}")

        except HttpError as e:
            await callback.message.edit_text(f"Ошибка обновления данных в Google Sheets: {e.status_code} - {e.reason}. Проверьте /debug.")
            logger.error(f"Ошибка обработки возврата: {e.status_code} - {e.reason}, user_id={callback.from_user.id}")
        except Exception as e:
            await callback.message.edit_text(f"Неожиданная ошибка: {str(e)}. Проверьте /debug.")
            logger.error(f"Неожиданная ошибка обработки возврата: {str(e)}, user_id={callback.from_user.id}")

    await state.clear()
    await callback.answer()

# Отмена ("Сброс")
@return_router.message(F.text == "Сброс", ReturnReceipt)
async def cancel_return(message: Message, state: FSMContext):
    await message.answer("Все действия по возврату отменены. /start", reply_markup=reset_keyboard())
    await state.clear()
    logger.info(f"/return отменён: user_id={message.from_user.id}")