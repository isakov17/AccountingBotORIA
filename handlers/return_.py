import logging
from aiogram import F, Router
from aiogram.types import Message, CallbackQuery, FSInputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import Command
import gspread
from gspread.exceptions import APIError, WorksheetNotFound
from config import SHEET_NAME, SHEET_CREDENTIALS  # ← Добавлено: импорт SHEET_NAME
from utils import parse_qr_from_photo, safe_float, reset_keyboard
from keyboards import get_return_keyboard, get_position_keyboard, get_confirm_keyboard

logger = logging.getLogger("AccountingBot")
return_router = Router()

class ReturnReceipt(StatesGroup):
    FISCAL_DOC = State()  # Ввод fiscal_doc
    POSITIONS = State()   # Выбор позиций для возврата
    RETURN_QR = State()   # QR возврата (op=2)
    CONFIRM = State()     # Подтверждение

@return_router.message(Command("return"))
async def return_handler(message: Message, state: FSMContext) -> None:
    """
    Старт /return — ввод fiscal_doc.
    """
    if not await is_user_allowed(message.from_user.id):
        await message.answer("🚫 Доступ запрещен.")
        return

    await message.answer("Введите фискальный номер чека для возврата (формат: FN-FD или FN-FD-FP):", reply_markup=reset_keyboard())
    await state.set_state(ReturnReceipt.FISCAL_DOC)
    logger.info(f"Запрос фискального номера для /return: user_id={message.from_user.id}")

@return_router.message(ReturnReceipt.FISCAL_DOC)
async def process_fiscal_doc(message: Message, state: FSMContext) -> None:
    """
    Обработка fiscal_doc — поиск в 'Чеки'.
    """
    fiscal_doc = message.text.strip()
    user_id = message.from_user.id

    try:
        # Подключение к Sheets (фикс: импорт SHEET_NAME)
        gc = gspread.service_account(filename=SHEET_CREDENTIALS)
        worksheet = gc.open_by_key(SHEET_NAME).worksheet('Чеки')  # SHEET_NAME из config

        # Поиск строки с fiscal_doc (A=Дата, C=Фискальный номер чека)
        rows = worksheet.get_all_values()
        found_row = None
        for idx, row in enumerate(rows[1:], start=2):  # Пропуск заголовков
            if fiscal_doc in row[2]:  # C=Фискальный номер (fiscal_doc)
                if row[11] == "Доставлено":  # L=Статус
                    found_row = idx
                    break
                else:
                    await message.answer(f"❌ Чек {fiscal_doc} не доставлен (статус: {row[11]}). Только для 'Доставлено'.", reply_markup=reset_keyboard())
                    await state.clear()
                    return

        if not found_row:
            await message.answer(f"❌ Чек с номером {fiscal_doc} не найден в таблице.", reply_markup=reset_keyboard())
            await state.clear()
            return

        # Сохраняем found_row и данные чека
        check_data = rows[found_row - 1]  # 0-based
        await state.update_data(found_row=found_row, fiscal_doc=fiscal_doc, check_data=check_data)
        
        # KB позиций (по аналогии с /expenses)
        positions = []  # Парсинг items из check_data (предполагаем, что items в M-N или отдельный лист)
        # TODO: Извлечь items из check_data[12:] (M=Items JSON или текст)
        inline_kb = get_position_keyboard(positions, fiscal_doc)  # Функция из keyboards.py
        await message.answer(f"✅ Чек {fiscal_doc} найден (сумма: {safe_float(check_data[2])} RUB).\nВыберите позицию для возврата:", reply_markup=inline_kb)
        await state.set_state(ReturnReceipt.POSITIONS)

        logger.info(f"Чек для возврата найден: row={found_row}, fiscal_doc={fiscal_doc}, user_id={user_id}")

    except (APIError, WorksheetNotFound) as e:
        logger.error(f"Ошибка доступа к Sheets в /return: {str(e)}")
        await message.answer("⚠️ Ошибка доступа к таблице. Попробуйте позже.", reply_markup=reset_keyboard())
        await state.clear()
    except Exception as e:
        logger.error(f"Неожиданная ошибка /return: {str(e)}, user_id={user_id}")
        await message.answer(f"⚠️ Неожиданная ошибка: {str(e)}. Обратитесь к админу.", reply_markup=reset_keyboard())
        await state.clear()

@return_router.callback_query(lambda c: c.data.startswith("return_pos_"))
async def process_return_position(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Выбор позиции для возврата → QR возврата (op=2).
    """
    pos_index = int(callback.data.split("_")[-1])
    data = await state.get_data()
    positions = data.get("positions", [])  # Извлечь из check_data

    if 0 <= pos_index < len(positions):
        selected_pos = positions[pos_index]
        await state.update_data(selected_pos=selected_pos)
        await callback.message.edit_text(
            f"✅ Выбрана позиция: {selected_pos['name']} ({selected_pos['sum']} RUB).\n"
            f"Отправьте фото QR-кода чека возврата (op=2, сумма должна совпадать с оригиналом).",
            reply_markup=None
        )
        await state.set_state(ReturnReceipt.RETURN_QR)
        await callback.answer("Переход к QR возврата...")
    else:
        await callback.answer("❌ Неверная позиция.")

@return_router.message(ReturnReceipt.RETURN_QR, F.photo)
async def process_return_qr(message: Message, state: FSMContext, bot) -> None:
    """
    Обработка QR возврата — проверка op=2, сумма=оригинал.
    """
    data = await state.get_data()
    found_row = data["found_row"]
    check_data = data["check_data"]
    original_sum = safe_float(check_data[2])  # Сумма оригинала

    loading = await message.answer("⌛ Проверяю QR возврата...")
    try:
        parsed_data = await asyncio.wait_for(
            parse_qr_from_photo(bot, message.photo[-1].file_id),
            timeout=10.0
        )

        if not parsed_data or parsed_data.get("operation_type") != 2:  # op=2 для возврата
            await loading.edit_text(f"❌ QR возврата некорректен (op_type={parsed_data.get('operation_type', 'unknown')}). Должен быть возврат прихода (op=2).")
            await state.clear()
            return

        return_sum = parsed_data.get("total_sum", 0)
        if abs(return_sum - original_sum) > 0.01:  # Точность 0.01 RUB
            await loading.edit_text(f"❌ Сумма возврата ({return_sum} RUB) не совпадает с оригиналом ({original_sum} RUB).")
            await state.clear()
            return

        await loading.edit_text("✅ QR возврата проверен. Детали: Чек возврата, Сумма совпадает.")
        
        # KB подтверждения
        inline_kb = get_confirm_keyboard("Подтвердить возврат")
        await message.answer(
            f"Подтвердите возврат для чека {data['fiscal_doc']}:\n"
            f"• Позиция: {data['selected_pos']['name']} ({data['selected_pos']['sum']} RUB)\n"
            f"• Сумма возврата: {return_sum} RUB\n"
            f"• Новый статус: Возвращен",
            reply_markup=inline_kb
        )
        await state.update_data(return_data=parsed_data)
        await state.set_state(ReturnReceipt.CONFIRM)

        logger.info(f"QR возврата проверен: return_sum={return_sum}, original={original_sum}, user_id={message.from_user.id}")

    except asyncio.TimeoutError:
        await loading.edit_text("❌ Превышено время обработки QR. Попробуйте снова.")
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка обработки QR возврата: {str(e)}")
        await loading.edit_text(f"⚠️ Ошибка: {str(e)}. Попробуйте снова.")
        await state.clear()

@return_router.callback_query(lambda c: c.data == "confirm_return")
async def confirm_return(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Подтверждение возврата — обновление 'Чеки' и summary.
    """
    data = await state.get_data()
    found_row = data["found_row"]
    fiscal_doc = data["fiscal_doc"]
    check_data = data["check_data"]
    return_data = data["return_data"]
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)

    original_sum = safe_float(check_data[2])
    return_sum = return_data.get("total_sum", original_sum)

    try:
        # Обновление 'Чеки'
        gc = gspread.service_account(filename=SHEET_CREDENTIALS)
        worksheet = gc.open_by_key(SHEET_NAME).worksheet('Чеки')
        
        # Обновляем статус (L=Статус) и дату возврата (M=Дата возврата)
        worksheet.update(f'L{found_row}', "Возвращен")
        worksheet.update(f'M{found_row}', datetime.now().strftime("%d.%m.%Y %H:%M"))  # Дата возврата
        worksheet.update(f'N{found_row}', f"Возврат подтверждён пользователем {username}")  # Примечание

        # Обновление 'Сводка' (доход +return_sum)
        summary_ws = gc.open_by_key(SHEET_NAME).worksheet('Сводка')
        summary_rows = summary_ws.get_all_values()
        for idx, row in enumerate(summary_rows[1:], start=2):
            if row[0] == datetime.now().strftime("%d.%m.%Y"):  # Сегодняшняя строка
                current_income = safe_float(row[2])  # C=Приход
                summary_ws.update(f'C{idx}', current_income + return_sum)
                break
        else:
            # Новая строка для сегодня
            summary_ws.append_row([datetime.now().strftime("%d.%m.%Y"), "Возврат чека", return_sum, 0, f"Возврат {fiscal_doc}"])

        await callback.message.edit_text(f"✅ Возврат подтверждён!\nЧек {fiscal_doc} обновлён (статус: Возвращен, сумма: +{return_sum} RUB).")
        await callback.answer("Возврат сохранён!")

        # Уведомление (личное + group)
        await callback.message.answer(
            f"↩️ *Возврат подтверждён*\n\n"
            f"📄 Чек: {fiscal_doc}\n"
            f"💰 Сумма возврата: {return_sum} RUB\n"
            f"📦 Позиция: {data['selected_pos']['name']}\n"
            f"👤 Пользователь: {username}\n\n"
            f"💸 Новый баланс: {19721.22 + return_sum} RUB"  # Пример; рассчитай реальный
        )
        logger.info(f"Возврат подтверждён: fiscal_doc={fiscal_doc}, sum={return_sum}, user_id={user_id}")

    except (APIError, WorksheetNotFound) as e:
        logger.error(f"Ошибка обновления Sheets в /return: {str(e)}")
        await callback.message.edit_text("⚠️ Ошибка обновления таблицы. Попробуйте позже.")
    except Exception as e:
        logger.error(f"Неожиданная ошибка подтверждения возврата: {str(e)}")
        await callback.message.edit_text(f"⚠️ Неожиданная ошибка: {str(e)}.")

    await state.clear()

# Обработчик отмены ("Сброс")
@return_router.message(F.text == "Сброс", ReturnReceipt)
async def cancel_return(message: Message, state: FSMContext) -> None:
    await message.answer("Все действия отменены. Выберите команду: /start", reply_markup=reset_keyboard())
    await state.clear()
    logger.info(f"/return отменён: user_id={message.from_user.id}")