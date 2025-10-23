from aiogram import Router, Bot, types
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, ReplyKeyboardRemove, ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext
from sheets import sheets_service, is_user_allowed, async_sheets_call, get_monthly_balance, is_fiscal_doc_unique  # + get_monthly_balance
from config import SHEET_NAME, PROVERKACHEKA_TOKEN, YOUR_ADMIN_ID, SPREADSHEETS_LINK
from exceptions import (
    get_excluded_items,
    add_excluded_item,
    remove_excluded_item
)
from utils import redis_client, safe_float
from googleapiclient.errors import HttpError
import logging
import aiohttp
from datetime import datetime

logger = logging.getLogger("AccountingBot")
router = Router()

@router.message(Command("start"))
async def start_command(message: Message):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("🚫 Доступ запрещен.")
        logger.info(f"Доступ запрещен для user_id={message.from_user.id}")
        return

    await message.answer(
        "👋 Добро пожаловать в *Бухгалтерия ОРИА*!\n\n"
        "Теперь вы можете просто отправить 📸 *фото QR-кода чека* — бот сам начнёт добавление!\n\n"
        "*Основные команды:*\n"
        "💰 `/balance` — показать текущий баланс\n"
        "📥 `/add` — добавить чек вручную по QR-коду\n"
        "✅ `/expenses` — подтвердить доставку товаров\n"
        "🔙 `/return` — обработать возврат\n\n"
        "📌 Если что-то пошло не так — используйте команду `Сброс` в клавиатуре.",
    )

    await message.answer(
        f"📊 [Открыть таблицу расходов]({SPREADSHEETS_LINK})",
        parse_mode="Markdown",
        disable_web_page_preview=True
    )

    logger.info(f"/start выполнена: user_id={message.from_user.id}")

@router.message(lambda message: message.text == "Сброс")
async def reset_command(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Все действия отменены. Выберите команду: /start", reply_markup=ReplyKeyboardRemove())
    logger.info(f"Состояние сброшено: user_id={message.from_user.id}")

@router.message(Command("test"))
async def test_connectivity(message: Message):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("Доступ запрещен.")
        logger.info(f"Доступ запрещен для /test: user_id={message.from_user.id}")
        return
    response = []
    try:
        await async_sheets_call(sheets_service.spreadsheets().get, spreadsheetId=SHEET_NAME)
        response.append("Google Sheets: Подключение успешно")
    except HttpError as e:
        response.append(f"Google Sheets: Ошибка - {e.status_code} {e.reason}")
        logger.error(f"Ошибка проверки Google Sheets: {e.status_code} - {e.reason}")
    except Exception as e:
        response.append(f"Google Sheets: Неожиданная ошибка - {str(e)}")
        logger.error(f"Неожиданная ошибка проверки Google Sheets: {str(e)}")
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get("https://proverkacheka.com/api/v1/check/get", params={"token": PROVERKACHEKA_TOKEN}) as resp:
                response.append(f"Proverkacheka API: HTTP {resp.status}")
                logger.info(f"Проверка Proverkacheka API: status={resp.status}")
        except Exception as e:
            response.append(f"Proverkacheka API: Ошибка - {str(e)}")
            logger.error(f"Ошибка проверки Proverkacheka API: {str(e)}")
    
    await message.answer("\n".join(response))
    logger.info(f"Команда /test выполнена: user_id={message.from_user.id}")
    
@router.message(Command("disable_notifications"))
async def disable_notifications(message: Message, state: FSMContext):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("Доступ запрещен.")
        logger.info(f"Доступ запрещен для /disable_notifications: user_id={message.from_user.id}")
        return
    try:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.answer("Укажите ключ уведомления (например, /disable_notifications 199977_2).")
            logger.info(f"Ключ уведомления не указан: user_id={message.from_user.id}")
            return
        notification_key = args[1]
        await redis_client.sadd("notified_items", notification_key)
        await message.answer(f"Уведомления для {notification_key} отключены.")
        logger.info(f"Уведомления отключены: notification_key={notification_key}, user_id={message.from_user.id}")
    except Exception as e:
        await message.answer(f"Ошибка отключения уведомлений: {str(e)}. Проверьте /debug.")
        logger.error(f"Ошибка /disable_notifications: {str(e)}, user_id={message.from_user.id}")

@router.message(Command("debug"))
async def debug_sheets(message: Message):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("Доступ запрещен.")
        logger.info(f"Доступ запрещен для /debug: user_id={message.from_user.id}")
        return
    try:
        spreadsheet = await async_sheets_call(sheets_service.spreadsheets().get, spreadsheetId=SHEET_NAME)
        sheet_names = [sheet["properties"]["title"] for sheet in spreadsheet.get("sheets", [])]
        response = [f"Google Sheet ID: {SHEET_NAME}", "Листы:"]
        for sheet in sheet_names:
            result = await async_sheets_call(
                sheets_service.spreadsheets().values().get,
                spreadsheetId=SHEET_NAME, range=f"{sheet}!A1:Z1"
            )
            headers = result.get("values", [[]])[0]
            response.append(f"- {sheet}: {', '.join(str(h) for h in headers) if headers else 'пусто'}")
        await message.answer("\n".join(response))
        logger.info(f"Команда /debug выполнена: user_id={message.from_user.id}")
    except HttpError as e:
        await message.answer(f"Ошибка доступа к Google Sheets: {e.status_code} - {e.reason}")
        logger.error(f"Ошибка /debug: {e.status_code} - {e.reason}, user_id={message.from_user.id}")
    except Exception as e:
        await message.answer(f"Неожиданная ошибка: {str(e)}")
        logger.error(f"Неожиданная ошибка /debug: {str(e)}, user_id={message.from_user.id}")

@router.message(Command("add_user"))
async def add_user(message: types.Message):
    if not await is_user_allowed(message.from_user.id) or message.from_user.id != YOUR_ADMIN_ID:
        await message.answer("🚫 Доступ запрещен. Только администратор может добавлять пользователей.")
        logger.info(f"Доступ запрещен для /add_user: user_id={message.from_user.id}")
        return
    try:
        args = message.text.split(None, 1)
        if len(args) < 2:
            await message.answer("❌ Укажите Telegram ID и Имя Фамилия: /add_user [Telegram ID] [Имя Фамилия]")
            logger.info(f"Некорректный формат /add_user: text={message.text}, user_id={message.from_user.id}")
            return
        parts = args[1].split(None, 1)
        if len(parts) < 2:
            await message.answer("❌ Укажите Telegram ID и Имя Фамилия: /add_user [Telegram ID] [Имя Фамилия]")
            return
        user_id_str, user_name = parts[0], parts[1].strip()
        if not user_id_str.isdigit():
            await message.answer("❌ Telegram ID должен содержать только цифры.")
            logger.info(f"Некорректный Telegram ID: {user_id_str}, user_id={message.from_user.id}")
            return
        if not user_name:
            await message.answer("❌ Имя Фамилия не может быть пустым.")
            return
        user_id = int(user_id_str)

        result = await async_sheets_call(
            sheets_service.spreadsheets().values().get,
            spreadsheetId=SHEET_NAME, range="AllowedUsers!A:B"
        )
        allowed_users = [(int(row[0]), row[1] if len(row) > 1 else "") for row in result.get("values", [])[1:] if row and row[0].isdigit()]
        if any(uid == user_id for uid, _ in allowed_users):
            await message.answer("✅ Пользователь уже в списке.")
            logger.info(f"Пользователь уже в списке: {user_id}, user_id={message.from_user.id}")
            return

        await async_sheets_call(
            sheets_service.spreadsheets().values().append,
            spreadsheetId=SHEET_NAME,
            range="AllowedUsers!A:B",
            valueInputOption="RAW",
            body={"values": [[user_id_str, user_name]]}
        )

        from utils import cache_set  # Invalidate
        await cache_set("allowed_users_list", None)

        await message.answer(f"✅ Пользователь {user_id} ({user_name}) добавлен.")
        logger.info(f"Пользователь добавлен: {user_id}, name={user_name}, user_id={message.from_user.id}")
    except HttpError as e:
        await message.answer(f"❌ Ошибка добавления пользователя в Google Sheets: {e.status_code} - {e.reason}.")
        logger.error(f"Ошибка /add_user: {e.status_code} - {e.reason}, user_id={message.from_user.id}")
    except Exception as e:
        await message.answer(f"❌ Неожиданная ошибка: {str(e)}.")
        logger.error(f"Неожиданная ошибка /add_user: {str(e)}, user_id={message.from_user.id}")

@router.message(Command("remove_user"))
async def remove_user(message: types.Message):
    if not await is_user_allowed(message.from_user.id) or message.from_user.id != YOUR_ADMIN_ID:
        await message.answer("🚫 Доступ запрещен. Только администратор может удалять пользователей.")
        logger.info(f"Доступ запрещен для /remove_user: user_id={message.from_user.id}")
        return
    try:
        args = message.text.split(None, 1)
        if len(args) < 2:
            await message.answer("❌ Укажите Telegram ID или Имя Фамилия: /remove_user [Telegram ID или Имя Фамилия]")
            logger.info(f"Некорректный формат /remove_user: text={message.text}, user_id={message.from_user.id}")
            return
        identifier = args[1].strip()

        result = await async_sheets_call(
            sheets_service.spreadsheets().values().get,
            spreadsheetId=SHEET_NAME, range="AllowedUsers!A:B"
        )
        rows = result.get("values", [])
        
        if len(rows) <= 1:
            await message.answer("❌ Список пользователей пуст.")
            logger.info(f"Список пуст при попытке удалить: {identifier}, user_id={message.from_user.id}")
            return

        header = rows[0] if rows else ["Users", "Name"]
        data_rows = rows[1:]

        is_digit = identifier.isdigit()
        filtered_rows = []
        removed = False
        for row in data_rows:
            if not row or not row[0].isdigit():
                continue
            row_id, row_name = row[0], row[1] if len(row) > 1 else ""
            if (is_digit and row_id == identifier) or (not is_digit and row_name.strip() == identifier):
                removed = True
                continue
            filtered_rows.append(row)

        if not removed:
            await message.answer(f"✅ Пользователь {identifier} не найден в списке.")
            logger.info(f"Пользователь не найден: {identifier}, user_id={message.from_user.id}")
            return

        await async_sheets_call(
            sheets_service.spreadsheets().values().clear,
            spreadsheetId=SHEET_NAME,
            range="AllowedUsers!A:B"
        )
        new_values = [header] + filtered_rows
        await async_sheets_call(
            sheets_service.spreadsheets().values().update,
            spreadsheetId=SHEET_NAME,
            range="AllowedUsers!A1",
            valueInputOption="RAW",
            body={"values": new_values}
        )

        from utils import cache_set
        await cache_set("allowed_users_list", None)

        await message.answer(f"✅ Пользователь {identifier} удален из таблицы.")
        logger.info(f"Пользователь удален: {identifier}, user_id={message.from_user.id}")

    except HttpError as e:
        await message.answer(f"❌ Ошибка работы с Google Sheets: {e.status_code} - {e.reason}.")
        logger.error(f"Ошибка /remove_user: {e.status_code} - {e.reason}, user_id={message.from_user.id}")
    except Exception as e:
        await message.answer(f"❌ Неожиданная ошибка: {str(e)}.")
        logger.error(f"Неожиданная ошибка /remove_user: {str(e)}, user_id={message.from_user.id}")

@router.message(Command("summary"))
async def summary_report(message: Message):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("Доступ запрещен.")
        logger.info(f"Доступ запрещен для /summary: user_id={message.from_user.id}")
        return
    try:
        result = await async_sheets_call(
            sheets_service.spreadsheets().values().get,
            spreadsheetId=SHEET_NAME, range="Чеки!A:L"
        )
        receipts = result.get("values", [])[1:]
        summary = {}
        for row in receipts:
            if len(row) < 9:
                continue
            date_str = row[1] if row[1] else ""
            try:
                if date_str:
                    dt = datetime.strptime(date_str, "%d.%m.%Y")
                    month = dt.strftime("%Y-%m")
                else:
                    month = "Неизвестно"
                amount = safe_float(row[2])
                if amount == 0:
                    continue
            except (ValueError, IndexError) as e:
                logger.warning(f"Некорректная дата/сумма в row {row}: {e}")
                continue
            user_id = row[5] if row[5] else "Неизвестно"
            store = row[6] if row[6] else "Неизвестно"
            receipt_type = row[11] if len(row) > 11 else "Неизвестно"
            
            if month not in summary:
                summary[month] = {"total_amount": 0.0, "users": {}, "stores": {}, "types": {}}
            summary[month]["total_amount"] += amount
            summary[month]["users"].setdefault(user_id, 0.0)
            summary[month]["users"][user_id] += amount
            summary[month]["stores"].setdefault(store, 0.0)
            summary[month]["stores"][store] += amount
            summary[month]["types"].setdefault(receipt_type, 0.0)
            summary[month]["types"][receipt_type] += amount
        
        values = [["Месяц", "Общая сумма", "Пользователи", "Магазины", "Типы чека"]]
        for month, data in summary.items():
            users_str = "; ".join([f"{uid}: {amt:.2f}" for uid, amt in data["users"].items()])
            stores_str = "; ".join([f"{store}: {amt:.2f}" for store, amt in data["stores"].items()])
            types_str = "; ".join([f"{rtype}: {amt:.2f}" for rtype, amt in data["types"].items()])
            values.append([month, f"{data['total_amount']:.2f}", users_str, stores_str, types_str])
        
        await async_sheets_call(
            sheets_service.spreadsheets().values().update,
            spreadsheetId=SHEET_NAME,
            range="Summary!A:E",
            valueInputOption="RAW",
            body={"values": values}
        )
        
        response = "Сводный отчет:\n"
        for month, data in summary.items():
            response += f"\nМесяц: {month}\n"
            response += f"Общая сумма: {data['total_amount']:.2f} RUB\n"
            response += "По пользователям:\n" + "\n".join([f"  {uid}: {amt:.2f} RUB" for uid, amt in data["users"].items()]) + "\n"
            response += "По магазинам:\n" + "\n".join([f"  {store}: {amt:.2f} RUB" for store, amt in data["stores"].items()]) + "\n"
            response += "По типам чека:\n" + "\n".join([f"  {rtype}: {amt:.2f} RUB" for rtype, amt in data["types"].items()]) + "\n"
        
        await message.answer(response)
        logger.info(f"Сводный отчет сгенерирован: user_id={message.from_user.id}")
    except HttpError as e:
        await message.answer(f"Ошибка генерации отчета из Google Sheets: {e.status_code} - {e.reason}. Проверьте /debug.")
        logger.error(f"Ошибка /summary: {e.status_code} - {e.reason}, user_id={message.from_user.id}")
    except Exception as e:
        await message.answer(f"Неожиданная ошибка генерации отчета: {str(e)}. Проверьте /debug.")
        logger.error(f"Неожиданная ошибка /summary: {str(e)}, user_id={message.from_user.id}")

@router.message(Command("listexclusions"))
async def list_exclusions_command(message: Message):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("🚫 Доступ запрещён.")
        logger.info(f"Доступ запрещён для /listexclusions: user_id={message.from_user.id}")
        return

    items = get_excluded_items()
    if items:
        content = "📋 *Исключённые позиции (case-insensitive):*\n" + "\n".join(f"• `{item}`" for item in items)
    else:
        content = "📋 *Исключённые позиции:* пусто"

    await message.answer(content, parse_mode="Markdown")
    logger.info(f"Пользователь {message.from_user.id} запросил список исключений")

@router.message(Command("addexclusion"))
async def add_exclusion_command(message: Message):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("🚫 Доступ запрещён.")
        logger.info(f"Доступ запрещён для /addexclusion: user_id={message.from_user.id}")
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer(
            "❗ Укажите название товара для добавления в исключения.\n"
            "Пример: `/addexclusion Доставка`",
            parse_mode="Markdown"
        )
        logger.info(f"Не указано название для /addexclusion: user_id={message.from_user.id}")
        return

    item = args[1].strip()
    if not item:
        await message.answer("❗ Название не может быть пустым.")
        return

    if add_excluded_item(item):
        await message.answer(f"✅ Добавлено в исключения: `{item}`", parse_mode="Markdown")
        logger.info(f"Добавлено исключение: '{item}', user_id={message.from_user.id}")
    else:
        await message.answer(f"⚠️ Уже есть в списке исключений: `{item}`", parse_mode="Markdown")
        logger.info(f"Попытка повторного добавления исключения: '{item}', user_id={message.from_user.id}")

@router.message(Command("removeexclusion"))
async def remove_exclusion_command(message: Message):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("🚫 Доступ запрещён.")
        logger.info(f"Доступ запрещён для /removeexclusion: user_id={message.from_user.id}")
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer(
            "❗ Укажите название товара для удаления из исключений.\n"
            "Пример: `/removeexclusion Доставка`",
            parse_mode="Markdown"
        )
        logger.info(f"Не указано название для /removeexclusion: user_id={message.from_user.id}")
        return

    item = args[1].strip()
    if not item:
        await message.answer("❗ Название не может быть пустым.")
        return

    if remove_excluded_item(item):
        await message.answer(f"✅ Удалено из исключений: `{item}`", parse_mode="Markdown")
        logger.info(f"Удалено исключение: '{item}', user_id={message.from_user.id}")
    else:
        await message.answer(f"❌ Не найдено в списке исключений: `{item}`", parse_mode="Markdown")
        logger.info(f"Попытка удалить несуществующее исключение: '{item}', user_id={message.from_user.id}")

@router.message(Command("balance"))
async def get_balance(message: Message):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("🚫 Доступ запрещен.")
        logger.info(f"Доступ запрещен для /balance: user_id={message.from_user.id}")
        return

    loading_message = await message.answer("⌛ Получение баланса...")  # Короткий текст, чтобы пользователь видел прогресс
    try:
        # NOVOYE: Вызываем функцию с кэшем (по умолчанию force_refresh=False)
        balance_data = await get_monthly_balance()  # Автоматически: кэш или 1 запрос
        if balance_data:  # Если данные ок
            initial_balance = balance_data.get("initial_balance", 0.0)
            spent = abs(balance_data.get("spent", 0.0))  # abs, если минус
            returned = balance_data.get("returned", 0.0)
            balance = balance_data.get("balance", 0.0)

            # Дата обновления: Оставляем отдельный запрос (малозначительный, ~0.1с; можно объединить позже)
            try:
                date_result = await async_sheets_call(
                    sheets_service.spreadsheets().values().get,
                    spreadsheetId=SHEET_NAME, range="Сводка!A1"  # Только A1 для даты
                )
                update_date = date_result.get("values", [[datetime.now().strftime("%d.%m.%Y")]])[0][0]
            except Exception:
                update_date = datetime.now().strftime("%d.%m.%Y")  # Fallback текущая дата
                logger.warning("Не удалось получить дату из A1, используем текущую")

            # Формируем ответ (как раньше)
            await loading_message.edit_text(
                f"💸 Баланс на {update_date}:\n"
                f"💰 Начальный баланс: {initial_balance:.2f} RUB\n"
                f"➖ Потрачено: {spent:.2f} RUB\n"
                f"➕ Возвращено: {returned:.2f} RUB\n"
                f"🟰 Остаток: {balance:.2f} RUB",
                parse_mode="Markdown"  # Для форматирования
            )
            logger.info(  # Лог для мониторинга
                f"Баланс выдан: initial_balance={initial_balance}, spent={spent}, returned={returned}, balance={balance}, user_id={message.from_user.id}"
            )
        else:
            await loading_message.edit_text("❌ Ошибка получения данных о балансе.")
            logger.error(f"Ошибка получения баланса: user_id={message.from_user.id}")
    except Exception as e:
        await loading_message.edit_text(f"❌ Неожиданная ошибка: {str(e)}. Проверьте /debug.")
        logger.error(f"Неожиданная ошибка /balance: {str(e)}, user_id={message.from_user.id}")

@router.message(Command("clear_cache"))
async def clear_cache(message: Message):
    if not await is_user_allowed(message.from_user.id) or message.from_user.id != YOUR_ADMIN_ID:
        await message.answer("🚫 Доступ запрещен. Только администратор.")
        logger.info(f"Доступ запрещен для /clear_cache: user_id={message.from_user.id}")
        return
    try:
        # Clear fiscal
        await redis_client.delete("fiscal_docs_set")
        # Clear allowed (optional)
        await redis_client.delete("allowed_users_list")
        # Clear notified (optional, large?)
        # await redis_client.delete("notified_items")  # Uncomment if need full reset
        await message.answer("✅ Кэш очищен: fiscal_docs_set (и allowed). Проверьте /add.")
        logger.info(f"Кэш очищен: user_id={message.from_user.id}")
    except Exception as e:
        await message.answer(f"❌ Ошибка очистки кэша: {str(e)}.")
        logger.error(f"Ошибка /clear_cache: {str(e)}, user_id={message.from_user.id}")

        from utils import redis_client  # Add import if not

@router.message(Command("flush_cache"))
async def flush_cache(message: Message):
    if not await is_user_allowed(message.from_user.id) or message.from_user.id != YOUR_ADMIN_ID:
        await message.answer("🚫 Доступ запрещен. Только администратор.")
        logger.info(f"Доступ запрещен для /flush_cache: user_id={message.from_user.id}")
        return
    try:
        # Nuclear: Clear all keys (or specific)
        keys_to_del = await redis_client.keys("*")  # All keys
        deleted = await redis_client.delete(*keys_to_del)
        await message.answer(f"✅ Полная очистка кэша: удалено {deleted} ключей (all). Проверьте /add или /balance.")
        logger.info(f"Full cache flush: deleted {deleted} keys, user_id={message.from_user.id}")
        
        # Optional: Test refresh
        test_docs = await is_fiscal_doc_unique("test_flush")  # Force refresh, should unique
        await message.answer(f"Тест unique 'test_flush': {test_docs} (should be True).")
    except Exception as e:
        await message.answer(f"❌ Ошибка очистки: {str(e)}.")
        logger.error(f"Ошибка /flush_cache: {str(e)}, user_id={message.from_user.id}")


from sheets import sheets_service, async_sheets_call, SHEET_NAME  # Add imports

@router.message(Command("clear_sheet"))
async def clear_sheet(message: Message):
    if message.from_user.id != YOUR_ADMIN_ID:  # Admin only
        await message.answer("🚫 Доступ запрещен. Только администратор.")
        return
    try:
        # Clear data in Чеки!A2:Q1000 (keep header row1)
        await async_sheets_call(
            sheets_service.spreadsheets().values().clear,
            spreadsheetId=SHEET_NAME, range="Чеки!A2:Q1000"  # Clear all data below header
        )
        # Optional: Clear Сводка data (A2:E1000)
        await async_sheets_call(
            sheets_service.spreadsheets().values().clear,
            spreadsheetId=SHEET_NAME, range="Сводка!A2:E1000"
        )
        await message.answer("✅ Листы 'Чеки' и 'Сводка' очищены (data rows deleted, headers kept). Проверьте /add или /debug.")
        logger.info(f"Sheet cleared by admin user_id={message.from_user.id}")
    except Exception as e:
        await message.answer(f"❌ Ошибка очистки листа: {str(e)}.")
        logger.error(f"Ошибка /clear_sheet: {str(e)}")

# Тестовая команда
@router.message(Command("test_group"))
async def test_group_notification(message: Message, bot: Bot):
    if not GROUP_CHAT_ID:
        await message.answer("❌ GROUP_CHAT_ID не задан в конфигурации.")
        logger.error("GROUP_CHAT_ID не задан")
        return

    test_message = "🔔 Тестовое уведомление в групповой чат!"
    try:
        logger.debug(f"Тест отправки в групповой чат, GROUP_CHAT_ID={GROUP_CHAT_ID}")
        await bot.send_message(chat_id=GROUP_CHAT_ID, text=test_message)
        logger.info(f"✅ Тестовое уведомление отправлено в групповой чат, chat_id={GROUP_CHAT_ID}")
        await message.answer("✅ Тестовое уведомление отправлено в групповой чат.")
    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)
        logger.error(f"❌ Ошибка отправки тестового уведомления: {error_type}: {error_msg}, chat_id={GROUP_CHAT_ID}")
        await message.answer(f"❌ Ошибка отправки: {error_type}: {error_msg}")


# Тестовая команда для получения ID чата
@router.message(Command("getchatid"))
async def get_chat_id(message: Message, bot: Bot):
    chat_id = message.chat.id
    chat_type = message.chat.type
    chat_title = message.chat.title if chat_type in ["group", "supergroup"] else "Личный чат"
    
    response = f"📌 ID текущего чата: `{chat_id}`\n📋 Тип чата: {chat_type}\n🏷 Название: {chat_title}"
    await message.answer(response, parse_mode="Markdown")

# 🚨 ДОБАВИТЬ В commands.py:

@router.message(Command("pending_stats"))
async def show_pending_stats(message: Message):
    """Показать статистику pending задач (только для админов)"""
    if not await is_user_allowed(message.from_user.id) or message.from_user.id != YOUR_ADMIN_ID:
        await message.answer("🚫 Доступ запрещен. Только администратор.")
        logger.info(f"Доступ запрещен для /pending_stats: user_id={message.from_user.id}")
        return
        
    try:
        from utils import get_pending_stats  # Импорт функции
        
        stats = await get_pending_stats()
        
        text = (
            "📊 Статистика pending задач:\n"
            f"• Всего задач: {stats['total']}\n"
            "• По типам:\n"
        )
        
        for check_type, count in stats['by_type'].items():
            text += f"  - {check_type}: {count}\n"
            
        if stats['old_tasks']:
            text += f"\n⚠️ Старых задач (>1ч): {len(stats['old_tasks'])}\n"
            for task in stats['old_tasks'][:5]:  # Показываем первые 5
                text += f"  - {task['key']}: {task['age_hours']:.1f}ч, {task['retries']} попыток\n"
            if len(stats['old_tasks']) > 5:
                text += f"  ... и еще {len(stats['old_tasks']) - 5}\n"
        else:
            text += "\n✅ Нет старых задач"
            
        await message.answer(text)
        logger.info(f"Статистика pending показана: total={stats['total']}, user_id={message.from_user.id}")
        
    except Exception as e:
        await message.answer(f"❌ Ошибка получения статистики: {str(e)}")
        logger.error(f"Ошибка /pending_stats: {str(e)}, user_id={message.from_user.id}")

@router.message(Command("cleanup_old_tasks"))
async def cleanup_old_tasks_command(message: Message):
    """Очистка устаревших pending задач (только для админов)"""
    if not await is_user_allowed(message.from_user.id) or message.from_user.id != YOUR_ADMIN_ID:
        await message.answer("🚫 Доступ запрещен. Только администратор.")
        logger.info(f"Доступ запрещен для /cleanup_old_tasks: user_id={message.from_user.id}")
        return
        
    try:
        from utils import get_pending_stats, remove_pending
        
        stats = await get_pending_stats()
        cleaned = 0
        
        for task in stats['old_tasks']:
            fiscal_key = task['key'].replace("pending:", "")
            await remove_pending(fiscal_key)
            cleaned += 1
            
        await message.answer(f"✅ Очищено {cleaned} старых задач из {len(stats['old_tasks'])}")
        logger.info(f"Очищено старых задач: {cleaned}, user_id={message.from_user.id}")
        
    except Exception as e:
        await message.answer(f"❌ Ошибка очистки: {str(e)}")
        logger.error(f"Ошибка /cleanup_old_tasks: {str(e)}, user_id={message.from_user.id}")