from aiogram import Router, Bot
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, ReplyKeyboardRemove, ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext
from sheets import sheets_service, is_user_allowed
from config import SHEET_NAME, PROVERKACHEKA_TOKEN, YOUR_ADMIN_ID
from exceptions import (
    get_excluded_items,
    add_excluded_item,
    remove_excluded_item
)
from googleapiclient.errors import HttpError
import logging
import aiohttp

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
        "*Доступные команды:*\n"
        "💰 `/balance` — Показать текущий баланс\n"
        "📥 `/add` — Добавить чек по QR-коду\n"
        #"✍️ `/add_manual` — Добавить чек вручную\n"
        "✅ `/expenses` — Подтвердить доставку\n"
        "🔙 `/return` — Обработать возврат\n"
        #"🔔 `/disable_notifications [ФД_индекс]` — Отключить уведомления\n"
    )
    logger.info(f"Команда /start выполнена: user_id={message.from_user.id}")
    
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
        sheets_service.spreadsheets().get(spreadsheetId=SHEET_NAME).execute()
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
        from notifications import notified_items
        notified_items.add(notification_key)
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
        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=SHEET_NAME).execute()
        sheet_names = [sheet["properties"]["title"] for sheet in spreadsheet.get("sheets", [])]
        response = [f"Google Sheet ID: {SHEET_NAME}", "Листы:"]
        for sheet in sheet_names:
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=SHEET_NAME, range=f"{sheet}!A1:Z1"
            ).execute()
            headers = result.get("values", [[]])[0]
            response.append(f"- {sheet}: {', '.join(headers) if headers else 'пусто'}")
        await message.answer("\n".join(response))
        logger.info(f"Команда /debug выполнена: user_id={message.from_user.id}")
    except HttpError as e:
        await message.answer(f"Ошибка доступа к Google Sheets: {e.status_code} - {e.reason}")
        logger.error(f"Ошибка /debug: {e.status_code} - {e.reason}, user_id={message.from_user.id}")
    except Exception as e:
        await message.answer(f"Неожиданная ошибка: {str(e)}")
        logger.error(f"Неожиданная ошибка /debug: {str(e)}, user_id={message.from_user.id}")

@router.message(Command("add_user"))
async def add_user(message: Message):
    if not await is_user_allowed(message.from_user.id) or message.from_user.id != YOUR_ADMIN_ID:
        await message.answer("🚫 Доступ запрещен. Только администратор может добавлять пользователей.")
        logger.info(f"Доступ запрещен для /add_user: user_id={message.from_user.id}")
        return
    try:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.answer("❌ Укажите Telegram ID: /add_user [Telegram ID]")
            logger.info(f"Некорректный формат /add_user: text={message.text}, user_id={message.from_user.id}")
            return
        user_id_str = args[1]
        if not user_id_str.isdigit():
            await message.answer("❌ Telegram ID должен содержать только цифры.")
            logger.info(f"Некорректный Telegram ID: {user_id_str}, user_id={message.from_user.id}")
            return
        user_id = int(user_id_str)
        # Получаем текущий список
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="AllowedUsers!A:A"
        ).execute()
        allowed_users = [int(row[0]) for row in result.get("values", [])[1:] if row and row[0].isdigit()]
        if user_id in allowed_users:
            await message.answer("✅ Пользователь уже в списке.")
            logger.info(f"Пользователь уже в списке: {user_id}, user_id={message.from_user.id}")
            return
        # Добавляем нового пользователя
        sheets_service.spreadsheets().values().append(
            spreadsheetId=SHEET_NAME,
            range="AllowedUsers!A:A",
            valueInputOption="RAW",
            body={"values": [[user_id_str]]}
        ).execute()
        await message.answer(f"✅ Пользователь {user_id} добавлен.")
        logger.info(f"Пользователь добавлен: {user_id}, user_id={message.from_user.id}")
    except HttpError as e:
        await message.answer(f"❌ Ошибка добавления пользователя в Google Sheets: {e.status_code} - {e.reason}.")
        logger.error(f"Ошибка /add_user: {e.status_code} - {e.reason}, user_id={message.from_user.id}")
    except Exception as e:
        await message.answer(f"❌ Неожиданная ошибка: {str(e)}.")
        logger.error(f"Неожиданная ошибка /add_user: {str(e)}, user_id={message.from_user.id}")

@router.message(Command("remove_user"))
async def remove_user(message: Message):
    if not await is_user_allowed(message.from_user.id) or message.from_user.id != YOUR_ADMIN_ID:
        await message.answer("🚫 Доступ запрещен. Только администратор может удалять пользователей.")
        logger.info(f"Доступ запрещен для /remove_user: user_id={message.from_user.id}")
        return
    try:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.answer("❌ Укажите Telegram ID: /remove_user [Telegram ID]")
            logger.info(f"Некорректный формат /remove_user: text={message.text}, user_id={message.from_user.id}")
            return
        user_id_str = args[1]
        if not user_id_str.isdigit():
            await message.answer("❌ Telegram ID должен содержать только цифры.")
            logger.info(f"Некорректный Telegram ID: {user_id_str}, user_id={message.from_user.id}")
            return
        user_id = int(user_id_str)

        # Получаем текущий список
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="AllowedUsers!A:A"
        ).execute()
        rows = result.get("values", [])
        
        if len(rows) == 0:
            await message.answer("❌ Список пользователей пуст.")
            logger.info(f"Список пуст при попытке удалить: {user_id}, user_id={message.from_user.id}")
            return

        # Проверяем, есть ли заголовок
        header = rows[0] if rows else ["User ID"]  # предполагаем заголовок
        data_rows = rows[1:]

        # Фильтруем: оставляем только тех, кто не совпадает с user_id
        filtered_rows = [row for row in data_rows if not (row and row[0].isdigit() and int(row[0]) == user_id)]

        if len(data_rows) == len(filtered_rows):
            await message.answer("✅ Пользователь не найден в списке.")
            logger.info(f"Пользователь не найден: {user_id}, user_id={message.from_user.id}")
            return

        # Очищаем весь диапазон A:A, чтобы гарантировать удаление "хвостов"
        sheets_service.spreadsheets().values().clear(
            spreadsheetId=SHEET_NAME,
            range="AllowedUsers!A:A"
        ).execute()

        # Подготавливаем новые данные: заголовок + отфильтрованные строки
        new_values = [header] + filtered_rows

        # Записываем обратно
        sheets_service.spreadsheets().values().update(
            spreadsheetId=SHEET_NAME,
            range="AllowedUsers!A1",
            valueInputOption="RAW",
            body={"values": new_values}
        ).execute()

        await message.answer(f"✅ Пользователь {user_id} удален из таблицы.")
        logger.info(f"Пользователь удален: {user_id}, user_id={message.from_user.id}")

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
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Чеки!A:L"
        ).execute()
        receipts = result.get("values", [])[1:]
        summary = {}
        for row in receipts:
            date = row[0]
            month = date[:7] if date else "Неизвестно"
            try:
                amount = float(row[1]) if row[1] else 0
            except (ValueError, IndexError):
                logger.warning(f"Некорректная сумма в чеке: row={row}, user_id={message.from_user.id}")
                continue
            user_id = row[2] if row[2] else "Неизвестно"
            store = row[3] if row[3] else "Неизвестно"
            receipt_type = row[8] if row[8] else "Неизвестно"
            
            if month not in summary:
                summary[month] = {"total_amount": 0, "users": {}, "stores": {}, "types": {}}
            summary[month]["total_amount"] += amount
            summary[month]["users"].setdefault(user_id, 0)
            summary[month]["users"][user_id] += amount
            summary[month]["stores"].setdefault(store, 0)
            summary[month]["stores"][store] += amount
            summary[month]["types"].setdefault(receipt_type, 0)
            summary[month]["types"][receipt_type] += amount
        
        values = [["Месяц", "Общая сумма", "Пользователи", "Магазины", "Типы чека"]]
        for month, data in summary.items():
            users_str = "; ".join([f"{uid}: {amt}" for uid, amt in data["users"].items()])
            stores_str = "; ".join([f"{store}: {amt}" for store, amt in data["stores"].items()])
            types_str = "; ".join([f"{rtype}: {amt}" for rtype, amt in data["types"].items()])
            values.append([month, data["total_amount"], users_str, stores_str, types_str])
        
        sheets_service.spreadsheets().values().update(
            spreadsheetId=SHEET_NAME,
            range="Summary!A:E",
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
        
        response = "Сводный отчет:\n"
        for month, data in summary.items():
            response += f"\nМесяц: {month}\n"
            response += f"Общая сумма: {data['total_amount']} RUB\n"
            response += "По пользователям:\n" + "\n".join([f"  {uid}: {amt} RUB" for uid, amt in data["users"].items()]) + "\n"
            response += "По магазинам:\n" + "\n".join([f"  {store}: {amt} RUB" for store, amt in data["stores"].items()]) + "\n"
            response += "По типам чека:\n" + "\n".join([f"  {rtype}: {amt} RUB" for rtype, amt in data["types"].items()]) + "\n"
        
        await message.answer(response)
        logger.info(f"Сводный отчет сгенерирован: user_id={message.from_user.id}")
    except HttpError as e:
        await message.answer(f"Ошибка генерации отчета из Google Sheets: {e.status_code} - {e.reason}. Проверьте /debug.")
        logger.error(f"Ошибка /summary: {e.status_code} - {e.reason}, user_id={message.from_user.id}")
    except Exception as e:
        await message.answer(f"Неожиданная ошибка генерации отчета: {str(e)}. Проверьте /debug.")
        logger.error(f"Неожиданная ошибка /summary: {str(e)}, user_id={message.from_user.id}")
        
        
# ... (ваш существующий импорт и router)

@router.message(Command("listexclusions"))
async def list_exclusions_command(message: Message):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("🚫 Доступ запрещён.")
        logger.info(f"Доступ запрещён для /listexclusions: user_id={message.from_user.id}")
        return

    items = get_excluded_items()
    if items:
        content = "📋 *Исключённые позиции (полное совпадение):*\n" + "\n".join(f"• `{item}`" for item in items)
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