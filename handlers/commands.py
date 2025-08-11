from aiogram import Router, Bot
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, ReplyKeyboardRemove, ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext
from sheets import sheets_service, is_user_allowed
from config import SHEET_NAME, PROVERKACHEKA_TOKEN
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
        "✍️ `/add_manual` — Добавить чек вручную\n"
        "✅ `/expenses` — Подтвердить доставку\n"
        "🔙 `/return [ФД]` — Обработать возврат (например, `/return 199977`)\n"
        "🔔 `/disable_notifications [ФД_индекс]` — Отключить уведомления\n"
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
    if not await is_user_allowed(message.from_user.id):
        await message.answer("Доступ запрещен.")
        logger.info(f"Доступ запрещен для /add_user: user_id={message.from_user.id}")
        return
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="AllowedUsers!A:A"
        ).execute()
        allowed_users = [int(uid) for row in result.get("values", [])[1:] for uid in row if uid and uid.isdigit()]
        if message.from_user.id != allowed_users[0]:
            await message.answer("Только первый пользователь может добавлять других.")
            logger.info(f"Доступ запрещен для добавления пользователей: user_id={message.from_user.id}")
            return
        user_id = message.text.split()[1]
        if not user_id.isdigit():
            await message.answer("Telegram ID должен содержать только цифры.")
            logger.info(f"Некорректный Telegram ID: {user_id}, user_id={message.from_user.id}")
            return
        user_id = int(user_id)
        if user_id in allowed_users:
            await message.answer("Пользователь уже в списке.")
            logger.info(f"Пользователь уже в списке: {user_id}, user_id={message.from_user.id}")
            return
        allowed_users.append(user_id)
        sheets_service.spreadsheets().values().update(
            spreadsheetId=SHEET_NAME,
            range="AllowedUsers!A:A",
            valueInputOption="RAW",
            body={"values": [[str(uid) for uid in allowed_users]]}
        ).execute()
        await message.answer(f"Пользователь {user_id} добавлен.")
        logger.info(f"Пользователь добавлен: {user_id}, user_id={message.from_user.id}")
    except (IndexError, ValueError):
        await message.answer("Укажите Telegram ID: /add_user [Telegram ID]")
        logger.info(f"Некорректный формат для /add_user: text={message.text}, user_id={message.from_user.id}")
    except HttpError as e:
        await message.answer(f"Ошибка добавления пользователя в Google Sheets: {e.status_code} - {e.reason}. Проверьте /debug.")
        logger.error(f"Ошибка /add_user: {e.status_code} - {e.reason}, user_id={message.from_user.id}")
    except Exception as e:
        await message.answer(f"Неожиданная ошибка: {str(e)}. Проверьте /debug.")
        logger.error(f"Неожиданная ошибка /add_user: {str(e)}, user_id={message.from_user.id}")

@router.message(Command("remove_user"))
async def remove_user(message: Message):
    if not await is_user_allowed(message.from_user.id):
        await message.answer("Доступ запрещен.")
        logger.info(f"Доступ запрещен для /remove_user: user_id={message.from_user.id}")
        return
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="AllowedUsers!A:A"
        ).execute()
        allowed_users = [int(uid) for row in result.get("values", [])[1:] for uid in row if uid and uid.isdigit()]
        if message.from_user.id != allowed_users[0]:
            await message.answer("Только первый пользователь может удалять других.")
            logger.info(f"Доступ запрещен для удаления пользователей: user_id={message.from_user.id}")
            return
        user_id = int(message.text.split()[1])
        if user_id not in allowed_users:
            await message.answer("Пользователь не в списке.")
            logger.info(f"Пользователь не в списке: {user_id}, user_id={message.from_user.id}")
            return
        allowed_users.remove(user_id)
        sheets_service.spreadsheets().values().update(
            spreadsheetId=SHEET_NAME,
            range="AllowedUsers!A:A",
            valueInputOption="RAW",
            body={"values": [[str(uid) for uid in allowed_users]]}
        ).execute()
        await message.answer(f"Пользователь {user_id} удален.")
        logger.info(f"Пользователь удален: {user_id}, user_id={message.from_user.id}")
    except (IndexError, ValueError):
        await message.answer("Укажите Telegram ID: /remove_user [Telegram ID]")
        logger.info(f"Некорректный формат для /remove_user: text={message.text}, user_id={message.from_user.id}")
    except HttpError as e:
        await message.answer(f"Ошибка удаления пользователя из Google Sheets: {e.status_code} - {e.reason}. Проверьте /debug.")
        logger.error(f"Ошибка /remove_user: {e.status_code} - {e.reason}, user_id={message.from_user.id}")
    except Exception as e:
        await message.answer(f"Неожиданная ошибка: {str(e)}. Проверьте /debug.")
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