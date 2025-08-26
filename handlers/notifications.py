from aiogram import Bot
from googleapiclient.errors import HttpError
from sheets import sheets_service
from config import SHEET_NAME, YOUR_ADMIN_ID, USER_ID_1, USER_ID_2, USERS
from datetime import datetime, timedelta
import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from utils import redis_client, cache_get, cache_set

logger = logging.getLogger("AccountingBot")
scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
notified_items = set()

async def get_allowed_users() -> list:
    """Получает список разрешённых пользователей из Redis или Google Sheets."""
    cache_key = "allowed_users"
    cached_users = await cache_get(cache_key)
    if cached_users is not None:
        logger.info("Cache hit for allowed_users")
        return cached_users

    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="AllowedUsers!A:A"
        ).execute()
        allowed_users = [int(uid) for row in result.get("values", [])[1:] for uid in row if uid and uid.isdigit()]
        await cache_set(cache_key, allowed_users, expire=86400)  # Кэш на 24 часа
        logger.info("Allowed users cached")
        return allowed_users
    except HttpError as e:
        logger.error(f"Ошибка получения AllowedUsers: {e.status_code} - {e.reason}")
        return []
    except Exception as e:
        logger.error(f"Неожиданная ошибка получения AllowedUsers: {str(e)}")
        return []

async def send_notifications(bot: Bot):
    """Отправляет уведомления о чеках со статусом 'Ожидает'."""
    # Проверяем, будний ли день
    today = datetime.now().weekday()  # 0=понедельник, 6=воскресенье
    if today >= 5:  # Суббота или воскресенье
        logger.info("Уведомления не отправляются в выходные")
        return

    try:
        cache_key = "notifications:pending_checks"
        cached_checks = await cache_get(cache_key)
        if cached_checks is not None:
            logger.info("Cache hit for pending checks")
            checks = cached_checks
        else:
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=SHEET_NAME, range="Чеки!A:G,K"  # A:G,K для оптимизации
            ).execute()
            checks = [
                row + [str(index)] for index, row in enumerate(result.get("values", [])[1:], start=2)
                if len(row) > 6 and row[6].lower() == "ожидает" and len(row) > 5 and row[5]
            ]
            await cache_set(cache_key, checks, expire=10800)  # Кэш на 3 часа
            logger.info("Pending checks cached")

        if not checks:
            logger.info("Нет чеков для уведомлений")
            return

        today_date = datetime.now().strftime("%d.%m.%Y")
        three_days_ago = (datetime.now() - timedelta(days=3)).strftime("%d.%m.%Y")
        allowed_users = await get_allowed_users()
        if not allowed_users:
            logger.warning("Список разрешённых пользователей пуст")
            return

        for row in checks:
            delivery_date = row[5]  # Столбец F
            fiscal_doc = row[7] if len(row) > 7 else ""  # Столбец K
            item_name = row[4] if len(row) > 4 else ""  # Столбец E
            user_name = row[2] if len(row) > 2 else ""  # Столбец C (имя пользователя)
            notification_key = f"{fiscal_doc}_{row[-1]}"  # Индекс строки

            if delivery_date in [today_date, three_days_ago] and notification_key not in notified_items:
                # Получатели: админ, два фиксированных пользователя, пользователь чека
                recipients = {YOUR_ADMIN_ID, USER_ID_1, USER_ID_2}  # Множество исключает дубли
                check_user_id = USERS.get(user_name)
                if check_user_id and check_user_id in allowed_users:
                    recipients.add(check_user_id)
                else:
                    logger.warning(f"Пользователь {user_name} не найден в USERS или не в allowed_users")

                for user_id in recipients:
                    try:
                        await bot.send_message(
                            user_id,
                            f"Напоминание: товар {item_name} из чека {fiscal_doc} ожидает доставку {delivery_date}. "
                            f"Отключить: /disable_notifications {notification_key}. "
                            f"Подтвердить доставку: /expenses"
                        )
                        logger.info(f"Уведомление отправлено: fiscal_doc={fiscal_doc}, item={item_name}, user_id={user_id}")
                    except Exception as e:
                        logger.error(f"Ошибка отправки уведомления пользователю {user_id}: {str(e)}")
                notified_items.add(notification_key)

    except HttpError as e:
        logger.error(f"Ошибка получения чеков: {e.status_code} - {e.reason}")
        await asyncio.sleep(60)  # Retry через 1 минуту
    except Exception as e:
        logger.error(f"Неожиданная ошибка отправки уведомлений: {str(e)}")
        await asyncio.sleep(60)

def start_notifications(bot: Bot):
    """Запускает уведомления по расписанию в 15:00 МСК в будние дни."""
    scheduler.add_job(
        send_notifications,
        trigger=CronTrigger(day_of_week="mon-fri", hour=15, minute=0, timezone="Europe/Moscow"),
        args=[bot],
        max_instances=1
    )
    scheduler.start()
    logger.info("Уведомления запущены: будние дни, 15:00 МСК")