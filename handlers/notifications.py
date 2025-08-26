from aiogram import Bot
from googleapiclient.errors import HttpError
from sheets import sheets_service
from config import SHEET_NAME, YOUR_ADMIN_ID, USER_ID_1, USER_ID_2
from datetime import datetime, timedelta
import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from utils import redis_client, cache_get, cache_set

logger = logging.getLogger("AccountingBot")
scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
notified_items = set()

async def get_allowed_users() -> dict:
    cache_key = "allowed_users"
    cached_users = await cache_get(cache_key)
    if cached_users is not None:
        logger.info("Cache hit for allowed_users")
        return cached_users

    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="AllowedUsers!A:B"
        ).execute()
        users = {int(row[0]): row[1] for row in result.get("values", [])[1:] if row and row[0].isdigit()}
        await cache_set(cache_key, users, expire=86400)  # Кэш на 24 часа
        logger.info("Allowed users cached")
        return users
    except HttpError as e:
        logger.error(f"Ошибка получения AllowedUsers: {e.status_code} - {e.reason}")
        return {}
    except Exception as e:
        logger.error(f"Неожиданная ошибка получения AllowedUsers: {str(e)}")
        return {}

async def send_notifications(bot: Bot):
    today = datetime.now()
    if today.weekday() >= 5:  # Суббота или воскресенье
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
                spreadsheetId=SHEET_NAME, range="Чеки!A:M"
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

        today_date = today.strftime("%d.%m.%Y")
        three_days_ago = (today - timedelta(days=3)).strftime("%d.%m.%Y")
        allowed_users = await get_allowed_users()
        if not allowed_users:
            logger.warning("Список разрешённых пользователей пуст")
            return

        for row in checks:
            delivery_date = row[5]  # Столбец F
            fiscal_doc = row[10] if len(row) > 10 else ""  # Столбец K
            item_name = row[8] if len(row) > 8 else ""  # Столбец I
            user_name = row[3] if len(row) > 3 else ""  # Столбец D
            notification_key = f"{fiscal_doc}_{row[-1]}"  # Индекс строки

            if delivery_date in [today_date, three_days_ago] and notification_key not in notified_items:
                recipients = {YOUR_ADMIN_ID, USER_ID_1, USER_ID_2}
                check_user_id = next((uid for uid, name in allowed_users.items() if name == user_name), None)
                if check_user_id and check_user_id in allowed_users:
                    recipients.add(check_user_id)
                else:
                    logger.warning(f"Пользователь {user_name} не найден в AllowedUsers")

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
        await asyncio.sleep(60)
    except Exception as e:
        logger.error(f"Неожиданная ошибка отправки уведомлений: {str(e)}")
        await asyncio.sleep(60)

def start_notifications(bot: Bot):
    scheduler.add_job(
        send_notifications,
        trigger=CronTrigger(day_of_week="mon-fri", hour=15, minute=0, timezone="Europe/Moscow"),
        args=[bot],
        max_instances=1
    )
    scheduler.start()
    logger.info("Уведомления запущены: будние дни, 15:00 МСК")