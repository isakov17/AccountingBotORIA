from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from googleapiclient.errors import HttpError
from sheets import sheets_service, get_monthly_balance, async_sheets_call  # Async
from config import SHEET_NAME, GROUP_CHAT_ID
from datetime import datetime, timedelta
import asyncio
import logging
import os  # Для ENV в start_notifications
import random  # Для rate limit
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger  # Для prod
from apscheduler.triggers.interval import IntervalTrigger  # Test
from utils import safe_float, redis_client  # Redis for notified

logger = logging.getLogger("AccountingBot")
scheduler = AsyncIOScheduler(timezone="Europe/Moscow")

async def send_notification(
    bot: Bot,
    action: str,
    items: list[dict],
    user_name: str,
    fiscal_doc: str,
    delivery_date: str,
    balance: float,
    is_group: bool = False,
    chat_id: int = None
):
    """
    Объединённая отправка уведомления (user или group).
    """
    try:
        normalized_items = [
            {
                "name": item.get("name", "—"),
                "sum": safe_float(item.get("sum", 0)),
                "quantity": int(item.get("quantity", 1) or 1),
                "price": safe_float(item.get("price", item.get("sum", 0) / max(item.get("quantity", 1), 1))),  # Если no price
                "link": item.get("link", ""),
                "comment": item.get("comment", ""),
                "delivery_date": item.get("delivery_date", ""),
            }
            for item in items
        ]

        total_sum = sum(it["sum"] for it in normalized_items)
        total_positions = len(normalized_items)

        all_dates = [it["delivery_date"] for it in normalized_items if it["delivery_date"]]
        date_header = delivery_date
        if all_dates and len(set(all_dates)) == 1:
            date_header = all_dates[0]
        elif all_dates:
            date_header = "Разные даты"

        items_text = "\n".join(
            f"  • {it['name']} — {it['quantity']} шт. × {it['price']:.2f} ₽ (итого {it['sum']:.2f} ₽)"
            + (f"\n    📅 {it['delivery_date']}" if it['delivery_date'] else "")
            + (f"\n    🔗 {it['link']}" if it['link'] else "")
            + (f"\n    💬 {it['comment']}" if it['comment'] else "")
            for it in normalized_items
        )

        text = (
            f"{action}\n\n"
            f"👤 Пользователь: {user_name}\n"
            f"📑 Фискальный номер: {fiscal_doc}\n"
            f"📅 Дата доставки: {date_header}\n\n"
            f"🛒 Товары ({total_positions} шт.):\n{items_text}\n\n"
            f"📦 Всего позиций: {total_positions}\n"
            f"💰 Общая сумма: {total_sum:.2f} ₽\n"
            f"💳 Баланс: {balance:.2f} ₽"
        )

        reply_markup = None
        target_chat = GROUP_CHAT_ID if is_group else chat_id
        if is_group:
            reply_markup = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🚀 Открыть бота", url="https://t.me/TESTAccountingORIABot")]
                ]
            )

        await bot.send_message(target_chat, text, reply_markup=reply_markup)
        logger.info(f"Уведомление отправлено {'группе' if is_group else 'пользователю'}: {action}, чек={fiscal_doc}, chat={target_chat}")

    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления {'группе' if is_group else f'chat_id={chat_id}'}: {str(e)}")

async def send_notifications(bot: Bot):
    """Проверка таблицы и напоминания (async)."""
    logger.info("Начало выполнения send_notifications")
    today = datetime.now()
    if today.weekday() >= 5:  # Sat/Sun
        logger.info("Уведомления не отправляются в выходные")
        return

    try:
        result = await async_sheets_call(
            sheets_service.spreadsheets().values().get,
            spreadsheetId=SHEET_NAME, range="Чеки!A:Q"
        )
        rows = result.get("values", [])[1:]
        logger.info(f"Загружено {len(rows)} строк из Google Sheets")

        if not rows:
            return

        today_str = today.strftime("%d.%m.%Y")
        three_days_ago = (today - timedelta(days=3)).strftime("%d.%m.%Y")

        for idx, row in enumerate(rows, start=2):
            if len(row) < 17:
                continue

            fiscal_doc = row[12].strip() if row[12] else ""
            if not fiscal_doc:
                continue

            status = row[8].strip().lower() if row[8] else ""
            delivery_date = row[7].strip() if row[7] else ""

            if status != "ожидает" or delivery_date not in [today_str, three_days_ago]:
                continue

            notification_key = f"{fiscal_doc}_{idx}"
            if await redis_client.sismember("notified_items", notification_key):
                continue

            item_name = row[10].strip() if row[10] else ""
            item_sum = safe_float(row[2]) if row[2] else 0.0
            qty = int(row[4]) if row[4] else 1
            item_link = row[15].strip() if len(row) > 15 else ""
            item_comment = row[16].strip() if len(row) > 16 else ""

            items = [{
                "name": item_name,
                "sum": item_sum,
                "quantity": qty,
                "link": item_link,
                "comment": item_comment,
                "delivery_date": delivery_date
            }]

            user_name = row[5].strip() if row[5] else ""

            balance_data = await get_monthly_balance()
            balance = safe_float(balance_data.get("balance", 0.0)) if balance_data else 0.0

            delivery_date_header = delivery_date

            # Send group only (as per original)
            await send_notification(
                bot=bot,
                action="📦 Напоминание о доставке",
                items=items,
                user_name=user_name,
                fiscal_doc=fiscal_doc,
                delivery_date=delivery_date_header,
                balance=balance,
                is_group=True
            )
            
            # Rate limit
            await asyncio.sleep(random.uniform(1, 3))
            
            await redis_client.sadd("notified_items", notification_key)
            logger.info(f"Отправлено напоминание: fiscal_doc={fiscal_doc}, row={idx}, item={item_name[:50]}..., delivery_date={delivery_date}")

    except HttpError as e:
        logger.error(f"Ошибка получения чеков: {e.status_code} - {e.reason}")
        await asyncio.sleep(60)
    except Exception as e:
        logger.error(f"Неожиданная ошибка отправки уведомлений: {str(e)}")
        await asyncio.sleep(60)

def start_notifications(bot: Bot):
    # Prod: Cron mon-fri 15:00
    # Test: interval 1min (env? Hardcode)
    if os.getenv("ENV") == "prod":  # Assume .env ENV=prod
        trigger = CronTrigger(day_of_week="mon-fri", hour=15, minute=0, timezone="Europe/Moscow")
        logger.info("Уведомления: будние 15:00")
    else:
        trigger = IntervalTrigger(minutes=1)
        logger.info("Уведомления: тест 1min")
    
    scheduler.add_job(
        send_notifications,
        trigger=trigger,
        args=[bot],
        max_instances=1,
    )
    scheduler.start()