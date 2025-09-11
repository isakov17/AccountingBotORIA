from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from googleapiclient.errors import HttpError
from sheets import sheets_service, get_monthly_balance
from config import SHEET_NAME, GROUP_CHAT_ID
from datetime import datetime, timedelta
import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from utils import safe_float

logger = logging.getLogger("AccountingBot")
scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
notified_items = set()


async def send_user_notification(
    bot: Bot,
    chat_id: int,
    action: str,
    items: list[dict],
    user_name: str,
    fiscal_doc: str,
    delivery_date: str,  # Для заголовка (первая или общая)
    balance: float,
):
    """Уведомление пользователю"""
    try:
        # ИСПРАВЛЕНИЕ: normalized_items с per-item link, comment, delivery_date (из items)
        normalized_items = [
            {
                "name": item.get("name", "—"),
                "sum": safe_float(item.get("sum", 0)),
                "quantity": int(item.get("quantity", 1) or 1),
                "link": item.get("link", ""),  # Per-item
                "comment": item.get("comment", ""),  # Per-item
                "delivery_date": item.get("delivery_date", ""),  # Per-item
            }
            for item in items
        ]

        total_sum = sum(it["sum"] for it in normalized_items)
        total_positions = len(normalized_items)  # ИСПРАВЛЕНИЕ: количество позиций (товаров), не sum qty

        # Для заголовка: если все даты одинаковые — показать общую; иначе "Разные даты"
        all_dates = [it["delivery_date"] for it in normalized_items if it["delivery_date"]]
        date_header = delivery_date  # Fallback на переданную
        if all_dates and len(set(all_dates)) == 1:
            date_header = all_dates[0]
        elif all_dates:
            date_header = "Разные даты"

        items_text = "\n".join(
            f"  • {it['name']} — {it['quantity']} шт. × {it['sum']:.2f} ₽"
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
            f"📦 Всего позиций: {total_positions}\n"  # ИСПРАВЛЕНИЕ: позиции, не qty
            f"💰 Общая сумма: {total_sum:.2f} ₽\n"
            f"💳 Баланс: {balance:.2f} ₽"
        )

        await bot.send_message(chat_id, text)
        logger.info(f"Уведомление отправлено пользователю: {action}, чек={fiscal_doc}, chat_id={chat_id}")

    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления пользователю: {str(e)}, chat_id={chat_id}")


async def send_group_notification(
    bot: Bot,
    action: str,
    items: list[dict],
    user_name: str,
    fiscal_doc: str,
    delivery_date: str,  # Для заголовка
    balance: float,
):
    """Уведомление в группу"""
    try:
        # ИСПРАВЛЕНИЕ: Аналогично user_notification
        normalized_items = [
            {
                "name": item.get("name", "—"),
                "sum": safe_float(item.get("sum", 0)),
                "quantity": int(item.get("quantity", 1) or 1),
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
            f"  • {it['name']} — {it['quantity']} шт. × {it['sum']:.2f} ₽"
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

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🚀 Открыть бота", url="https://t.me/TESTAccountingORIABot")]
            ]
        )

        await bot.send_message(GROUP_CHAT_ID, text, reply_markup=keyboard)
        logger.info(f"Уведомление отправлено: {action}, чек={fiscal_doc}")

    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления в группу: {str(e)}")


async def send_notifications(bot: Bot):
    """Проверка таблицы и напоминания"""
    logger.info("Начало выполнения send_notifications")
    today = datetime.now()
    if today.weekday() >= 5:
        logger.info("Уведомления не отправляются в выходные")
        return

    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Чеки!A:Q"
        ).execute()
        rows = result.get("values", [])[1:]  # Пропустить заголовок
        logger.info(f"Загружено {len(rows)} строк из Google Sheets")

        if not rows:
            return

        today_str = today.strftime("%d.%m.%Y")
        three_days_ago = (today - timedelta(days=3)).strftime("%d.%m.%Y")

        # ИСПРАВЛЕНИЕ: Цикл по всем строкам (не группировать), отправка per-item
        for idx, row in enumerate(rows, start=2):
            if len(row) < 17:  # Минимум до комментария (Q=16)
                continue

            fiscal_doc = row[12].strip() if row[12] else ""  # M Фискальный номер
            if not fiscal_doc:
                continue

            status = row[8].strip().lower() if row[8] else ""  # I Статус
            delivery_date = row[7].strip() if row[7] else ""  # H Дата доставки

            # ИСПРАВЛЕНИЕ: Условие только для этого товара: status="ожидает" И дата = today ИЛИ 3 дня назад
            if status != "ожидает" or delivery_date not in [today_str, three_days_ago]:
                continue

            notification_key = f"{fiscal_doc}_{idx}"
            if notification_key in notified_items:
                continue  # Уже напоминали для этого товара

            # Собираем данные для ЭТОГО товара (items = [one_item])
            item_name = row[10].strip() if row[10] else ""  # K Товар
            item_sum = safe_float(row[2]) if row[2] else 0.0  # C Сумма
            qty = int(row[4]) if row[4] else 1  # E Кол-во
            item_link = row[15].strip() if len(row) > 15 and row[15] else ""  # P Ссылка
            item_comment = row[16].strip() if len(row) > 16 and row[16] else ""  # Q Комментарий

            items = [{
                "name": item_name,
                "sum": item_sum,
                "quantity": qty,
                "link": item_link,
                "comment": item_comment,
                "delivery_date": delivery_date  # Per-item (здесь одна)
            }]

            user_name = row[5].strip() if row[5] else ""  # F Пользователь

            try:
                balance_data = await get_monthly_balance()
                balance = safe_float(balance_data.get("balance", 0.0)) if balance_data else 0.0
            except Exception as e:
                logger.error(f"Ошибка получения баланса: {str(e)}")
                balance = 0.0

            # ИСПРАВЛЕНИЕ: delivery_date_header = delivery_date (одна для этого товара)
            delivery_date_header = delivery_date

            # Отправляем уведомление для ЭТОГО товара
            await send_group_notification(
                bot=bot,
                action="📦 Напоминание о доставке",
                items=items,  # Список с одним элементом
                user_name=user_name,
                fiscal_doc=fiscal_doc,
                delivery_date=delivery_date_header,
                balance=balance,
            )
            notified_items.add(notification_key)
            logger.info(f"Отправлено напоминание: fiscal_doc={fiscal_doc}, row={idx}, item={item_name[:50]}..., delivery_date={delivery_date}")

    except HttpError as e:
        logger.error(f"Ошибка получения чеков: {e.status_code} - {e.reason}")
        await asyncio.sleep(60)
    except Exception as e:
        logger.error(f"Неожиданная ошибка отправки уведомлений: {str(e)}")
        await asyncio.sleep(60)


def start_notifications(bot: Bot):
    scheduler.add_job(
        send_notifications,
        trigger=IntervalTrigger(minutes=1),
        args=[bot],
        max_instances=1,
    )
    scheduler.start()
    logger.info("Уведомления запущены: каждая минута (тестовый режим)")

# Закомментированная часть остаётся как есть



# def start_notifications(bot: Bot):
#     scheduler.add_job(
#         send_notifications,
#         trigger=CronTrigger(day_of_week="mon-fri", hour=15, minute=0, timezone="Europe/Moscow"),
#         args=[bot],
#         max_instances=1
#     )
#     scheduler.start()
#     logger.info("Уведомления запущены: будние дни, 15:00 МСК")