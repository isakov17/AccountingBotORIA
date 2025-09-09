from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from googleapiclient.errors import HttpError
from sheets import sheets_service
from config import SHEET_NAME, GROUP_CHAT_ID
from datetime import datetime, timedelta
import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger("AccountingBot")
scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
notified_items = set()

def safe_float(value: str | float | int, default: float = 0.0) -> float:
    """
    Безопасное преобразование строки/числа в float
    Заменяет запятые на точки, отсекает пробелы
    """
    try:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            return float(value.replace(",", ".").strip())
    except Exception:
        return default
    return default

async def send_group_notification(
    bot: Bot,
    action: str,
    items: list[dict],
    user_name: str,
    fiscal_doc: str,
    delivery_date: str,
    balance: float,
    links: list[str] | None = None,
):
    """
    Универсальное уведомление в группу
    """
    try:
        # Нормализуем товары
        normalized_items = []
        for it in items:
            normalized_items.append({
                "name": it.get("name", "—"),
                "sum": safe_float(it.get("sum", 0)),
                "quantity": int(it.get("quantity", 1) or 1)
            })

        total_sum = sum(it["sum"] for it in normalized_items)
        total_qty = sum(it["quantity"] for it in normalized_items)

        # Строки с товарами
        items_text = "\n".join(
            [
                f"  • {it['name']} — {it['quantity']} шт. × {it['sum']:.2f} ₽"
                for it in normalized_items
            ]
        )

        links_text = "\n".join([f"🔗 {link}" for link in links]) if links else ""

        text = (
            f"{action}\n\n"
            f"👤 Пользователь: {user_name}\n"
            f"📑 Фискальный номер: {fiscal_doc}\n"
            f"📅 Дата доставки: {delivery_date}\n\n"
            f"🛒 Товары ({len(normalized_items)} шт.):\n{items_text}\n\n"
            f"📦 Всего позиций: {total_qty}\n"
            f"💰 Общая сумма: {total_sum:.2f} ₽\n"
            f"💳 Баланс: {balance:.2f} ₽\n"
            f"{links_text}"
        )

        # Добавляем кнопку перехода в бота
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
    logger.info("Начало выполнения send_notifications")
    today = datetime.now()
    if today.weekday() >= 5:  # Суббота или воскресенье
        logger.info("Уведомления не отправляются в выходные")
        return

    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Чеки!A:P"
        ).execute()
        rows = result.get("values", [])[1:]  # Пропускаем заголовок
        logger.info(f"Загружено {len(rows)} строк из Google Sheets")

        if not rows:
            return

        today_str = today.strftime("%d.%m.%Y")
        three_days_ago = (today - timedelta(days=3)).strftime("%d.%m.%Y")

        for idx, row in enumerate(rows, start=2):
            if len(row) < 13:
                continue

            status = row[8].strip().lower() if row[8] else ""  # I: статус
            delivery_date = row[7].strip() if row[7] else ""   # H: дата доставки
            fiscal_doc = row[12].strip() if row[12] else ""    # M: fiscal_doc
            item_name = row[10].strip() if row[10] else ""     # K: товар
            user_name = row[5].strip() if row[5] else ""       # F: пользователь
            item_sum = safe_float(row[2]) if row[2] else 0.0   # C: сумма
            qty = int(row[4]) if row[4] else 1                 # E: количество
            link = row[15].strip() if len(row) > 15 else ""    # P: ссылка
            balance = safe_float(row[3]) if len(row) > 3 and row[3] else 0.0  # D: баланс
            notification_key = f"{fiscal_doc}_{idx}"

            # Условие: статус "ожидает" и дата доставки сегодня или 3 дня назад
            if status == "ожидает" and delivery_date in [today_str, three_days_ago]:
                if notification_key not in notified_items:
                    await send_group_notification(
                        bot=bot,
                        action="📦 Напоминание о доставке",
                        items=[{"name": item_name, "sum": item_sum, "quantity": qty}],
                        user_name=user_name,
                        fiscal_doc=fiscal_doc,
                        delivery_date=delivery_date,
                        balance=balance,
                        links=[link] if link else []
                    )
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
        trigger=IntervalTrigger(minutes=1),
        args=[bot],
        max_instances=1
    )
    scheduler.start()
    logger.info("Уведомления запущены: каждая минута (тестовый режим)")


# def start_notifications(bot: Bot):
#     scheduler.add_job(
#         send_notifications,
#         trigger=CronTrigger(day_of_week="mon-fri", hour=15, minute=0, timezone="Europe/Moscow"),
#         args=[bot],
#         max_instances=1
#     )
#     scheduler.start()
#     logger.info("Уведомления запущены: будние дни, 15:00 МСК")