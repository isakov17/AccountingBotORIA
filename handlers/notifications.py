from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from aiogram.filters import Command
from googleapiclient.errors import HttpError
from sheets import sheets_service, get_monthly_balance, async_sheets_call
from config import SHEET_NAME, GROUP_CHAT_ID
from datetime import datetime, timedelta
import asyncio
import logging
import random
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from utils import safe_float, redis_client

logger = logging.getLogger("AccountingBot")
scheduler = AsyncIOScheduler(timezone="Europe/Moscow")


# ==========================================================
# 📩 Универсальная функция отправки уведомлений
# ==========================================================
async def send_notification(
    bot: Bot,
    action: str,
    items: list[dict],
    user_name: str,
    fiscal_doc: str,
    operation_date: str,
    balance: float,
    is_group: bool = False,
    chat_id: int = None,
    pdf_url: str = "",
    excluded_sum: float = 0.0  # ✅ НОВОЕ: Добавлен аргумент для суммы доставки/исключений
):
    """
    Универсальная функция отправки уведомления.
    В шапке теперь указывается дата операции, а не дата доставки.
    """
    try:
        normalized_items = [
            {
                "name": item.get("name", "—"),
                "sum": safe_float(item.get("sum", 0)),
                "quantity": int(item.get("quantity", 1) or 1),
                "price": safe_float(item.get("price", 0)) or safe_float(item.get("sum", 0)) / max(int(item.get("quantity", 1) or 1), 1),
                "link": item.get("link", ""),
                "comment": item.get("comment", ""),
                "delivery_date": item.get("delivery_date", ""),
            }
            for item in items
        ]

        items_total = sum(it["sum"] for it in normalized_items)
        full_total = items_total + excluded_sum  # Полная сумма чека с учетом доставки

        items_text = "\n".join(
            f"▫️ <b>{it['name']}</b>\n"
            f"   ├ 💰 {it['quantity']} × {it['price']:.2f} ₽ = <b>{it['sum']:.2f} ₽</b>"
            + (f"\n   ├ 📅 {it['delivery_date']}" if it['delivery_date'] else "")
            + (f"\n   ├ 🔗 <a href=\"{it['link']}\">Ссылка</a>" if it['link'] else "")
            + (f"\n   └ 💬 {it['comment']}" if it['comment'] else "")
            for it in normalized_items
        )

        receipt_link_text = f"\n📄 Чек (PDF): <a href=\"{pdf_url}\">Скачать / Открыть</a>" if pdf_url else ""

        # ✅ НОВОЕ: Если есть доставка (исключения) - расписываем подробно. Если нет - просто Итого.
        if excluded_sum > 0:
            totals_text = (
                f"💰 <b>Сумма товаров:</b> {items_total:.2f} ₽\n"
                f"🚚 <b>Исключено (доставка/услуги):</b> {excluded_sum:.2f} ₽\n"
                f"🧾 <b>Полная сумма чека:</b> {full_total:.2f} ₽"
            )
        else:
            totals_text = f"💰 <b>Итого:</b> {items_total:.2f} ₽"

        text = (
            f"<b>{action}</b>\n\n"
            f"👤 Пользователь: <b>{user_name}</b>\n"
            f"🧾 Фискальный номер: <code>{fiscal_doc}</code>\n"
            f"📅 Дата операции: {operation_date or datetime.now().strftime('%d.%m.%Y')}"
            f"{receipt_link_text}\n\n"
            f"{items_text}\n\n"
            f"{totals_text}\n"  # <--- Используем наш новый красивый блок итогов
            f"💳 <b>Баланс:</b> {balance:.2f} ₽"
        )

        reply_markup = None
        target_chat = GROUP_CHAT_ID if is_group else chat_id
        if is_group:
            reply_markup = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🚀 Открыть бота", url="https://t.me/AccountingORIABot")]
                ]
            )

        await bot.send_message(
            target_chat,
            text,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=reply_markup
        )

        logger.info(
            f"📨 Уведомление отправлено {'группе' if is_group else 'пользователю'}: "
            f"{action}, чек={fiscal_doc}, chat={target_chat}"
        )

    except Exception as e:
        logger.error(
            f"❌ Ошибка при отправке уведомления {'группе' if is_group else f'chat_id={chat_id}'}: {type(e).__name__}: {e}"
        )


# ==========================================================
# 📦 Планировщик уведомлений о доставке
# ==========================================================
async def send_notifications(bot: Bot):
    """Ежедневная проверка Google Sheets и напоминания о доставках."""
    logger.info("🚀 Начало выполнения send_notifications")

    today = datetime.now()
    if today.weekday() >= 5:  # Сб/Вс
        logger.info(f"⏭️ Уведомления не отправляются в выходные (weekday={today.weekday()})")
        return

    try:
        result = await async_sheets_call(
            sheets_service.spreadsheets().values().get,
            spreadsheetId=SHEET_NAME,
            range="Чеки!A:Q"
        )
        rows = result.get("values", [])[1:]
        logger.info(f"📊 Загружено {len(rows)} строк из Google Sheets (Чеки!A:Q)")

        today_str = today.strftime("%d.%m.%Y")
        three_days_ago = (today - timedelta(days=3)).strftime("%d.%m.%Y")

        notified_count = 0
        skipped_count = 0

        for idx, row in enumerate(rows, start=2):
            if len(row) < 13:
                skipped_count += 1
                continue

            fiscal_doc = (row[12] or "").strip()
            if not fiscal_doc:
                skipped_count += 1
                continue

            status = (row[8] or "").strip().lower().replace(" ", "")
            delivery_date = (row[7] or "").strip()

            if status != "ожидает" or delivery_date not in [today_str, three_days_ago]:
                skipped_count += 1
                continue

            item_name = (row[10] or "").strip() or "Неизвестно"
            item_sum = safe_float(row[2]) if len(row) > 2 else 0.0
            qty = int(row[4]) if len(row) > 4 and row[4] else 1
            item_link = (row[15] or "").strip() if len(row) > 15 else ""
            item_comment = (row[16] or "").strip() if len(row) > 16 else ""
            user_name = (row[5] or "").strip() or "Неизвестно"

            items = [{
                "name": item_name,
                "sum": item_sum,
                "quantity": qty,
                "link": item_link,
                "comment": item_comment,
                "delivery_date": delivery_date
            }]

            balance_data = await get_monthly_balance()
            balance = safe_float(balance_data.get("balance", 0.0)) if balance_data else 0.0

            await send_notification(
                bot=bot,
                action="📦 Напоминание о доставке",
                items=items,
                user_name=user_name,
                fiscal_doc=fiscal_doc,
                operation_date=datetime.now().strftime("%d.%m.%Y"),
                balance=balance,
                is_group=True
            )

            notified_count += 1
            await asyncio.sleep(random.uniform(1, 2))

        logger.info(f"✅ Отправлено уведомлений: {notified_count}, пропущено: {skipped_count}")

    except HttpError as e:
        logger.error(f"❌ Ошибка доступа к Google Sheets: {e.status_code} - {e.reason}")
        await asyncio.sleep(60)
    except Exception as e:
        logger.error(f"❌ Неожиданная ошибка в send_notifications: {e}")
        await asyncio.sleep(60)


# ==========================================================
# 🕐 Планировщик (ежедневно по будням)
# ==========================================================
def start_notifications(bot: Bot):
    """Запуск планировщика уведомлений."""
    trigger = CronTrigger(day_of_week="mon-fri", hour=12, minute=0, timezone="Europe/Moscow")
    scheduler.add_job(send_notifications, trigger=trigger, args=[bot], max_instances=1)
    scheduler.start()
    logger.info("🕐 Scheduler уведомлений запущен (будни 12:00 МСК)")

    # Тестовое уведомление при запуске
    try:
        logger.debug(f"Тест отправки при запуске, GROUP_CHAT_ID={GROUP_CHAT_ID}")
        logger.info(f"✅ Тестовое уведомление отправлено при запуске (имитация), chat_id={GROUP_CHAT_ID}")
    except Exception as e:
        logger.error(f"❌ Ошибка при тестовом уведомлении: {type(e).__name__}: {e}")
