from aiogram import Bot
from aiogram.filters import Command  # Добавлен импорт Command
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
        logger.info(f"📨 Уведомление отправлено {'группе' if is_group else 'пользователю'}: {action}, чек={fiscal_doc}, chat={target_chat}")

    except Exception as e:
        logger.error(f"❌ Ошибка при отправке уведомления {'группе' if is_group else f'chat_id={chat_id}'}: {str(e)}")

async def send_notifications(bot: Bot):
    """Проверка таблицы и напоминания (async)."""
    logger.info("🚀 Начало выполнения send_notifications")
    today = datetime.now()
    if today.weekday() >= 5:  # Sat/Sun
        logger.info(f"⏭️ Уведомления не отправляются в выходные (weekday={today.weekday()})")
        return

    try:
        result = await async_sheets_call(
            sheets_service.spreadsheets().values().get,
            spreadsheetId=SHEET_NAME, range="Чеки!A:Q"
        )
        rows = result.get("values", [])[1:]  # Пропускаем заголовок
        logger.info(f"📊 Загружено {len(rows)} строк из Google Sheets (range A:Q)")

        if not rows:
            logger.warning("⚠️ Нет данных в таблице 'Чеки'")
            return

        today_str = today.strftime("%d.%m.%Y")
        three_days_ago = (today - timedelta(days=3)).strftime("%d.%m.%Y")
        logger.info(f"📅 Today: '{today_str}', 3 days ago: '{three_days_ago}'")

        notified_count = 0
        skipped_count = 0
        for idx, row in enumerate(rows, start=2):
            logger.info(f"🔍 Обработка row {idx}: len(row)={len(row)}, raw_row[7:13]={[str(x)[:20] for x in row[7:13]]}")  # Видимый лог для КАЖДОЙ строки (H=delivery, I=status, M=fiscal)

            # ✅ ФИКС: Снижаем порог до 13 (минимум A-M: до fiscal/status/date). P/Q optional.
            if len(row) < 13:
                logger.info(f"⏭️ Row {idx}: Пропуск (len(row)={len(row)} < 13 — слишком короткая строка)")
                skipped_count += 1
                continue

            fiscal_doc = (row[12] or "").strip()
            if not fiscal_doc:
                logger.info(f"⏭️ Row {idx}: Пропуск (fiscal_doc пустой: '{row[12]}')")
                skipped_count += 1
                continue

            status_raw = row[8] if row[8] else ""
            status = status_raw.strip().lower().replace(" ", "")  # Удаляем пробелы/символы для надёжности (e.g., "Ожидает " → "ожидает")
            delivery_date_raw = row[7] if row[7] else ""
            delivery_date = delivery_date_raw.strip()  # Только strip

            logger.info(f"🔍 Row {idx}: fiscal_doc='{fiscal_doc}', status_raw='{status_raw}' → status='{status}', delivery_date='{delivery_date}'")

            if status != "ожидает" or delivery_date not in [today_str, three_days_ago]:
                reason = "status != 'ожидает'" if status != "ожидает" else f"date '{delivery_date}' != '{today_str}/{three_days_ago}'"
                logger.info(f"⏭️ Row {idx}: Пропуск ({reason})")
                skipped_count += 1
                continue

            # ❌ ВРЕМЕННО ОТКЛЮЧЁН: Redis-check (чтобы уведомления приходили каждый раз для теста)
            # notification_key = f"{fiscal_doc}_{idx}"
            # if await redis_client.sismember("notified_items", notification_key):
            #     logger.info(f"⏭️ Row {idx}: Уже уведомлено (Redis key: {notification_key})")
            #     skipped_count += 1
            #     continue

            # Подготовка items
            item_name = (row[10] or "").strip()
            item_sum = safe_float(row[2]) if len(row) > 2 and row[2] else 0.0  # Безопасно для C=сумма
            qty = int(row[4]) if len(row) > 4 and row[4] else 1  # E=qty
            # ✅ ФИКС: Optional P/Q с проверкой len
            item_link = (row[15] or "").strip() if len(row) > 15 else ""
            item_comment = (row[16] or "").strip() if len(row) > 16 else ""

            items = [{
                "name": item_name or "Неизвестно",
                "sum": item_sum,
                "quantity": qty,
                "link": item_link,
                "comment": item_comment,
                "delivery_date": delivery_date
            }]

            user_name = (row[5] or "").strip() or "Неизвестно"  # F=user

            logger.info(f"📤 Подготовка уведомления для row {idx}: {fiscal_doc}, item='{item_name[:30]}...', user={user_name}, sum={item_sum}")

            balance_data = await get_monthly_balance()
            balance = safe_float(balance_data.get("balance", 0.0)) if balance_data else 0.0
            logger.info(f"💰 Баланс для уведомления: {balance:.2f}")

            delivery_date_header = delivery_date

            # Send group only
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
            
            notified_count += 1
            
            # ❌ ВРЕМЕННО ОТКЛЮЧЁН: Добавление в Redis (раскомментируй для прод/блокировки повторов)
            # await redis_client.sadd("notified_items", notification_key)
            
            # Rate limit
            await asyncio.sleep(random.uniform(1, 3))
            
            logger.info(f"✅ Успешно отправлено: fiscal_doc={fiscal_doc}, row={idx}, item={item_name[:50]}..., delivery_date={delivery_date}")

        logger.info(f"📊 Завершено: отправлено {notified_count} уведомлений, пропущено {skipped_count} строк из {len(rows)}")

    except HttpError as e:
        logger.error(f"❌ Ошибка получения чеков: {e.status_code} - {e.reason}")
        await asyncio.sleep(60)
    except Exception as e:
        logger.error(f"❌ Неожиданная ошибка отправки уведомлений: {str(e)}")
        await asyncio.sleep(60)


# def start_notifications(bot: Bot):
#     # ✅ АКТИВНЫЙ ВАРИАНТ: ПРОДОВЫЙ — cron mon-fri 12:00 МСК (будние дни в 12:00)
#     trigger = CronTrigger(day_of_week="mon-fri", hour=12, minute=0, timezone="Europe/Moscow")
#     logger.info("🔔 Уведомления: будние 12:00 (прод режим)")
    
#     scheduler.add_job(
#         send_notifications,
#         trigger=trigger,
#         args=[bot],
#         max_instances=1,
#     )
#     scheduler.start()
#     logger.info("🕐 Scheduler уведомлений запущен (прод режим)")

    # ❌ ЗАКОММЕНТИРОВАННЫЙ ВАРИАНТ: ТЕСТОВЫЙ — каждую минуту (раскомментируй для разработки/тестирования)
    # trigger = IntervalTrigger(minutes=1)
    # logger.info("🔔 Уведомления: тест 1min (каждую минуту)")
    # 
    # scheduler.add_job(
    #     send_notifications,
    #     trigger=trigger,
    #     args=[bot],
    #     max_instances=1,
    # )
    # scheduler.start()
    # logger.info("🕐 Scheduler уведомлений запущен (тестовый режим)")

    # Тестовая команда
from aiogram import Router
router = Router()

def start_notifications(bot: Bot):
    trigger = CronTrigger(day_of_week="mon-fri", hour=12, minute=0, timezone="Europe/Moscow")
    logger.info("🔔 Уведомления: будние 12:00 (прод режим)")
    
    scheduler.add_job(
        send_notifications,
        trigger=trigger,
        args=[bot],
        max_instances=1,
    )
    scheduler.start()
    logger.info("🕐 Scheduler уведомлений запущен (прод режим)")

    # Тестовая отправка при запуске
    test_message = "🔔 Тестовое уведомление при запуске бота!"
    try:
        logger.debug(f"Тест отправки при запуске, GROUP_CHAT_ID={GROUP_CHAT_ID}")
        asyncio.run_coroutine_threadsafe(
            bot.send_message(chat_id=GROUP_CHAT_ID, text=test_message),
            asyncio.get_event_loop()
        ).result(timeout=10)
        logger.info(f"✅ Тестовое уведомление отправлено при запуске, chat_id={GROUP_CHAT_ID}")
    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)
        logger.error(f"❌ Ошибка отправки тестового уведомления при запуске: {error_type}: {error_msg}, chat_id={GROUP_CHAT_ID}")

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