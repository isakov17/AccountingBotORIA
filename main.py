import asyncio
import logging
import signal
from aiogram import Bot, Dispatcher, BaseMiddleware, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import Message, CallbackQuery
from config import TELEGRAM_TOKEN
from handlers.commands import router as commands_router
from handlers.add import add_router
from handlers.return_ import return_router
from handlers.expenses import expenses_router
from handlers.notifications import start_notifications, scheduler
from utils import restore_pending_tasks  # ✅ ДОБАВИТЬ ЭТОТ ИМПОРТ


# ---------------------------------------------------------
# Логирование
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)
logger = logging.getLogger("AccountingBot")

BOT_USERNAME: str | None = None


# ---------------------------------------------------------
# Middleware: обработка ошибок
# ---------------------------------------------------------
class ErrorMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            return await handler(event, data)
        except Exception as e:
            logger.error(f"Error in handler {getattr(handler, '__name__', repr(handler))}: {e}", exc_info=True)
            with contextlib.suppress(TelegramBadRequest):
                if hasattr(event, "message") and event.message:
                    await event.message.answer("⚠️ Произошла ошибка. Попробуйте /start или позже.")


# ---------------------------------------------------------
# Middleware: фильтр сообщений в группах
# ---------------------------------------------------------
class GroupFilterMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            if isinstance(event, Message):
                msg: Message = event
                if msg.chat and msg.chat.type in ("group", "supergroup"):
                    text = (msg.text or msg.caption or "").strip().lower()
                    bot_username = BOT_USERNAME or ""
                    allowed_prefixes = ("/balance", f"/balance@{bot_username}" if bot_username else "/balance")
                    if not any(text.startswith(p) for p in allowed_prefixes):
                        logger.debug(f"🔇 Ignored group message from chat {msg.chat.id}: {text[:80]}")
                        return
            elif isinstance(event, CallbackQuery):
                if event.message and event.message.chat and event.message.chat.type in ("group", "supergroup"):
                    logger.debug(f"🔇 Ignored callback_query in group {event.message.chat.id}")
                    return
        except Exception as e:
            logger.exception(f"Exception in GroupFilterMiddleware: {e}")
            return await handler(event, data)
        return await handler(event, data)


# ---------------------------------------------------------
# Инициализация
# ---------------------------------------------------------
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()

dp.message.middleware(GroupFilterMiddleware())
dp.callback_query.middleware(GroupFilterMiddleware())
dp.message.middleware(ErrorMiddleware())
dp.callback_query.middleware(ErrorMiddleware())

dp.include_router(commands_router)
dp.include_router(add_router)
dp.include_router(return_router)
dp.include_router(expenses_router)


# ---------------------------------------------------------
# Startup / Shutdown
# ---------------------------------------------------------
async def on_startup():
    global BOT_USERNAME
    try:
        me = await bot.get_me()
        BOT_USERNAME = (me.username or "").lower()
        logger.info(f"🤖 Bot username cached: @{BOT_USERNAME}")
    except Exception as e:
        logger.warning(f"⚠️ Не удалось получить username бота на старте: {e}")
        BOT_USERNAME = None

    logger.info("🔔 Инициализация уведомлений и планировщика задач...")
    start_notifications(bot)

    # 🔄 ВОССТАНОВЛЕНИЕ PENDING ЗАДАЧ
    logger.info("🔄 Восстановление отложенных задач...")
    restored_count = await restore_pending_tasks(bot)
    
    if restored_count > 0:
        logger.info(f"🎯 Восстановлено {restored_count} отложенных задач")
    else:
        logger.info("✅ Нет отложенных задач для восстановления")

    jobs = scheduler.get_jobs()
    if jobs:
        logger.info("📅 Активные задачи планировщика:")
        for job in jobs:
            logger.info(f" - {job.id} | next: {job.next_run_time}")
    else:
        logger.info("📅 Активные задачи планировщика: нет активных")


async def on_shutdown():
    logger.info("🔻 Завершение работы...")
    try:
        if scheduler.running:
            scheduler.shutdown(wait=False)
    except Exception as e:
        logger.warning(f"⚠️ Ошибка при остановке scheduler: {e}")
    await bot.session.close()
    logger.info("✅ Завершено корректно.")


# ---------------------------------------------------------
# Основная точка входа
# ---------------------------------------------------------
async def main():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(on_shutdown()))

    logger.info("🚀 Bot is starting...")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🧩 Остановка по Ctrl+C")
    except Exception as e:
        logger.error(f"💥 Ошибка при запуске бота: {e}", exc_info=True)
