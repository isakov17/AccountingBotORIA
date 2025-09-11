import asyncio
import logging
import signal
from aiogram import Bot, Dispatcher, BaseMiddleware
from aiogram.exceptions import TelegramBadRequest
from config import TELEGRAM_TOKEN
from handlers.commands import router as commands_router
from handlers.add import add_router
from handlers.return_ import return_router
from handlers.expenses import expenses_router
from handlers.notifications import start_notifications, scheduler

# Logging early
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AccountingBot")

# Middleware for errors
class ErrorMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            return await handler(event, data)
        except Exception as e:
            logger.error(f"Error in handler {handler.__name__}: {e}", exc_info=True)
            if hasattr(event, 'message') and event.message:
                try:
                    await event.message.answer("Произошла ошибка. Попробуйте /start или позже.")
                except TelegramBadRequest:
                    pass  # Already sent or invalid

# Init
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()

# Register middleware (before includes)
dp.message.middleware(ErrorMiddleware())
dp.callback_query.middleware(ErrorMiddleware())

# Include routers (only once, no duplicates)
dp.include_router(commands_router)
dp.include_router(add_router)
dp.include_router(return_router)
dp.include_router(expenses_router)

async def on_startup():
    logger.info("Бот запущен, уведомления стартуют")
    start_notifications(bot)

async def on_shutdown():
    logger.info("Shutdown: stopping scheduler and polling")
    scheduler.shutdown(wait=True)
    await bot.session.close()

def signal_handler(signum, frame):
    logger.info("Received signal, shutting down...")
    asyncio.create_task(on_shutdown())

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    try:
        asyncio.run(dp.start_polling(bot))
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt, shutting down")
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {str(e)}")