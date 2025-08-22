from aiogram import Bot, Dispatcher
from handlers.commands import router as commands_router
from handlers.routers import router as routers
from handlers.notifications import send_notifications
import asyncio
from config import TELEGRAM_TOKEN
from googleapiclient.errors import HttpError
import logging

# Инициализация
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()

# Регистрация роутеров
dp.include_router(commands_router)
dp.include_router(routers)

# Запуск уведомлений
async def on_startup():
    try:
        logger.info("Запуск уведомлений")
        asyncio.create_task(send_notifications(bot))
    except Exception as e:
        logger.error(f"Ошибка при запуске уведомлений: {str(e)}")

# Запуск бота
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("AccountingBot")
    dp.startup.register(on_startup)
    try:
        asyncio.run(dp.start_polling(bot))
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {str(e)}")