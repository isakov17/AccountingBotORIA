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
from apscheduler.schedulers.asyncio import AsyncIOScheduler  # Добавил импорт, если scheduler - APScheduler

# ---------------------------------------------------------
# Логирование
# ---------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("AccountingBot")

# ---------------------------------------------------------
# Глобальная переменная для username бота (кэш)
# ---------------------------------------------------------
BOT_USERNAME: str | None = None

# ---------------------------------------------------------
# Middleware для ошибок (оставляем как у тебя было)
# ---------------------------------------------------------
class ErrorMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            return await handler(event, data)
        except Exception as e:
            logger.error(f"Error in handler {getattr(handler, '__name__', repr(handler))}: {e}", exc_info=True)
            # Попробуем уведомить пользователя (если есть message)
            try:
                if hasattr(event, "message") and event.message:
                    await event.message.answer("Произошла ошибка. Попробуйте /start или позже.")
            except TelegramBadRequest:
                pass

# ---------------------------------------------------------
# Middleware: блокируем всё в группах, кроме /balance (и /balance@botname).
# Также блокируем callback_query из групп.
# ---------------------------------------------------------
class GroupFilterMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            # --- Message ---
            if isinstance(event, Message):
                msg: Message = event
                # Только для групп/супергрупп действуем
                if msg.chat and msg.chat.type in ("group", "supergroup"):
                    text = (msg.text or msg.caption or "").strip().lower()
                    # Получаем username бота (кэшируем в BOT_USERNAME на старте)
                    bot_username = BOT_USERNAME
                    if not bot_username:
                        # fallback — один раз получить от API
                        try:
                            bot_info = await msg.bot.get_me()
                            bot_username = (bot_info.username or "").lower()
                        except Exception:
                            bot_username = ""
                    allowed_prefixes = ("/balance", f"/balance@{bot_username}" if bot_username else "/balance")
                    # Если сообщение не начинается с разрешённой команды — просто НЕ вызываем handler
                    if not any(text.startswith(p) for p in allowed_prefixes):
                        logger.debug(f"🔇 Ignored group message from chat {msg.chat.id}: {text[:80]}")
                        return  # не вызываем handler — обработка прекращена

            # --- CallbackQuery ---
            if isinstance(event, CallbackQuery):
                # Игнорируем все callback_query из групп (чтобы кнопки в группах не тригерили)
                if event.message and event.message.chat and event.message.chat.type in ("group", "supergroup"):
                    logger.debug(f"🔇 Ignored callback_query in group {event.message.chat.id}")
                    return

        except Exception as e:
            # Если что-то упало в мидлваре, логируем и даём обработке пройти (чтобы бот не молчал из-за ошибки мидлвари)
            logger.exception(f"Exception in GroupFilterMiddleware: {e}")
            return await handler(event, data)

        # Всё ок — продолжаем цепочку
        return await handler(event, data)


# ---------------------------------------------------------
# Инициализация бота и диспетчера
# ---------------------------------------------------------
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()

# Регистрируем мидлвари — сначала фильтр групп (чтобы он прерывал обработку при необходимости),
# затем мидлварь ошибок (чтобы ловить исключения в хендлерах)
dp.message.middleware(GroupFilterMiddleware())
dp.callback_query.middleware(GroupFilterMiddleware())

dp.message.middleware(ErrorMiddleware())
dp.callback_query.middleware(ErrorMiddleware())

# ---------------------------------------------------------
# Подключаем роутеры
# ---------------------------------------------------------
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
        logger.info(f"Bot username cached: {BOT_USERNAME}")
    except Exception as e:
        logger.warning(f"Не удалось получить username бота на старте: {e}")
        BOT_USERNAME = None

    logger.info("Бот запущен, уведомления стартуют")
    start_notifications(bot)

async def on_shutdown():
    logger.info("Shutdown: stopping scheduler and closing bot session")
    scheduler.shutdown(wait=True)
    await bot.session.close()

def signal_handler(signum, frame):
    logger.info("Received signal, shutting down...")
    asyncio.create_task(on_shutdown())

# ---------------------------------------------------------
# Точка входа
# ---------------------------------------------------------

if __name__ == "__main__":
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    # Обработка сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # 🕐 Логирование активных задач планировщика
    jobs = scheduler.get_jobs()
    if jobs:
        logger.info("📅 Активные задачи в планировщике:")
        for job in jobs:
            logger.info(f" - {job.id} | next run: {job.next_run_time}")
    else:
        logger.info("📅 Активные задачи в планировщике: []")

    try:
        asyncio.run(dp.start_polling(bot))
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt, shutting down")
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {str(e)}")
