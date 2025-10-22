import asyncio
import time
import logging
from config import RETRY_INTERVAL, MAX_RETRIES_BACKGROUND, REDIS_RETRY_PREFIX
from utils import cache_get, cache_set, parse_qr_from_photo, confirm_manual_api
from handlers.notifications import send_notification

logger = logging.getLogger("AccountingBot")

async def retry_check_task(retry_key: str, bot):
    """
    Фоновая задача для повторной проверки чека
    """
    max_attempts = MAX_RETRIES_BACKGROUND  # 24 часа
    check_interval = RETRY_INTERVAL  # 1 час
    
    retry_data = await cache_get(retry_key)
    if not retry_data:
        logger.info(f"Retry task stopped: no data for key={retry_key}")
        return

    user_id = retry_data["user_id"]
    chat_id = retry_data["chat_id"]
    attempts = retry_data["attempts"]
    message_id = retry_data.get("message_id")
    
    logger.info(f"Starting retry task: key={retry_key}, attempts={attempts}, user_id={user_id}")

    while attempts < max_attempts:
        attempts += 1
        retry_data["attempts"] = attempts
        retry_data["last_attempt"] = time.time()
        await cache_set(retry_key, retry_data, expire=86400 * 2)
        
        logger.info(f"Retry attempt {attempts}/{max_attempts} for key={retry_key}")
        
        try:
            # Определяем тип запроса (QR фото или manual)
            if "qrfile" in retry_data["params"]:
                # QR фото - имитируем вызов API
                success, parsed_data = await simulate_qr_api_call(retry_data["params"])
            else:
                # Manual - используем confirm_manual_api
                success, msg, parsed_data = await confirm_manual_api(retry_data["params"], type('User', (), {'id': user_id}))
            
            if success and parsed_data:
                # ✅ УСПЕХ: чек появился в базе
                await handle_successful_check(retry_key, retry_data, parsed_data, bot, user_id, chat_id)
                return
                
            elif not success and "не готовы" in str(msg):
                # ⏳ Еще не готово, продолжаем ждать
                logger.info(f"Check still not ready, waiting... attempt {attempts}")
                if attempts % 6 == 0:  # Каждые 6 часов уведомляем о статусе
                    await send_progress_notification(bot, chat_id, attempts, max_attempts, False)
                
            else:
                # ❌ ОШИБКА: прекращаем попытки
                await handle_failed_check(retry_key, retry_data, f"API error: {msg}", bot, user_id, chat_id)
                return
                
        except Exception as e:
            logger.error(f"Error in retry task {retry_key}: {str(e)}")
            if attempts >= max_attempts:
                await handle_failed_check(retry_key, retry_data, f"Exception: {str(e)}", bot, user_id, chat_id)
                return
        
        # Ждем перед следующей попыткой
        await asyncio.sleep(check_interval)
    
    # Достигнут лимит попыток
    await handle_failed_check(retry_key, retry_data, "Достигнут лимит попыток (24 часа)", bot, user_id, chat_id)

async def simulate_qr_api_call(params):
    """
    Имитация API вызова для тестирования
    В продакшене заменить на реальный вызов parse_qr_from_photo
    """
    # Для теста: на 3-й попытке возвращаем успех
    retry_data = await cache_get(f"{REDIS_RETRY_PREFIX}{params.get('user_id', '')}:{params.get('photo_hash', '')}")
    attempts = retry_data.get("attempts", 0) if retry_data else 0
    
    if attempts >= 2:  # На 3-й попытке успех
        logger.info("SIMULATION: Check found in database!")
        parsed_data = {
            "fiscal_doc": "1234567890",
            "date": "15.01.2024",
            "store": "Тестовый магазин",
            "items": [
                {
                    "name": "Тестовый товар",
                    "sum": 100.0,
                    "price": 100.0,
                    "quantity": 1
                }
            ],
            "qr_string": "simulated_qr_string",
            "total_sum": 100.0,
            "excluded_sum": 0.0,
            "excluded_items": []
        }
        return True, parsed_data
    else:
        logger.info("SIMULATION: Check not ready yet")
        return False, None

async def handle_successful_check(retry_key, retry_data, parsed_data, bot, user_id, chat_id):
    """Обработка успешного нахождения чека"""
    try:
        # Сохраняем данные чека для продолжения
        success_key = f"check_success:{user_id}:{parsed_data['fiscal_doc']}"
        await cache_set(success_key, parsed_data, expire=3600)  # 1 час на продолжение
        
        # Создаем клавиатуру для продолжения
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[
                InlineKeyboardButton(
                    text="✅ Продолжить добавление чека", 
                    callback_data=f"continue_add:{parsed_data['fiscal_doc']}"
                )
            ]]
        )
        
        # Отправляем уведомление об успехе
        await bot.send_message(
            chat_id=chat_id,
            text=f"🎉 Чек найден! Фискальный номер: {parsed_data['fiscal_doc']}\n"
                 f"Магазин: {parsed_data['store']}\n"
                 f"Сумма: {parsed_data['total_sum']:.2f} ₽\n"
                 f"Можете продолжить добавление чека:",
            reply_markup=keyboard
        )
        
        # Удаляем задачу из Redis
        await cache_set(retry_key, None, expire=1)
        
        logger.info(f"Check found and user notified: key={retry_key}, user_id={user_id}")
        
    except Exception as e:
        logger.error(f"Error in handle_successful_check: {str(e)}")
        await bot.send_message(chat_id, "❌ Ошибка при обработке найденного чека")

async def handle_failed_check(retry_key, retry_data, error_msg, bot, user_id, chat_id):
    """Обработка неудачного завершения проверки"""
    try:
        # Создаем клавиатуру для повторной попытки
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[
                InlineKeyboardButton(
                    text="🔄 Попробовать снова", 
                    callback_data="retry_failed_check"
                )
            ]]
        )
        
        await bot.send_message(
            chat_id=chat_id,
            text=f"❌ Чек так и не появился в базе за 24 часа.\n"
                 f"Причина: {error_msg}\n"
                 f"Вы можете попробовать снова:",
            reply_markup=keyboard
        )
        
        # Удаляем задачу из Redis
        await cache_set(retry_key, None, expire=1)
        
        logger.info(f"Check not found after max attempts: key={retry_key}, user_id={user_id}")
        
    except Exception as e:
        logger.error(f"Error in handle_failed_check: {str(e)}")

async def send_progress_notification(bot, chat_id, attempts, max_attempts, is_success):
    """Уведомление о прогрессе проверки"""
    try:
        if is_success:
            await bot.send_message(chat_id, "✅ Чек найден! Можете продолжить добавление.")
        else:
            hours_passed = attempts
            hours_total = max_attempts
            await bot.send_message(
                chat_id, 
                f"⏳ Проверяю чек... Прошло {hours_passed}ч из {hours_total}ч"
            )
    except Exception as e:
        logger.error(f"Error sending progress notification: {str(e)}")