from exceptions import is_excluded, get_excluded_items
import logging
import aiohttp
from config import PROVERKACHEKA_TOKEN
import redis.asyncio as redis
import json
from datetime import datetime
import calendar  # Для валидации дат
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton  # Для reset_keyboard
from typing import Tuple, Dict, Any, Optional
import requests  # Для API запросов (fallback)
import time  # Для time.sleep в retry
from io import BytesIO
from aiogram import Bot  # Для уведомлений в retry
from datetime import timezone
from PIL import Image
import io

# Безопасный импорт pyzbar с обработкой ошибок
try:
    import pyzbar.pyzbar as pyzbar
    from PIL import Image
    import io
    PYZBAR_AVAILABLE = True
except ImportError as e:
    PYZBAR_AVAILABLE = False


logger = logging.getLogger("AccountingBot")

# Redis с pool и reconnect
pool = redis.ConnectionPool(host='localhost', port=6379, db=0, decode_responses=True, max_connections=10, retry_on_timeout=True)
redis_client = redis.Redis(connection_pool=pool)

import asyncio
from datetime import datetime, timedelta



from aiogram import Router, F
from aiogram.types import CallbackQuery

cancel_router = Router()

@cancel_router.callback_query(F.data.startswith("cancel_check:"))
async def cancel_pending_check(callback: CallbackQuery):
    """Обработка отмены отложенной проверки чека"""
    try:
        safe_fiscal_key = callback.data.split(":")[1]
        
        # Восстанавливаем оригинальный fiscal_key (обратная замена)
        fiscal_key = safe_fiscal_key.replace("_", "=").replace("_", "&").replace("_", ".").replace("_", ":")
        
        # Удаляем из pending
        await remove_pending(fiscal_key)
        
        # Удаляем связанные задачи планировщика
        from handlers.notifications import scheduler
        job_id = f"retry_check:{fiscal_key}"
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)
            logger.info(f"🗑️ Удалена задача планировщика: {job_id}")
        
        await callback.message.edit_text(
            "❌ Проверка чека отменена. Вы можете добавить чек заново когда будет удобно.",
            reply_markup=None
        )
        
        await callback.answer("Проверка отменена")
        logger.info(f"✅ Пользователь отменил проверку чека: {fiscal_key}, user_id={callback.from_user.id}")
        
    except Exception as e:
        logger.error(f"Ошибка при отмене проверки: {str(e)}")
        await callback.answer("❌ Ошибка при отмене проверки")


async def cache_get(key: str) -> any:
    try:
        data = await redis_client.get(key)
        if data is not None:
            return json.loads(data)
        return None
    except Exception as e:
        logger.error(f"Ошибка чтения из Redis: {str(e)}")
        return None

async def cache_set(key: str, value: any, expire: int = None) -> bool:
    try:
        await redis_client.set(key, json.dumps(value))
        if expire:
            await redis_client.expire(key, expire)
        return True
    except Exception as e:
        logger.error(f"Ошибка записи в Redis: {str(e)}")
        return False

# НОВЫЕ: Функции для pending checks с правильной структурой
async def is_pending_or_processed(fiscal_key: str) -> bool:
    """Проверяет, в обработке ли чек или уже сохранён."""
    processed = await redis_client.exists(f"processed:{fiscal_key}")
    pending = await redis_client.exists(f"pending:{fiscal_key}")
    return processed or pending

async def add_to_pending(fiscal_key: str, data: dict, expire: int = 36400) -> bool:
    """Добавляет в pending с expire."""
    data['retries'] = 0
    data['created_at'] = time.time()
    return await cache_set(f"pending:{fiscal_key}", data, expire=expire)

async def get_pending(fiscal_key: str) -> dict | None:
    return await cache_get(f"pending:{fiscal_key}")

async def update_pending(fiscal_key: str, data: dict):
    """Обновляет данные pending задачи"""
    current = await get_pending(fiscal_key)
    if current:
        # Сохраняем TTL существующей задачи
        ttl = await redis_client.ttl(f"pending:{fiscal_key}")
        await cache_set(f"pending:{fiscal_key}", data, expire=ttl)

async def remove_pending(fiscal_key: str):
    await redis_client.delete(f"pending:{fiscal_key}")

async def add_to_processed(fiscal_key: str):
    await cache_set(f"processed:{fiscal_key}", {"processed_at": time.time()}, expire=86400)


def normalize_date(date_str: str) -> str:
    """
    Нормализует дату: YYYY.MM.DD или DD.MM.YYYY → DD.MM.YYYY.
    """
    date_str = date_str.replace("-", ".")
    try:
        parts = date_str.split(".")
        if len(parts) == 3:
            if len(parts[0]) == 4:  # YYYY.MM.DD
                return datetime.strptime(date_str, "%Y.%m.%d").strftime("%d.%m.%Y")
            else:  # DD.MM.YYYY
                return datetime.strptime(date_str, "%d.%m.%Y").strftime("%d.%m.%Y")
    except ValueError:
        pass
    return datetime.now().strftime("%d.%m.%Y")

def safe_float(value: str | float | int, default: float = 0.0) -> float:
    """
    Безопасное преобразование строки/числа в float.
    Заменяет запятые на точки, отсекает пробелы.
    """
    try:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            return float(value.replace(",", ".").strip())
    except (ValueError, AttributeError):
        return default
    return default

# ==========================================================
# 🔧 Константы настройки фоновых задач
# ==========================================================

# --- Режим работы (для теста можно временно включить TEST_MODE) ---
TEST_MODE = False

# --- Продакшен-параметры ---
PROD_RETRY_INTERVAL_MIN = 3   # 60 мин (1 час)
PROD_MAX_RETRIES = 3            # 8 попыток (8 часов)

# --- Тестовый режим (ускоренный) ---
TEST_RETRY_INTERVAL_MIN = 1
TEST_MAX_RETRIES = 3

# --- Автовыбор ---
RETRY_INTERVAL_MIN = TEST_RETRY_INTERVAL_MIN if TEST_MODE else PROD_RETRY_INTERVAL_MIN
MAX_RETRIES = TEST_MAX_RETRIES if TEST_MODE else PROD_MAX_RETRIES


# Добавьте эту функцию после импортов
async def extract_qr_raw_from_photo(photo_data: bytes) -> str | None:
    """
    Локально извлекает сырую строку QR-кода из изображения
    """
    if not PYZBAR_AVAILABLE:
        logger.warning("❌ Pyzbar недоступен, пропускаем локальное распознавание")
        return None
        
    try:
        # Если photo_data это BytesIO, преобразуем в bytes
        if hasattr(photo_data, 'getvalue'):
            photo_data = photo_data.getvalue()
        
        # Конвертируем bytes в изображение
        image = Image.open(io.BytesIO(photo_data))
        
        # Декодируем QR-коды
        decoded_objects = pyzbar.decode(image)
        
        if decoded_objects:
            for obj in decoded_objects:
                if obj.type == 'QRCODE':
                    qr_raw = obj.data.decode('utf-8')
                    logger.info(f"✅ QR-код распознан локально: {qr_raw}")
                    return qr_raw
        
        logger.warning("❌ QR-код не найден на изображении")
        return None
        
    except Exception as e:
        logger.error(f"❌ Ошибка при локальном распознавании QR-кода: {e}")
        return None

# ЗАМЕНИТЕ существующую функцию parse_qr_from_photo на эту новую версию:
async def parse_qr_from_photo(bot, file_id, user_id=None, chat_id=None) -> dict | None:
    file = await bot.get_file(file_id)
    file_path = file.file_path
    photo = await bot.download_file(file_path)  # photo это BytesIO
    
    # 1. Сначала пробуем извлечь qrraw локально
    # Преобразуем BytesIO в bytes для локального распознавания
    photo_bytes = photo.getvalue() if hasattr(photo, 'getvalue') else photo
    qr_raw = await extract_qr_raw_from_photo(photo_bytes)
    
    if qr_raw:
        # 2. Используем Формат запроса 2 (qrraw) - более надежный
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            form = aiohttp.FormData()
            form.add_field("qrraw", qr_raw)
            form.add_field("token", PROVERKACHEKA_TOKEN)
            
            async with session.post("https://proverkacheka.com/api/v1/check/get", data=form) as response:
                if response.status == 200:
                    result = await response.json()
                    return await process_api_response(result, qr_raw, file_id, user_id, chat_id, bot)
                else:
                    logger.error(f"HTTP error при запросе qrraw: {response.status}")
                    # Fallback на старый метод
                    # Сбрасываем позицию BytesIO перед использованием
                    if hasattr(photo, 'seek'):
                        photo.seek(0)
                    return await parse_qr_from_photo_fallback(bot, file_id, user_id, chat_id, photo)
    else:
        # 3. Если не удалось распознать QR локально - fallback на старый метод
        logger.info("🔄 Не удалось распознать QR локально, использую старый метод")
        # Сбрасываем позицию BytesIO перед использованием
        if hasattr(photo, 'seek'):
            photo.seek(0)
        return await parse_qr_from_photo_fallback(bot, file_id, user_id, chat_id, photo)

# Добавьте эту новую функцию для fallback
async def parse_qr_from_photo_fallback(bot, file_id, user_id=None, chat_id=None, photo_data=None):
    """Fallback метод - отправка файла как было раньше"""
    if not photo_data:
        file = await bot.get_file(file_id)
        file_path = file.file_path
        photo_data = await bot.download_file(file_path)
    
    # Сбрасываем позицию BytesIO если нужно
    if hasattr(photo_data, 'seek'):
        photo_data.seek(0)
    
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        form = aiohttp.FormData()
        form.add_field("qrfile", photo_data, filename="check.jpg", content_type="image/jpeg")
        form.add_field("token", PROVERKACHEKA_TOKEN)
        async with session.post("https://proverkacheka.com/api/v1/check/get", data=form) as response:
            if response.status == 200:
                result = await response.json()
                qrraw = result.get("request", {}).get("qrraw", "")
                return await process_api_response(result, qrraw, file_id, user_id, chat_id, bot)
            else:
                logger.error(f"HTTP error в fallback: {response.status}")
                return None

# Добавьте эту новую функцию для обработки ответа API (общая для обоих методов)
async def process_api_response(result, qr_raw, file_id, user_id, chat_id, bot):
    """Обработка ответа от API (общая для обоих методов)"""
    code = result.get("code")
    
    if code == 1:
        # ... существующая логика для успешного ответа ...
        return parsed_data
    
    # Обработка кодов 2/5 - чек не найден в базе ФНС
    elif code in (2, 5):
        qrraw = result.get("request", {}).get("qrraw", "") or qr_raw
        fiscal_key = qrraw or f"temp_{hash(file_id)}"

        if await is_pending_or_processed(fiscal_key):
            logger.info(f"ℹ️ Уже в pending, не добавляем заново: {fiscal_key}")
        else:
            pending_data = {
                "type": "qr",
                "file_id": file_id,
                "user_id": user_id,
                "chat_id": chat_id,
                "retries": 0,
                "created_at": time.time(),
                "last_code": code  # Сохраняем код для логики ретраев
            }
            await add_to_pending(fiscal_key, pending_data)
            from handlers.notifications import scheduler
            schedule_async_job(scheduler, retry_check, RETRY_INTERVAL_MIN, bot, fiscal_key, "qr")

        return {
            "delayed": True,
            "message": "⏳ Чек пока не в базе ФНС (код 2/5). Запускаю фоновую проверку — теперь я буду проверять чек каждый час.\n💡 Когда чек появится, я пришлю уведомление, и вы сможете продолжить добавление.\nПовторно добавлять чек не потребуется.",
            "retry_type": "not_found"  # Тип для разной логики ретраев
        }
    
    # Обработка кода 4 - слишком частые запросы
    elif code == 4:
        data_field = result.get("data")
        if isinstance(data_field, dict):
            wait_seconds = data_field.get("wait")
        else:
            logger.info(f"API code=4: data={data_field}")
            wait_seconds = None

        # Прогрессивные интервалы для кода 4
        current_pending = await get_pending(f"pending:{qr_raw}") if qr_raw else None
        retries = current_pending.get("retries", 0) if current_pending else 0
        
        # Определяем интервал в зависимости от количества попыток
        if retries == 0:
            wait_min = 2  # Первая попытка через 2 минуты
        elif retries == 1:
            wait_min = 5  # Вторая попытка через 5 минут
        elif retries == 2:
            wait_min = 30  # Третья попытка через 30 минут
        else:
            wait_min = 60  # Последующие попытки через 60 минут
        
        # Если API указал свое время, используем его (но не меньше нашего)
        if wait_seconds:
            api_wait_min = max(1, int((wait_seconds + 59) // 60))
            wait_min = max(wait_min, api_wait_min)

        qrraw = result.get("request", {}).get("qrraw", "") or qr_raw
        fiscal_key = qrraw or f"temp_{hash(file_id)}"

        if await is_pending_or_processed(fiscal_key):
            logger.info(f"ℹ️ Уже в pending (code=4), не добавляем заново: {fiscal_key}")
        else:
            pending_data = {
                "type": "qr",
                "file_id": file_id,
                "user_id": user_id,
                "chat_id": chat_id,
                "retries": retries,
                "created_at": time.time(),
                "last_code": code  # Сохраняем код для логики ретраев
            }
            await add_to_pending(fiscal_key, pending_data)
            from handlers.notifications import scheduler
            schedule_async_job(scheduler, retry_check, wait_min, bot, fiscal_key, "qr")

        # Создаем клавиатуру с кнопкой отмены
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        safe_fiscal_key = fiscal_key.replace("=", "_").replace("&", "_").replace(".", "_")

        cancel_keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[
                InlineKeyboardButton(
                    text="❌ Отключить проверку", 
                    callback_data=f"cancel_check:{safe_fiscal_key}"
                )
            ]]
        )

        user_msg = f"⏳ Чек сейчас обрабатывается сервером. Я попробую снова через {wait_min} мин."
        logger.info(f"ℹ️ API code=4 for {fiscal_key}, scheduled retry in {wait_min} min. (попытка {retries + 1})")
        return {
            "delayed": True, 
            "message": user_msg,
            "keyboard": cancel_keyboard,
            "retry_type": "rate_limit"  # Тип для разной логики ретраев
        }

    else:
        logger.error(f"Ошибка: code={code}, data={result.get('data')}")
        return None

async def confirm_manual_api(bot: Bot, data: Dict[str, Any], user: Any = None, chat_id: int | None = None) -> Tuple[bool, str, Optional[Dict]]:
    """
    Запрос к proverkacheka.com API для manual чека (Формат 1 из спецификации).
    POST form-data: token, fn, fd, fp, t=YYYYMMDDTHHMM, n=op_type (1-4), s=RUB (str, e.g., '27.20'), qr=0.
    Возвращает: (success: bool, message: str, parsed_data: dict or None)
    """
    try:
        fn = data.get('fn', '').strip()
        fd = data.get('fd', '').strip()
        fp = data.get('fp', '').strip()
        s = float(data.get('s', 0))
        date_str = data.get('date', '').strip()
        time_str = data.get('time', '').strip()
        op_type = int(data.get('op_type', 1))

        if not all([fn, fd, fp, date_str]):
            return False, "❌ Недостаточно данных (FN, FD, FP, дата обязательны).", None

        # Форматируем дату: ддммгг → YYYYMMDD
        if len(date_str) == 6:
            day, month, year = date_str[:2].zfill(2), date_str[2:4].zfill(2), f"20{date_str[4:6]}"
            full_date = f"{year}{month}{day}"
        else:
            try:
                dt = datetime.strptime(date_str, "%d.%m.%Y")
                full_date = dt.strftime("%Y%m%d")
            except ValueError:
                full_date = datetime.now().strftime("%Y%m%d")

        # Время: ЧЧММ или ЧЧ:ММ → HHMM
        if ':' in time_str:
            full_time = time_str.replace(":", "")
        elif len(time_str) == 4:
            full_time = time_str
        else:
            full_time = datetime.now().strftime("%H%M")

        t_combined = f"{full_date}T{full_time}"
        sum_rub = f"{s:.2f}"
        n_type = str(op_type)
        
        # Формируем qrraw строку для единообразия
        qr_raw = f"t={t_combined}&s={sum_rub}&fn={fn}&i={fd}&fp={fp}&n={n_type}"

        # FormData
        form_data = aiohttp.FormData()
        form_data.add_field("token", PROVERKACHEKA_TOKEN)
        form_data.add_field("fn", fn)
        form_data.add_field("fd", fd)
        form_data.add_field("fp", fp)
        form_data.add_field("t", t_combined)
        form_data.add_field("n", n_type)
        form_data.add_field("s", sum_rub)
        form_data.add_field("qr", "0")

        user_id_log = user.id if user else 'retry'
        logger.info(f"confirm_manual_api: Запрос к proverkacheka API с fn={fn}, fd={fd}, fp={fp}, t={t_combined}, n={n_type}, s={sum_rub}, qr=0, user_id={user_id_log}")

        url = "https://proverkacheka.com/api/v1/check/get"
        timeout = aiohttp.ClientTimeout(total=30)

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(url, data=form_data) as response:
                        response_text = await response.text()
                        logger.info(f"API response: status={response.status}, text={response_text[:200]}...")

                        if response.status == 200:
                            try:
                                result = json.loads(response_text)
                                code = result.get("code")
                                fiscal_key = f"{fn}:{fd}:{fp}"
                                
                                if code == 1:
                                    data_json = result.get("data", {}).get("json", {})
                                    if data_json:
                                        items_raw = data_json.get("items", [])
                                        items = []
                                        excluded_sum = 0.0
                                        excluded_items_list = []

                                        for item in items_raw:
                                            name = item.get("name", "Неизвестно").strip()
                                            total_sum_item = safe_float(item.get("sum", 0)) / 100.0
                                            unit_price = safe_float(item.get("price", 0)) / 100.0
                                            quantity = item.get("quantity", 1)

                                            if is_excluded(name):
                                                logger.info(f"Найден исключённый товар: '{name}' (сумма: {total_sum_item})")
                                                excluded_sum += total_sum_item
                                                excluded_items_list.append(name)
                                                continue

                                            items.append({
                                                "name": name,
                                                "sum": total_sum_item,
                                                "price": unit_price,
                                                "quantity": quantity
                                            })

                                        total_sum_raw = safe_float(data_json.get("totalSum", 0)) / 100.0
                                        filtered_total = total_sum_raw - excluded_sum

                                        parsed_data = {
                                            "fiscal_doc": data_json.get("fiscalDocumentNumber", f"{fn}-{fd}-{fp}"),
                                            "qr_string": result.get("request", {}).get("qrraw", qr_raw),  # Используем сформированную строку
                                            "date": data_json.get("ticketDate", full_date).replace("-", "."),
                                            "store": data_json.get("user", data_json.get("retailPlace", "Неизвестно")),
                                            "items": items if items else [{"name": "Товар из чека", "sum": s, "price": s, "quantity": 1}],
                                            "operation_type": data_json.get("operationType", op_type),
                                            "total_sum": filtered_total,
                                            "excluded_sum": excluded_sum,
                                            "excluded_items": excluded_items_list,
                                            "nds18": data_json.get("nds18", 0) / 100.0,
                                            "nds": data_json.get("nds", 0) / 100.0,
                                            "nds0": data_json.get("nds0", 0) / 100.0,
                                            "ndsNo": data_json.get("ndsNo", 0) / 100.0,
                                            "cashTotalSum": data_json.get("cashTotalSum", 0) / 100.0,
                                            "ecashTotalSum": data_json.get("ecashTotalSum", 0) / 100.0
                                        }
                                        logger.info(f"API success: code=1, parsed_data keys={list(parsed_data.keys())}, items_count={len(items)}")
                                        await add_to_processed(fiscal_key)
                                        return True, "✅ Данные чека получены из API.", parsed_data
                                    else:
                                        logger.error("Нет data.json в ответе")
                                        return False, "❌ Нет данных чека в ответе API.", None
                                
                                # ⏳ Обработка отложенных запросов - УЛУЧШЕННАЯ ЛОГИКА
                                elif code in (2, 5):
                                    if await is_pending_or_processed(fiscal_key):
                                        return False, "⏳ Чек уже в обработке. Ожидайте уведомления.", None

                                    pending_data = {
                                        "type": "manual",
                                        "manual_data": data,
                                        "user_id": user.id if user else None,
                                        "chat_id": chat_id,
                                        "retries": 0,
                                        "created_at": time.time(),
                                    }
                                    await add_to_pending(fiscal_key, pending_data)

                                    from handlers.notifications import scheduler
                                    # Используем ту же логику ретраев что и для QR
                                    schedule_async_job(scheduler, retry_check, RETRY_INTERVAL_MIN, bot, fiscal_key, "manual")

                                    return False, "⏳ Чек пока не в базе ФНС (код 2/5). Запускаю фоновую проверку. При успешном запросе пришлю уведомление.", None

                                elif code == 3:
                                    if attempt < max_retries:
                                        logger.warning("Rate limit (code=3). Retry через 60s.")
                                        await asyncio.sleep(60)
                                        continue
                                    return False, "❌ Превышено количество запросов (code=3). Подождите 1 мин и попробуйте снова.", None
                                
                                # В функции confirm_manual_api обновите обработку кода 4:
                                elif code == 4:
                                    data_field = result.get("data")
                                    if isinstance(data_field, dict):
                                        wait_seconds = data_field.get("wait")
                                    else:
                                        wait_seconds = None

                                    if not wait_seconds:
                                        wait_seconds = 120

                                    wait_min = max(1, int((wait_seconds + 59) // 60))

                                    if await is_pending_or_processed(fiscal_key):
                                        return False, f"⏳ Чек уже обрабатывается. Попробую снова через {wait_min} мин.", None

                                    pending_data = {
                                        "type": "manual", 
                                        "manual_data": data,
                                        "user_id": user.id if user else None,
                                        "chat_id": chat_id,
                                        "retries": 0,
                                        "created_at": time.time(),
                                        "last_code": code  # Сохраняем код
                                    }
                                    await add_to_pending(fiscal_key, pending_data)

                                    from handlers.notifications import scheduler
                                    schedule_async_job(scheduler, retry_check, wait_min, bot, fiscal_key, "manual")

                                    return False, f"⏳ Чек сейчас обрабатывается сервером. Проверю снова через {wait_min} мин.", None
                                
                                else:
                                    error_msg = result.get("data", {}).get("message", f"Неизвестная ошибка (code={code})")
                                    if attempt < max_retries:
                                        logger.warning(f"API error code={code}: {error_msg}. Retry {attempt}/{max_retries} через 5s.")
                                        await asyncio.sleep(5)
                                        continue
                                    return False, f"❌ Ошибка API (code={code}: {error_msg}). Проверьте FN/FD/FP.", None
                            
                            except json.JSONDecodeError as e:
                                logger.error(f"Invalid JSON from API: {str(e)}, text={response_text[:200]}...")
                                if "<html" in response_text.lower() or "<!doctype" in response_text.lower():
                                    return False, "❌ Неверный ответ от API (HTML вместо JSON). Проверьте токен.", None
                                return False, "❌ Некорректный ответ от API (не JSON).", None

                        elif response.status in [401, 404, 429]:
                            if response.status == 429:
                                if attempt < max_retries:
                                    logger.warning("HTTP Rate limit 429. Retry через 10s.")
                                    await asyncio.sleep(10)
                                    continue
                                return False, "❌ Лимит запросов (HTTP 429). Подождите 1 мин.", None
                            else:
                                if attempt < max_retries:
                                    logger.warning(f"HTTP error {response.status}. Retry {attempt}/{max_retries} через 5s.")
                                    await asyncio.sleep(5)
                                    continue
                                return False, f"❌ HTTP Ошибка: code={response.status}. Проверьте данные.", None

                        else:
                            return False, f"❌ Ошибка API: HTTP {response.status}, {response_text[:100]}...", None

            except aiohttp.ClientTimeout:
                if attempt < max_retries:
                    logger.warning(f"Timeout. Retry {attempt}/{max_retries}.")
                    await asyncio.sleep(5)
                    continue
                return False, "❌ Таймаут запроса к API. Проверьте интернет.", None
            except aiohttp.ClientError as e:
                logger.error(f"Request error: {str(e)}")
                if attempt < max_retries:
                    await asyncio.sleep(5)
                    continue
                return False, f"⚠️ Ошибка сети: {str(e)}.", None
            except Exception as e:
                logger.error(f"Unexpected error in API request: {str(e)}")
                if attempt < max_retries:
                    await asyncio.sleep(5)
                    continue
                return False, f"⚠️ Неожиданная ошибка: {str(e)}.", None

        return False, "❌ Не удалось получить данные чека после 3 попыток.", None

    except Exception as e:
        logger.error(f"Ошибка в confirm_manual_api: {str(e)}, data={data}", exc_info=True)
        return False, f"⚠️ Внутренняя ошибка: {str(e)}. Обратитесь к админу.", None

# ==========================================================
# 🔄 Безопасный запуск async-задач через APScheduler
# ==========================================================
# Часовой пояс UTC+5

LOCAL_TZ = timezone(timedelta(hours=5))

def schedule_async_job(scheduler, coro_func, delay_min: int, *args):
    """
    Безопасное добавление async-задачи для APScheduler.
    Работает в UTC+5 и корректно использует основной asyncio loop.
    """
    run_date = datetime.now(LOCAL_TZ) + timedelta(minutes=delay_min)
    fiscal_key = str(args[1]) if len(args) > 1 else str(time.time())
    job_id = f"{coro_func.__name__}:{fiscal_key}"

    loop = asyncio.get_event_loop()

    async def wrapper():
        logger.info(f"▶️ [JOB START] {coro_func.__name__} для {fiscal_key}")
        try:
            await coro_func(*args)
            logger.info(f"✅ [JOB DONE] {coro_func.__name__} для {fiscal_key}")
        except Exception as e:
            logger.error(f"⚠️ Ошибка в async job {coro_func.__name__}: {e}", exc_info=True)

    def run_in_main_loop():
        """Запускает задачу в основном event loop, даже если APScheduler в другом потоке."""
        try:
            asyncio.run_coroutine_threadsafe(wrapper(), loop)
        except Exception as e:
            logger.error(f"🚨 Ошибка запуска coroutine в основном loop: {e}", exc_info=True)

    # Удаляем старую задачу с тем же ID, если есть
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
        logger.warning(f"♻️ Старое задание {job_id} заменено новым")

    scheduler.add_job(
        func=run_in_main_loop,
        trigger="date",
        run_date=run_date,
        id=job_id,
        replace_existing=True,
        misfire_grace_time=60,
        timezone=LOCAL_TZ,
    )

    logger.info(
        f"🕐 Задача '{coro_func.__name__}' ({job_id}) запланирована на {run_date.strftime('%H:%M:%S %Z')}"
    )
    return job_id



# ==========================================================
# ♻️ Переписанный retry_check с полной совместимостью
# ==========================================================
from utils import RETRY_INTERVAL_MIN, MAX_RETRIES

# ==========================================================
# ♻️ Умный retry_check с уведомлениями и адаптивным интервалом
# ==========================================================
async def retry_check(bot: Bot, fiscal_key: str, check_type: str):
    """
    Фоновая проверка чека через APScheduler.
    • Для кода 4 (rate limit): 2 мин → 5 мин → 30 мин → 60 мин (макс 4 попытки)
    • Для кодов 2/5 (not found): 60 мин (макс 8 попыток)
    """
    from handlers.notifications import scheduler

    pending = await get_pending(fiscal_key)
    if not pending:
        logger.info(f"⚠️ Pending задача не найдена: {fiscal_key}")
        return

    retries = pending.get("retries", 0) + 1
    last_code = pending.get("last_code")
    
    # Разные лимиты для разных типов ошибок
    if last_code == 4:
        max_retries = 4  # Макс 4 попытки для rate limit
        retry_type = "rate_limit"
    else:
        max_retries = MAX_RETRIES  # 8 попыток для not found
        retry_type = "not_found"
    
    logger.info(f"▶️ RETRY_TRIGGERED: {fiscal_key}, попытка {retries}/{max_retries}, тип={check_type}, ошибка={retry_type}")

    # --- Если превышен лимит ---
    if retries > max_retries:
        if retry_type == "rate_limit":
            message = f"❌ Чек не обработан после {max_retries} попыток. Сервер перегружен.\nПопробуйте позже или добавьте чек вручную."
        else:
            message = f"❌ Чек не найден после {max_retries} попыток ({max_retries} часов).\nПопробуйте позже или добавьте чек вручную."
        
        await bot.send_message(pending.get("chat_id"), message)
        logger.warning(f"❌ Чек {fiscal_key} не обработан после {max_retries} попыток — удалён из pending.")
        await remove_pending(fiscal_key)
        return

    try:
        parsed_data = None
        chat_id = pending.get("chat_id")

        # --- Проверка QR ---
        if check_type == "qr":
            parsed_data = await parse_qr_from_photo(
                bot,
                pending.get("file_id"),
                pending.get("user_id"),
                chat_id
            )

        # --- Проверка manual ---
        elif check_type == "manual":
            success, msg, parsed_data = await confirm_manual_api(
                bot,
                pending.get("manual_data"),
                type("User", (), {"id": pending.get("user_id")}),
                chat_id
            )
            if not success:
                parsed_data = None

        # --- ✅ Чек найден ---
        if parsed_data and not parsed_data.get("delayed"):
            logger.info(f"✅ Чек найден ({fiscal_key}) на попытке {retries}")

            from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
            inline_kb = InlineKeyboardMarkup(
                inline_keyboard=[[
                    InlineKeyboardButton(
                        text="✅ Продолжить добавление",
                        callback_data=f"continue_add:{fiscal_key}"
                    )
                ]]
            )

            await bot.send_message(
                chat_id,
                f"🎉 Чек найден после {retries} проверок!\n"
                f"Теперь можно продолжить добавление:",
                reply_markup=inline_kb
            )

            await cache_set(f"parsed_data:{fiscal_key}", parsed_data, expire=3600)
            await remove_pending(fiscal_key)
            await add_to_processed(fiscal_key)
            return

        # --- ❗ Чек не найден — планируем следующую проверку ---
        # Разные интервалы для разных типов ошибок
        if retry_type == "rate_limit":
            # Прогрессивные интервалы для кода 4
            if retries == 1:
                interval_min = 2
            elif retries == 2:
                interval_min = 5
            elif retries == 3:
                interval_min = 30
            else:
                interval_min = 60
            
            # Сообщения для rate limit
            if retries == 1:
                message_text = "⏳ Чек сейчас обрабатывается сервером. Я попробую снова через 2 мин."
            elif retries == 2:
                message_text = "⏳ Чек всё ещё обрабатывается сервером. Я попробую снова через 5 мин."
            elif retries == 3:
                message_text = "⏳ Чек обрабатывается дольше обычного. Я попробую снова через 30 мин."
            else:
                message_text = "⏳ Чек продолжает обрабатываться. Я проверяю каждый час."
            safe_fiscal_key = fiscal_key.replace("=", "_").replace("&", "_").replace(".", "_")

            # Добавляем кнопку отмены для rate limit
            from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
            cancel_keyboard = InlineKeyboardMarkup(
                inline_keyboard=[[
                    InlineKeyboardButton(
                        text="❌ Отключить проверку", 
                        callback_data=f"cancel_check:{safe_fiscal_key}"
                    )
                ]]
            )
            
            await bot.send_message(chat_id, message_text, reply_markup=cancel_keyboard)
            
        else:
            # Для кодов 2/5 - фиксированный интервал 60 минут
            interval_min = RETRY_INTERVAL_MIN  # 60 минут
            
            # Только первое уведомление для not found
            if retries == 1:
                await bot.send_message(
                    chat_id,
                    "⏳ Чек пока не в базе ФНС (код 2/5). Запускаю фоновую проверку — теперь я буду проверять чек каждый час.\n"
                    "💡 Когда чек появится, я пришлю уведомление, и вы сможете продолжить добавление.\n"
                    "Повторно добавлять чек не потребуется."
                )

        logger.info(
            f"🕐 Чек {fiscal_key} пока не найден (попытка {retries}/{max_retries}, тип={retry_type}). "
            f"Следующая через {interval_min} мин."
        )

        # сохраняем прогресс
        pending["retries"] = retries
        await update_pending(fiscal_key, pending)

        # запланировать следующую задачу
        schedule_async_job(scheduler, retry_check, interval_min, bot, fiscal_key, check_type)

    except Exception as e:
        logger.error(f"⚠️ Ошибка в retry_check ({fiscal_key}): {e}", exc_info=True)
        pending["retries"] = retries
        await update_pending(fiscal_key, pending)
        schedule_async_job(scheduler, retry_check, RETRY_INTERVAL_MIN, bot, fiscal_key, check_type)

async def get_pending_stats() -> dict:
    """Статистика pending задач"""
    keys = await redis_client.keys("pending:*")
    stats = {
        'total': len(keys),
        'by_type': {},
        'old_tasks': []
    }
    
    for key in keys:
        data = await cache_get(key)
        if data:
            check_type = data.get('type', 'unknown')
            stats['by_type'][check_type] = stats['by_type'].get(check_type, 0) + 1
            
            # Задачи старше 1 часа
            if time.time() - data.get('created_at', 0) > 3600:
                stats['old_tasks'].append({
                    'key': key,
                    'type': check_type,
                    'retries': data.get('retries', 0),
                    'age_hours': (time.time() - data.get('created_at', 0)) / 3600
                })
    
    return stats

async def send_retry_notification(bot: Bot, pending_data: dict, result: str, retries: int, fiscal_key: str):  # ✅ ДОБАВИТЬ fiscal_key
    """Умные уведомления о статусе проверки"""
    chat_id = pending_data.get('chat_id')
    if not chat_id:
        return
        
    messages = {
        'success': f"🎉 Чек найден после {retries} проверок!",
        'retrying': f"⏳ Проверяю чек... ({retries}/12 попыток)",
        'timeout': "❌ Чек не найден в течение 1 часа",
        'error': "⚠️ Ошибка при проверке чека"
    }
    
    message = messages.get(result, messages['error'])
    
    if result == 'success':
        # Добавляем кнопку продолжения
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton  # ✅ ДОБАВИТЬ импорт
        inline_kb = InlineKeyboardMarkup(
            inline_keyboard=[[
                InlineKeyboardButton(
                    text="✅ Продолжить добавление", 
                    callback_data=f"continue_add:{fiscal_key}"  # ✅ ИСПОЛЬЗУЕМ fiscal_key
                )
            ]]
        )
        await bot.send_message(chat_id, message, reply_markup=inline_kb)
    else:
        await bot.send_message(chat_id, message)

OP_TYPE_MAPPING = {
    "приход": 1,
    "возврат прихода": 2,
    "расход": 3,
    "возврат расхода": 4
}

def reset_keyboard() -> ReplyKeyboardMarkup:
    """Общая клавиатура сброса."""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Сброс")]],
        resize_keyboard=True
    )


def norm(s: str) -> str:
    """
    Нормализация строки для match (lower, strip, single spaces).
    """
    s = (s or "").lower()
    s = " ".join(s.split())  # Удалить лишние пробелы
    return s

async def restore_pending_tasks(bot: Bot):
    """Восстанавливает pending задачи при перезапуске бота"""
    try:
        from handlers.notifications import scheduler
        
        # Получаем все pending задачи из Redis
        pending_keys = await redis_client.keys("pending:*")
        restored_count = 0
        
        logger.info(f"🔍 Найдено {len(pending_keys)} pending задач для восстановления")
        
        for key in pending_keys:
            try:
                pending_data = await cache_get(key)
                if not pending_data:
                    continue
                    
                fiscal_key = key.replace("pending:", "")
                check_type = pending_data.get("type", "qr")
                retries = pending_data.get("retries", 0)
                last_code = pending_data.get("last_code")
                created_at = pending_data.get("created_at", 0)
                
                # Пропускаем слишком старые задачи (старше 24 часов)
                if time.time() - created_at > 86400:  # 24 часа
                    logger.info(f"🗑️ Удалена устаревшая задача: {fiscal_key}")
                    await remove_pending(fiscal_key)
                    continue
                
                # Определяем интервал в зависимости от типа ошибки и количества попыток
                if last_code == 4:
                    # Rate limit - прогрессивные интервалы
                    if retries == 0:
                        interval_min = 2
                    elif retries == 1:
                        interval_min = 5
                    elif retries == 2:
                        interval_min = 30
                    else:
                        interval_min = 60
                    error_type = "rate_limit"
                else:
                    # Not found - 60 минут
                    interval_min = RETRY_INTERVAL_MIN
                    error_type = "not_found"
                
                # Планируем задачу
                schedule_async_job(scheduler, retry_check, interval_min, bot, fiscal_key, check_type)
                restored_count += 1
                
                logger.info(f"♻️ Восстановлена задача: {fiscal_key}, тип={check_type}, попытки={retries}, ошибка={error_type}, интервал={interval_min}мин")
                
            except Exception as e:
                logger.error(f"❌ Ошибка восстановления задачи {key}: {e}")
                continue
        
        logger.info(f"✅ Восстановлено {restored_count} pending задач при запуске")
        return restored_count
        
    except Exception as e:
        logger.error(f"❌ Ошибка восстановления pending задач: {e}")
        return 0