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

logger = logging.getLogger("AccountingBot")

# Redis с pool и reconnect
pool = redis.ConnectionPool(host='localhost', port=6379, db=0, decode_responses=True, max_connections=10, retry_on_timeout=True)
redis_client = redis.Redis(connection_pool=pool)

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

# НОВОЕ: Функции для pending checks
async def is_pending_or_processed(fiscal_key: str) -> bool:
    """Проверяет, в обработке ли чек или уже сохранён."""
    processed = await redis_client.sismember("processed_fiscals", fiscal_key)
    pending = await redis_client.hexists("pending_checks", fiscal_key)
    return processed or pending

async def add_to_pending(fiscal_key: str, data: dict, expire: int = 3600) -> bool:
    """Добавляет в pending с expire (1 час)."""
    data['retries'] = 0
    await redis_client.hset("pending_checks", fiscal_key, json.dumps(data))
    await redis_client.expire("pending_checks", expire)  # Общий expire для hash
    return True

async def get_pending(fiscal_key: str) -> dict | None:
    data = await redis_client.hget("pending_checks", fiscal_key)
    return json.loads(data) if data else None

async def remove_pending(fiscal_key: str):
    await redis_client.hdel("pending_checks", fiscal_key)

async def add_to_processed(fiscal_key: str):
    await redis_client.sadd("processed_fiscals", fiscal_key)

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

# Тестовые константы (включить/выключить)
TEST_MODE = True  # Если True, всегда симулирует code=2 для любой фото/ручного ввода
PROD_RETRY_INTERVAL_MIN = 60  # Прод: 60 мин (1 час)
PROD_MAX_RETRIES = 12  # Прод: 12 попыток (12 часов)
TEST_RETRY_INTERVAL_MIN = 1  # Тест: 1 минута
TEST_MAX_RETRIES = 3  # Тест: 3 попытки, успех на 3-й

async def parse_qr_from_photo(bot, file_id) -> dict | None:
    file = await bot.get_file(file_id)
    file_path = file.file_path
    photo = await bot.download_file(file_path)
    
    if TEST_MODE:
        # Симуляция code=2 для теста
        qrraw = "test_qrraw"  # Mock qrraw
        fiscal_key = qrraw if qrraw else "temp_qr_" + str(hash(file_id))
        if await is_pending_or_processed(fiscal_key):
            logger.info(f"Дубликат pending/processed: {fiscal_key}")
            return None
        pending_data = {
            "type": "qr",
            "file_id": file_id,
            "user_id": bot.id,  # Заменить на реальный в хендлере
            "chat_id": None,  # Заполнить в хендлере
        }
        await add_to_pending(fiscal_key, pending_data)
        from handlers.notifications import scheduler  # Lazy import
        scheduler.add_job(retry_check, 'interval', minutes=TEST_RETRY_INTERVAL_MIN, args=(bot, fiscal_key, "qr"))
        return {"delayed": True, "message": "⏳ Чек пока не в базе ФНС. Проверю позже."}
    
    # Оригинальный код (не используется в TEST_MODE)
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        form = aiohttp.FormData()
        form.add_field("qrfile", photo, filename="check.jpg", content_type="image/jpeg")
        form.add_field("token", PROVERKACHEKA_TOKEN)
        async with session.post("https://proverkacheka.com/api/v1/check/get", data=form) as response:
            if response.status == 200:
                result = await response.json()
                code = result.get("code")
                if code == 1:
                    data_json = result.get("data", {}).get("json", {})
                    if data_json:
                        items = data_json.get("items", [])
                        excluded_items = get_excluded_items()
                        filtered_items = []
                        excluded_sum = 0.0
                        total_sum_raw = safe_float(data_json.get("totalSum", 0)) / 100
                        if total_sum_raw == 0:
                            total_sum_raw = sum(safe_float(it.get("sum", 0)) / 100 for it in items)
                        for item in items:
                            name = item.get("name", "Неизвестно").strip()
                            total_sum_item = safe_float(item.get("sum", 0)) / 100
                            unit_price = safe_float(item.get("price", 0)) / 100
                            quantity = item.get("quantity", 1)
                            if is_excluded(name):
                                excluded_sum += total_sum_item
                                continue
                            filtered_items.append({
                                "name": name,
                                "sum": total_sum_item,
                                "price": unit_price,
                                "quantity": quantity
                            })
                        filtered_total = total_sum_raw - excluded_sum
                        parsed_data = {
                            "fiscal_doc": data_json.get("fiscalDocumentNumber", "unknown"),
                            "date": data_json.get("dateTime", "").split("T")[0].replace("-", "."),
                            "store": data_json.get("user", "Неизвестно"),
                            "items": filtered_items,
                            "qr_string": result.get("request", {}).get("qrraw", ""),
                            "operation_type": data_json.get("operationType", 1),
                            "prepaid_sum": safe_float(data_json.get("prepaidSum", 0)) / 100,
                            "total_sum": filtered_total,
                            "totalSum": total_sum_raw,
                            "excluded_sum": excluded_sum,
                            "excluded_items": [item.get("name") for item in items if is_excluded(item.get("name", "").strip())]
                        }
                        fiscal_key = f"{parsed_data['fiscal_doc']}"
                        await add_to_processed(fiscal_key)
                        return parsed_data
                elif code == 2:
                    qrraw = result.get("request", {}).get("qrraw", "")
                    fiscal_key = qrraw if qrraw else "temp_qr_" + str(hash(file_id))
                    if await is_pending_or_processed(fiscal_key):
                        logger.info(f"Дубликат pending/processed: {fiscal_key}")
                        return None
                    pending_data = {
                        "type": "qr",
                        "file_id": file_id,
                        "user_id": bot.id,
                        "chat_id": None,
                    }
                    await add_to_pending(fiscal_key, pending_data)
                    from handlers.notifications import scheduler
                    scheduler.add_job(retry_check, 'interval', minutes=PROD_RETRY_INTERVAL_MIN, args=(bot, fiscal_key, "qr"))
                    return {"delayed": True, "message": "⏳ Чек пока не в базе ФНС. Проверю позже."}
                else:
                    logger.error(f"Ошибка: code={code}, data={result.get('data')}")
                    return None
            else:
                logger.error(f"HTTP error: {response.status}")
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

        if TEST_MODE:
            # Симуляция code=2 для теста
            fiscal_key = f"{fn}:{fd}:{fp}"
            if await is_pending_or_processed(fiscal_key):
                return False, "❌ Чек уже в обработке или сохранён.", None
            pending_data = {
                "type": "manual",
                "manual_data": data,
                "user_id": user.id if user else None,
                "chat_id": chat_id,
                "retries": 0
            }
            await add_to_pending(fiscal_key, pending_data)
            from handlers.notifications import scheduler  # Lazy import
            scheduler.add_job(retry_check, 'interval', minutes=TEST_RETRY_INTERVAL_MIN, args=(bot, fiscal_key, "manual"))
            return False, "⏳ Данные чека пока не готовы. Автоматическая проверка запущена.", None

        # Оригинальный код (не в TEST_MODE)
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
                                            "qr_string": result.get("request", {}).get("qrraw", f"t={t_combined}&s={sum_rub}&fn={fn}&i={fd}&fp={fp}&n={n_type}"),
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
                                elif code == 2:
                                    if await is_pending_or_processed(fiscal_key):
                                        return False, "❌ Чек уже в обработке или сохранён.", None
                                    pending_data = {
                                        "type": "manual",
                                        "manual_data": data,
                                        "user_id": user.id if user else None,
                                        "chat_id": chat_id,
                                        "retries": 0
                                    }
                                    await add_to_pending(fiscal_key, pending_data)
                                    from handlers.notifications import scheduler
                                    scheduler.add_job(retry_check, 'interval', minutes=PROD_RETRY_INTERVAL_MIN, args=(bot, fiscal_key, "manual"))
                                    return False, "⏳ Данные чека пока не готовы. Автоматическая проверка запущена.", None
                                elif code == 3:
                                    if attempt < max_retries:
                                        logger.warning("Rate limit (code=3). Retry через 60s.")
                                        await asyncio.sleep(60)
                                        continue
                                    return False, "❌ Превышено количество запросов (code=3). Подождите 1 мин и попробуйте снова.", None
                                elif code == 4:
                                    delay = result.get("data", {}).get("wait", 5)
                                    if attempt < max_retries:
                                        logger.warning(f"Ожидание (code=4, wait={delay}s). Retry через {delay}s.")
                                        await asyncio.sleep(delay)
                                        continue
                                    return False, f"❌ Ожидание перед повторным запросом (code=4, wait={delay}s).", None
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
                                    return False, "❌ Неверный ответ от API (HTML вместо JSON). Проверьте токен или используйте фото QR.", None
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

        
async def retry_check(bot: Bot, fiscal_key: str, check_type: str):
    pending = await get_pending(fiscal_key)
    if not pending:
        return
    retries = pending.get('retries', 0) + 1
    if retries > 12:  # Max 1 час
        from handlers.notifications import send_notification  # Lazy import to avoid cycle
        await send_notification(bot, action="❌ Ошибка: Чек не появился в ФНС после 1 часа.", is_group=False, chat_id=pending['chat_id'])
        await remove_pending(fiscal_key)
        return

    # Повторный запрос
    if check_type == "qr":
        # Заново parse_qr_from_photo с file_id
        parsed = await parse_qr_from_photo(bot, pending['file_id'])
    else:  # manual
        success, msg, parsed = await confirm_manual_api(bot, pending['manual_data'], None, pending['chat_id'])

    if isinstance(parsed, dict) and not parsed.get("delayed"):
        # Успех: Уведомить с кнопкой
        inline_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Продолжить добавление", callback_data=f"continue_add:{fiscal_key}")]])
        await bot.send_message(pending['chat_id'], "✅ Чек найден в ФНС! Нажмите, чтобы продолжить.", reply_markup=inline_kb)
        # Сохранить parsed в Redis для восстановления
        await cache_set(f"parsed_data:{fiscal_key}", parsed, expire=3600)
        await remove_pending(fiscal_key)
        await add_to_processed(fiscal_key)
    else:
        # Ещё code=2: update retries и перезапланировать
        pending['retries'] = retries
        await add_to_pending(fiscal_key, pending)
        from handlers.notifications import scheduler  # Lazy import to avoid cycle
        scheduler.add_job(retry_check, 'interval', minutes=5, args=(bot, fiscal_key, check_type))

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