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

async def parse_qr_from_photo(bot, file_id) -> dict | None:
    file = await bot.get_file(file_id)
    file_path = file.file_path
    photo = await bot.download_file(file_path)
    
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        form = aiohttp.FormData()
        form.add_field("qrfile", photo, filename="check.jpg", content_type="image/jpeg")
        form.add_field("token", PROVERKACHEKA_TOKEN)
        async with session.post("https://proverkacheka.com/api/v1/check/get", data=form) as response:
            if response.status == 200:
                result = await response.json()
                if result.get("code") == 1:
                    data_json = result.get("data", {}).get("json", {})
                    if data_json:
                        items = data_json.get("items", [])
                        excluded_items = get_excluded_items()
                        filtered_items = []
                        excluded_sum = 0.0

                        for item in items:
                            name = item.get("name", "Неизвестно").strip()
                            total_sum = item.get("sum", 0) / 100
                            unit_price = item.get("price", 0) / 100
                            quantity = item.get("quantity", 1)

                            if is_excluded(name):
                                logger.info(f"Найден исключённый товар: '{name}' (сумма: {total_sum})")
                                excluded_sum += total_sum
                                continue

                            filtered_items.append({
                                "name": name,
                                "sum": total_sum,
                                "price": unit_price,
                                "quantity": quantity
                            })

                        total_sum_raw = data_json.get("totalSum", 0) / 100
                        filtered_total = total_sum_raw - excluded_sum  # Фикс: filtered total

                        return {
                            "fiscal_doc": data_json.get("fiscalDocumentNumber", "unknown"),
                            "date": data_json.get("dateTime", "").split("T")[0].replace("-", "."),
                            "store": data_json.get("retailPlace", "Неизвестно"),
                            "items": filtered_items,
                            "qr_string": result.get("request", {}).get("qrraw", ""),
                            "operation_type": data_json.get("operationType", 1),
                            "prepaid_sum": data_json.get("prepaidSum", 0) / 100,
                            "total_sum": filtered_total,  # Исправлено: filtered
                            "excluded_sum": excluded_sum,
                            "excluded_items": [
                                item.get("name") for item in items if is_excluded(item.get("name", "").strip())
                            ]
                        }
                    else:
                        logger.error("Нет данных JSON в ответе от proverkacheka.com")
                        return None
                else:
                    logger.error(
                        f"Ошибка обработки на proverkacheka.com: code={result.get('code')}, message={result.get('data')}"
                    )
                    return None
            else:
                logger.error(f"Ошибка отправки на proverkacheka.com: status={response.status}")
                return None

async def confirm_manual_api(data: Dict[str, Any], user: Any) -> Tuple[bool, str, Optional[Dict]]:
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
            full_date = f"{year}{month}{day}"  # e.g., 080925 → 20250908
        else:
            # Fallback: парсим как DD.MM.YYYY → YYYYMMDD
            try:
                dt = datetime.strptime(date_str, "%d.%m.%Y")
                full_date = dt.strftime("%Y%m%d")
            except ValueError:
                full_date = datetime.now().strftime("%Y%m%d")

        # Время: ЧЧММ или ЧЧ:ММ → HHMM
        if ':' in time_str:
            full_time = time_str.replace(":", "")  # 18:34 → 1834
        elif len(time_str) == 4:
            full_time = time_str  # 1834
        else:
            full_time = datetime.now().strftime("%H%M")

        # t = YYYYMMDDTHHMM
        t_combined = f"{full_date}T{full_time}"

        # Сумма в RUB (str с десятичной, e.g., '27.20')
        sum_rub = f"{s:.2f}"

        # n = op_type (1=приход, 2=возврат прихода, 3=расход, 4=возврат расхода)
        n_type = str(op_type)

        # FormData по спецификации (multipart/form-data)
        form_data = aiohttp.FormData()
        form_data.add_field("token", PROVERKACHEKA_TOKEN)
        form_data.add_field("fn", fn)
        form_data.add_field("fd", fd)
        form_data.add_field("fp", fp)
        form_data.add_field("t", t_combined)  # YYYYMMDDTHHMM
        form_data.add_field("n", n_type)
        form_data.add_field("s", sum_rub)  # RUB str
        form_data.add_field("qr", "0")  # Manual, не QR

        logger.info(f"confirm_manual_api: Запрос к proverkacheka API с fn={fn}, fd={fd}, fp={fp}, t={t_combined}, n={n_type}, s={sum_rub}, qr=0, user_id={user.id}")

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
                                if code == 1:
                                    # Успех: data.json
                                    data_json = result.get("data", {}).get("json", {})
                                    if data_json:
                                        # Парсинг по спецификации
                                        items_raw = data_json.get("items", [])
                                        items = []
                                        excluded_sum = 0.0
                                        excluded_items_list = []

                                        for item in items_raw:
                                            name = item.get("name", "Неизвестно").strip()
                                            total_sum_item = safe_float(item.get("sum", 0)) / 100.0  # копейки → RUB
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
                                            "store": data_json.get("retailPlace", data_json.get("user", "Неизвестно")),
                                            "items": items if items else [{"name": "Товар из чека", "sum": s, "price": s, "quantity": 1}],  # Fallback
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
                                        return True, "✅ Данные чека получены из API.", parsed_data
                                    else:
                                        logger.error("Нет data.json в ответе")
                                        return False, "❌ Нет данных чека в ответе API.", None
                                elif code == 2:
                                    return False, "⏳ Данные чека пока не готовы. Попробуйте позже.", None
                                elif code == 3:
                                    if attempt < max_retries:
                                        logger.warning("Rate limit (code=3). Retry через 60s.")
                                        time.sleep(60)  # code=3: превышено кол-во запросов, подождать 1 мин
                                        continue
                                    return False, "❌ Превышено количество запросов (code=3). Подождите 1 мин и попробуйте снова.", None
                                elif code == 4:
                                    delay = result.get("data", {}).get("wait", 5)
                                    if attempt < max_retries:
                                        logger.warning(f"Ожидание (code=4, wait={delay}s). Retry через {delay}s.")
                                        time.sleep(delay)
                                        continue
                                    return False, f"❌ Ожидание перед повторным запросом (code=4, wait={delay}s).", None
                                else:  # code=0,5 или другие
                                    error_msg = result.get("data", {}).get("message", f"Неизвестная ошибка (code={code})")
                                    if attempt < max_retries:
                                        logger.warning(f"API error code={code}: {error_msg}. Retry {attempt}/{max_retries} через 5s.")
                                        time.sleep(5)
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
                                    time.sleep(10)
                                    continue
                                return False, "❌ Лимит запросов (HTTP 429). Подождите 1 мин.", None
                            else:
                                if attempt < max_retries:
                                    logger.warning(f"HTTP error {response.status}. Retry {attempt}/{max_retries} через 5s.")
                                    time.sleep(5)
                                    continue
                                return False, f"❌ HTTP Ошибка: code={response.status}. Проверьте данные.", None

                        else:
                            return False, f"❌ Ошибка API: HTTP {response.status}, {response_text[:100]}...", None

            except aiohttp.ClientTimeout:
                if attempt < max_retries:
                    logger.warning(f"Timeout. Retry {attempt}/{max_retries}.")
                    time.sleep(5)
                    continue
                return False, "❌ Таймаут запроса к API. Проверьте интернет.", None
            except aiohttp.ClientError as e:
                logger.error(f"Request error: {str(e)}")
                if attempt < max_retries:
                    time.sleep(5)
                    continue
                return False, f"⚠️ Ошибка сети: {str(e)}.", None
            except Exception as e:
                logger.error(f"Unexpected error in API request: {str(e)}")
                if attempt < max_retries:
                    time.sleep(5)
                    continue
                return False, f"⚠️ Неожиданная ошибка: {str(e)}.", None

        return False, "❌ Не удалось получить данные чека после 3 попыток.", None

    except Exception as e:
        logger.error(f"Ошибка в confirm_manual_api: {str(e)}, data={data}")
        return False, f"⚠️ Внутренняя ошибка: {str(e)}. Обратитесь к админу.", None

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
