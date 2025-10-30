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
import cv2
import numpy as np
from pyzbar.pyzbar import decode
import asyncio


logger = logging.getLogger("AccountingBot")
PROVERKA_API_URL = "https://proverkacheka.com/api/v1/check/get"


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

# ==================== 1. QR из фотографии ====================

async def parse_qr_from_photo(bot, file_id: str) -> dict | None:
    """
    Загружает фото с Telegram, распознаёт QR и отправляет в API.
    Возвращает dict или None.
    """
    try:
        # 1️⃣ Скачиваем изображение корректно
        file = await bot.get_file(file_id)
        file_stream = await bot.download_file(file.file_path)

        file_bytes = file_stream.read()
        if not file_bytes:
            logger.error("Не удалось прочитать байты изображения.")
            return None

        # 2️⃣ Декодирование в OpenCV
        image_array = np.frombuffer(file_bytes, np.uint8)
        image = cv2.imdecode(image_array, cv2.IMREAD_COLOR)

        if image is None:
            logger.error("cv2.imdecode вернул None — неверный формат изображения.")
            return None

        # 3️⃣ Поиск QR
        decoded = decode(image)
        if not decoded:
            logger.warning("QR-код не найден на изображении.")
            return None

        qr_raw = decoded[0].data.decode("utf-8").strip()
        logger.info(f"✅ QR распознан: {qr_raw}")

        # 4️⃣ Универсальный запрос
        success, msg, parsed = await process_check_from_qrraw(qr_raw)

        if not success or not parsed:
            logger.error(f"❌ Ошибка при обработке QR: {msg}")
            return None

        parsed["qr_string"] = qr_raw
        parsed["fiscal_doc"] = parsed.get("fiscal_doc") or "N/A"

        return parsed

    except Exception as e:  # ✅ теперь ловим только корректные исключения
        logger.exception(f"❌ ИСКЛЮЧЕНИЕ в parse_qr_from_photo: {e}")
        return None



# ==================== 2. QR из ручного ввода ====================

async def build_qr_from_manual(data: dict) -> str | None:
    """
    Формирует строку qrraw из ручного ввода FN, FD, FP, суммы и даты.
    Пример результата: t=20251029T1423&s=123.45&fn=9282000100012345&i=12345&fp=9876543210&n=1
    """
    try:
        fn = data.get("fn", "").strip()
        fd = data.get("fd", "").strip()
        fp = data.get("fp", "").strip()
        s = float(data.get("s", 0))
        date_str = data.get("date", "").strip()
        time_str = data.get("time", "").strip()
        n_type = str(data.get("op_type", 1))

        if not all([fn, fd, fp, date_str]):
            logger.warning("Недостаточно данных для формирования QR.")
            return None

        # 🕓 Форматируем дату
        if len(date_str) == 6:  # ддммгг
            day, month, year = date_str[:2], date_str[2:4], f"20{date_str[4:6]}"
            full_date = f"{year}{month}{day}"
        else:
            try:
                dt = datetime.strptime(date_str, "%d.%m.%Y")
                full_date = dt.strftime("%Y%m%d")
            except ValueError:
                full_date = datetime.now().strftime("%Y%m%d")

        # ⏰ Форматируем время
        if ":" in time_str:
            full_time = time_str.replace(":", "")
        elif len(time_str) == 4:
            full_time = time_str
        else:
            full_time = datetime.now().strftime("%H%M")

        t = f"{full_date}T{full_time}"
        s_str = f"{s:.2f}"

        qrraw = f"t={t}&s={s_str}&fn={fn}&i={fd}&fp={fp}&n={n_type}"
        logger.info(f"✅ Сформирован QR вручную: {qrraw}")
        return qrraw

    except Exception as e:
        logger.error(f"Ошибка при формировании QR вручную: {e}")
        return None


# ==================== 3. Единая обработка через API ====================

import aiohttp
import asyncio
import json
import time
import logging
from typing import Optional, Tuple, Dict, Any
from config import PROVERKACHEKA_TOKEN

logger = logging.getLogger("AccountingBot")
PROVERKA_API_URL = "https://proverkacheka.com/api/v1/check/get"


async def process_check_from_qrraw(qrraw: str, user_id: Optional[int] = None) -> Tuple[bool, str, Optional[Dict]]:
    """
    Универсальная функция: принимает qrraw, запрашивает API и возвращает результат.
    """
    timeout = aiohttp.ClientTimeout(total=30)
    max_retries = 3

    for attempt in range(1, max_retries + 1):
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                payload = {"token": PROVERKACHEKA_TOKEN, "qrraw": qrraw}
                async with session.post(PROVERKA_API_URL, json=payload) as response:
                    text = await response.text()
                    logger.info(f"[API] HTTP {response.status}: {text[:200]}...")

                    if response.status != 200:
                        return False, f"❌ Ошибка HTTP {response.status}", None

                    result = json.loads(text)
                    code = result.get("code")

                    if code == 1:
                        data_json = result.get("data", {}).get("json", {})

                        if not data_json:
                            return False, "❌ Нет данных JSON в ответе API.", None

                        from exceptions import is_excluded

                        items_raw = data_json.get("items", [])
                        items = []

                        excluded_sum = 0.0
                        excluded_items_list = []

                        # ✅ Важно: добавляем все товары
                        for i in items_raw:
                            name = i.get("name", "Товар").strip()
                            sum_value = i.get("sum", 0) / 100.0
                            price = i.get("price", 0) / 100.0
                            qty = i.get("quantity", 1)

                            item_is_excluded = is_excluded(name)

                            # ✅ Маркируем товар
                            item = {
                                "name": name,
                                "sum": sum_value,
                                "price": price,
                                "quantity": qty,
                                "excluded": item_is_excluded,
                            }

                            if item_is_excluded:
                                excluded_sum += sum_value
                                excluded_items_list.append(name)

                            items.append(item)

                        parsed = {
                            "store": data_json.get("user", "Неизвестно"),
                            "date": data_json.get("dateTime", "").split("T")[0].replace("-", "."),
                            "items": items,  # ✅ Все товары остаются здесь
                            "total_sum": data_json.get("totalSum", 0) / 100.0,
                            "fiscal_doc": str(data_json.get("fiscalDocumentNumber", "")),
                            "fiscal_sign": str(data_json.get("fiscalSign", "")),
                            "fiscal_drive": str(data_json.get("fiscalDriveNumber", "")),
                            "operation_type": data_json.get("operationType"),
                            "qr_string": qrraw,
                            "excluded_items": excluded_items_list,
                            "excluded_sum": round(excluded_sum, 2),
                        }

                        logger.info(
                            f"✅ Успешно получен чек "
                            f"(fiscal_doc={parsed['fiscal_doc']}, total_sum={parsed['total_sum']:.2f}, "
                            f"items_all={len(items)}, excluded={len(excluded_items_list)}, excluded_sum={excluded_sum:.2f})"
                        )

                        return True, "✅ Чек успешно получен.", parsed

                    elif code == 2:
                        return False, "⏳ Чек ещё обрабатывается. Повторите позже.", None
                    elif code == 3:
                        if attempt < max_retries:
                            logger.warning("Превышен лимит запросов. Повтор через 60 секунд...")
                            await asyncio.sleep(60)
                            continue
                        return False, "❌ Превышен лимит запросов API.", None
                    elif code == 4:
                        wait = result.get("data", {}).get("wait", 5)
                        if attempt < max_retries:
                            logger.warning(f"Повтор через {wait} секунд...")
                            await asyncio.sleep(wait)
                            continue
                        return False, f"❌ Подождите {wait} секунд перед повтором.", None

                    else:
                        msg = result.get("data", {}).get("message", f"Неизвестная ошибка (code={code})")
                        return False, f"❌ Ошибка API: {msg}", None

        except aiohttp.ClientTimeout:
            if attempt < max_retries:
                logger.warning(f"⏳ Таймаут. Повтор {attempt}/{max_retries}")
                await asyncio.sleep(5)
                continue
            return False, "❌ Таймаут запроса.", None

        except Exception as e:
            logger.error(f"⚠️ Ошибка process_check_from_qrraw: {e}")
            if attempt < max_retries:
                await asyncio.sleep(3)
                continue
            return False, f"⚠️ Ошибка: {str(e)}", None

    return False, "❌ Не удалось получить чек после 3 попыток.", None





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