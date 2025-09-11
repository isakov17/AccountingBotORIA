from exceptions import is_excluded, get_excluded_items
import logging
import aiohttp
from config import PROVERKACHEKA_TOKEN
import redis.asyncio as redis
import json

logger = logging.getLogger("AccountingBot")
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

async def cache_get(key):
    try:
        data = await redis_client.get(key)
        if data is not None:
            return json.loads(data)
        return None
    except Exception as e:
        logger.error(f"Ошибка чтения из Redis: {str(e)}")
        return None

async def cache_set(key, value, expire=None):
    try:
        await redis_client.set(key, json.dumps(value))
        if expire:
            await redis_client.expire(key, expire)
        return True
    except Exception as e:
        logger.error(f"Ошибка записи в Redis: {str(e)}")
        return False

async def parse_qr_from_photo(bot, file_id):
    file = await bot.get_file(file_id)
    file_path = file.file_path
    photo = await bot.download_file(file_path)
    
    async with aiohttp.ClientSession() as session:
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
                            total_sum = item.get("sum", 0) / 100        # общая сумма (рубли)
                            unit_price = item.get("price", 0) / 100     # цена за единицу (рубли)
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

                        total_sum = data_json.get("totalSum", 0) / 100
                        return {
                            "fiscal_doc": data_json.get("fiscalDocumentNumber", "unknown"),
                            "date": data_json.get("dateTime", "").split("T")[0].replace("-", "."),
                            "store": data_json.get("retailPlace", "Неизвестно"),
                            "items": filtered_items,
                            "qr_string": result.get("request", {}).get("qrraw", ""),
                            "operation_type": data_json.get("operationType", 1),
                            "prepaid_sum": data_json.get("prepaidSum", 0) / 100,
                            "total_sum": total_sum,
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


async def confirm_manual_api(data, user):
    logger.info(f"confirm_manual_api: Входные данные data={data}")
    sum_value = data.get("sum") or data.get("s", 0.0)
    op_type_str = data.get("op_type", "приход")
    op_type_num = OP_TYPE_MAPPING.get(op_type_str.lower(), 1)
    dt = datetime.strptime(f"{data['date']} {data['time']}", "%d.%m.%Y %H:%M:%S")
    qr_string = f"t={dt.strftime('%Y%m%dT%H%M%S')}&s={sum_value}&fn={data['fn']}&i={data['fd']}&fp={data['fp']}&n={op_type_num}"

    payload = {
        "fn": data["fn"],
        "fd": data["fd"],
        "fp": data["fp"],
        "t": dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "n": op_type_num,
        "s": float(sum_value),
        "qr": 0
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post("https://proverkacheka.com/api/v1/check/get", json=payload) as response:
                result = await response.json()
                if response.status != 200 or result.get("code") != 1:
                    msg = f"Ошибка API: code={result.get('code', 'unknown')}, message={result.get('message', 'Неизвестная ошибка')}"
                    logger.error(msg)
                    parsed_data = {
                        "fiscal_doc": data["fd"],
                        "date": dt.strftime("%d.%m.%Y"),
                        "store": "Неизвестно",
                        "items": [],
                        "qr_string": qr_string,
                        "operation_type": op_type_str,
                        "prepaid_sum": 0.0,
                        "total_sum": float(sum_value),
                        "excluded_sum": 0.0,
                        "excluded_items": []
                    }
                    return False, msg, parsed_data

                data_json = result.get("data", {}).get("json", {})
                items = data_json.get("items", [])
                filtered_items = []
                excluded_sum = 0.0
                for item in items:
                    name = item.get("name", "Неизвестно").strip()
                    price = item.get("sum", 0) / 100
                    if is_excluded(name):
                        logger.info(f"Исключённый товар (manual API): {name} ({price})")
                        excluded_sum += price
                        continue
                    filtered_items.append({
                        "name": name,
                        "sum": price,
                        "quantity": item.get("quantity", 1)
                    })

                total_sum = data_json.get("totalSum", 0) / 100

                parsed_data = {
                    "fiscal_doc": data_json.get("fiscalDocumentNumber", data["fd"]),
                    "date": data_json.get("dateTime", dt.strftime("%d.%m.%Y")).split("T")[0].replace("-", "."),
                    "store": data_json.get("retailPlace", "Неизвестно"),
                    "items": filtered_items,
                    "qr_string": qr_string,
                    "operation_type": op_type_str,
                    "prepaid_sum": data_json.get("prepaidSum", 0) / 100,
                    "total_sum": total_sum,
                    "excluded_sum": excluded_sum,
                    "excluded_items": [item.get("name") for item in items if is_excluded(item.get("name", "").strip())]
                }

                logger.info(f"✅ Чек подтверждён: FD={parsed_data['fiscal_doc']}, qr_string={parsed_data['qr_string']}, user={user.id}")
                return True, f"✅ Чек подтверждён!\nФД: {parsed_data['fiscal_doc']}\nСумма: {parsed_data['total_sum']} руб.", parsed_data

    except Exception as e:
        logger.exception(f"Ошибка confirm_manual_api: {e}")
        parsed_data = {
            "fiscal_doc": data["fd"],
            "date": dt.strftime("%d.%m.%Y"),
            "store": "Неизвестно",
            "items": [],
            "qr_string": qr_string,
            "operation_type": op_type_str,
            "prepaid_sum": 0.0,
            "total_sum": float(sum_value),
            "excluded_sum": 0.0,
            "excluded_items": []
        }
        return False, f"❌ Ошибка при обращении к API: {e}", parsed_data

OP_TYPE_MAPPING = {
    "приход": 1,
    "возврат прихода": 2,
    "расход": 3,
    "возврат расхода": 4
}

def safe_float(value: str | float | int, default: float = 0.0) -> float:
    """
    Безопасное преобразование строки/числа в float
    Заменяет запятые на точки, отсекает пробелы
    """
    try:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            return float(value.replace(",", ".").strip())
    except Exception:
        return default
    return default