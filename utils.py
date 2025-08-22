# utils.py — обновлённая функция parse_qr_from_photo
from exceptions import is_excluded, get_excluded_items
import logging
import aiohttp
from config import PROVERKACHEKA_TOKEN
logger = logging.getLogger("AccountingBot")

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
                        excluded_items = get_excluded_items()  # Загружаем исключения один раз
                        filtered_items = []
                        excluded_sum = 0.0

                        for item in items:
                            name = item.get("name", "Неизвестно").strip()
                            price = item.get("sum", 0) / 100  # в рублях
                            if is_excluded(name):
                                logger.info(f"Найден исключённый товар: '{name}' (цена: {price})")
                                excluded_sum += price
                                continue
                            filtered_items.append({
                                "name": name,
                                "sum": price,
                                "quantity": item.get("quantity", 1)
                            })

                        total_sum = data_json.get("totalSum", 0) / 100
                        # Сохраняем исключённую сумму отдельно
                        return {
                            "fiscal_doc": data_json.get("fiscalDocumentNumber", "unknown"),
                            "date": data_json.get("dateTime", "").split("T")[0].replace("-", "."),
                            "store": data_json.get("retailPlace", "Неизвестно"),
                            "items": filtered_items,
                            "qr_string": result.get("request", {}).get("qrraw", ""),
                            "operation_type": data_json.get("operationType", 1),
                            "prepaid_sum": data_json.get("prepaidSum", 0) / 100,
                            "total_sum": total_sum,  # полная сумма чека
                            "excluded_sum": excluded_sum,  # сумма исключённых позиций
                            "excluded_items": [item.get("name") for item in items if is_excluded(item.get("name", "").strip())]
                        }
                    else:
                        logger.error("Нет данных JSON в ответе от proverkacheka.com")
                        return None
                else:
                    logger.error(f"Ошибка обработки на proverkacheka.com: code={result.get('code')}, message={result.get('data')}")
                    return None
            else:
                logger.error(f"Ошибка отправки на proverkacheka.com: status={response.status}")
                return None

import aiohttp
from datetime import datetime

OP_TYPE_MAPPING = {
    "приход": 1,
    "возврат прихода": 2,
    "расход": 3,
    "возврат расхода": 4
}

async def confirm_manual_api(data, user):
    logger.info(f"confirm_manual_api: Входные данные data={data}")
    sum_value = data.get("sum") or data.get("s", 0.0)
    op_type_str = data.get("op_type", "приход")
    op_type_num = OP_TYPE_MAPPING.get(op_type_str.lower(), 1)

    if not sum_value:
        logger.error(f"Отсутствует поле sum/s в data: {data}")
        return False, "❌ Ошибка: отсутствует сумма чека.", None
    if not op_type_str:
        logger.error(f"Отсутствует поле op_type в data: {data}")
        return False, "❌ Ошибка: отсутствует тип операции.", None

    # Формируем дату и время для t
    try:
        dt = datetime.strptime(data['date'] + data['time'], "%d%m%y%H:%M")
        t = dt.strftime("%Y%m%dT%H%M")
    except ValueError as e:
        logger.error(f"Некорректный формат даты/времени: {data['date']} {data['time']}, ошибка: {e}")
        return False, f"❌ Ошибка: некорректный формат даты/времени ({data['date']} {data['time']}).", None

    # Генерируем qr_string
    qr_string = (
        f"t={t}&s={str(sum_value).replace('.', ',')}&fn={data['fn']}"
        f"&i={data['fd']}&fp={data['fp']}&n={op_type_num}"
    )

    # Формируем payload для API
    payload = {
        "token": PROVERKACHEKA_TOKEN,
        "fn": data["fn"],
        "fd": data["fd"],
        "fp": data["fp"],
        "t": dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "n": op_type_num,
        "s": float(sum_value),
        "qr": 0  # если добавляем вручную
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
                    "qr_string": qr_string,  # Всегда используем сгенерированную qr_string
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





