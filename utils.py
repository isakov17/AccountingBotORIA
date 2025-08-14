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