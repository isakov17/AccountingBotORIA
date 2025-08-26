from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.errors import HttpError
import logging
from config import SHEET_NAME, GOOGLE_CREDENTIALS
from datetime import datetime
from utils import redis_client, cache_get, cache_set

logger = logging.getLogger("AccountingBot")

# Инициализация Google Sheets API
creds = service_account.Credentials.from_service_account_info(
    GOOGLE_CREDENTIALS, scopes=['https://www.googleapis.com/auth/spreadsheets']
)
sheets_service = build('sheets', 'v4', credentials=creds)

async def is_user_allowed(user_id: int) -> str | None:
    cache_key = f"user_allowed:{user_id}"
    cached = await cache_get(cache_key)
    if cached is not None:
        logger.info(f"Cache hit for user_id={user_id}")
        return cached

    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="AllowedUsers!A:B"
        ).execute()
        rows = result.get("values", [])[1:]  # Пропускаем заголовок
        for row in rows:
            if len(row) > 0 and str(row[0]) == str(user_id):
                user_name = row[1] if len(row) > 1 else f"User_{user_id}"
                await cache_set(cache_key, user_name, expire=86400)  # Кэш на 24 часа
                logger.info(f"User cached: user_id={user_id}, name={user_name}")
                return user_name
        logger.info(f"Пользователь не найден в AllowedUsers: user_id={user_id}")
        await cache_set(cache_key, None, expire=86400)
        return None
    except HttpError as e:
        logger.error(f"Ошибка проверки пользователя: {e.status_code} - {e.reason}, user_id={user_id}")
        return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка проверки пользователя: {str(e)}, user_id={user_id}")
        return None

async def is_fiscal_doc_unique(fiscal_doc):
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Чеки!K:K"
        ).execute()
        fiscal_docs = [str(row[0]).strip() for row in result.get("values", []) if row]
        is_unique = str(fiscal_doc).strip() not in fiscal_docs
        logger.info(f"Проверка уникальности fiscal_doc {fiscal_doc}: {'уникален' if is_unique else 'уже существует'}")
        return is_unique
    except HttpError as e:
        logger.error(f"Ошибка проверки уникальности fiscal_doc {fiscal_doc}: {e.status_code} - {e.reason}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка проверки fiscal_doc {fiscal_doc}: {str(e)}")
        return False

async def save_receipt(
    data_or_parsed=None,
    user_name: str = "",
    customer: str | None = None,
    receipt_type: str = "Покупка",
    delivery_date: str | None = None,
    operation_type: int | None = None,
    **kwargs
):
    if data_or_parsed is None:
        if "parsed_data" in kwargs:
            data_or_parsed = kwargs["parsed_data"]
        elif "receipt" in kwargs:
            data_or_parsed = kwargs["receipt"]

    try:
        is_receipt_like = isinstance(data_or_parsed, dict) and (
            "status" in data_or_parsed
            or "receipt_type" in data_or_parsed
            or "customer" in data_or_parsed
        )
        data = data_or_parsed or {}

        # Получаем текущую длину таблицы для определения row_index
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Чеки!A:A"
        ).execute()
        row_index = len(result.get("values", []))  # Индекс новой строки (1-based)

        fiscal_doc = data.get("fiscal_doc", "") if data_or_parsed else ""
        store = data.get("store", "Неизвестно")
        amount = data.get("total_sum", "") if data_or_parsed else ""
        purchase_date = data.get("date", "") if data_or_parsed else datetime.now().strftime("%d.%m.%Y")
        qr_string = data.get("qr_string", "") if data_or_parsed else ""
        items = data.get("items", []) if data_or_parsed else []
        status = "Ожидает" if delivery_date else "Доставлено"

        for item in items:
            row_data = [
                datetime.now().strftime("%d.%m.%Y"),  # Дата добавления
                purchase_date,                        # Дата покупки
                str(item.get("sum", "")),             # Сумма
                user_name,                            # Имя пользователя
                store,                                # Магазин
                delivery_date or "",                  # Дата доставки
                status,                               # Статус
                customer or "",                       # Заказчик
                item.get("name", ""),                 # Товар
                receipt_type,                         # Тип чека
                fiscal_doc,                           # Фискальный номер
                qr_string,                            # QR-строка
                ""                                    # QR-строка возврата
            ]

            sheets_service.spreadsheets().values().append(
                spreadsheetId=SHEET_NAME,
                range="Чеки!A:M",
                valueInputOption="RAW",
                body={"values": [row_data]}
            ).execute()
            logger.info(f"Чек сохранён: fiscal_doc={fiscal_doc}, item={item.get('name', '')}, user_name={user_name}, row_index={row_index}")

        # Запись в Сводка для excluded_sum
        if data.get("excluded_sum", 0) > 0:
            formatted_date = datetime.now().strftime("%d.%m.%Y")
            operation_type = "Услуга"
            sum_value = data.get("excluded_sum", 0)
            note = ", ".join(data.get("excluded_items", [])) or "Исключённые товары"

            summary_row = [
                formatted_date,
                operation_type,
                "",  # income
                str(sum_value),  # expense
                note
            ]

            sheets_service.spreadsheets().values().append(
                spreadsheetId=SHEET_NAME,
                range="Сводка!A:E",
                valueInputOption="RAW",
                body={"values": [summary_row]}
            ).execute()
            logger.info(f"Запись в Сводка: {summary_row}")

        # Инвалидация кэша баланса и уведомлений
        await redis_client.delete("balance_data")
        logger.info("Кэш баланса инвалидирован: balance_data")
        await redis_client.delete("notifications:pending_checks")
        logger.info(f"Кэш уведомлений инвалидирован: fiscal_doc={fiscal_doc}")

        return True
    except HttpError as e:
        logger.error(f"Ошибка сохранения чека: {e.status_code} - {e.reason}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка сохранения чека: {str(e)}")
        return False

def normalize_amount(value: str) -> float:
    if not value:
        return 0.0
    try:
        return float(value.replace(" ", "").replace(",", "."))
    except (ValueError, AttributeError):
        logger.error(f"Некорректное число: {value}")
        return 0.0

async def get_monthly_balance():
    cache_key = "balance_data"
    cached = await cache_get(cache_key)
    if cached is not None:
        logger.info("Cache hit for balance_data")
        return cached

    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Сводка!C1:O2"
        ).execute()
        values = result.get("values", [])

        initial_balance_value = values[1][0] if len(values) > 1 and len(values[1]) > 0 else "0"
        initial_balance = normalize_amount(initial_balance_value)

        balance_value = values[0][6] if len(values) > 0 and len(values[0]) > 6 else "0"
        balance = normalize_amount(balance_value)

        spent_value = values[0][9] if len(values) > 0 and len(values[0]) > 9 else "0"
        spent = normalize_amount(spent_value)

        returned_value = values[0][12] if len(values) > 0 and len(values[0]) > 12 else "0"
        returned = normalize_amount(returned_value)

        balance_data = {
            "spent": round(spent, 2),
            "returned": round(returned, 2),
            "balance": round(balance, 2),
            "initial_balance": round(initial_balance, 2),
        }

        await cache_set(cache_key, balance_data, expire=10800)  # Кэш на 3 часа
        logger.info("Balance data cached")
        return balance_data

    except HttpError as e:
        logger.error(f"Ошибка получения баланса: {e.status_code} - {e.reason}")
        return {"spent": 0.0, "returned": 0.0, "balance": 0.0, "initial_balance": 0.0}
    except Exception as e:
        logger.error(f"Неожиданная ошибка получения баланса: {str(e)}")
        return {"spent": 0.0, "returned": 0.0, "balance": 0.0, "initial_balance": 0.0}