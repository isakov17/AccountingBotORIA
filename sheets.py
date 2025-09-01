from googleapiclient.discovery import build
from google.oauth2 import service_account
import logging
from config import SHEET_NAME, GOOGLE_CREDENTIALS
from datetime import datetime  # Импорт datetime
from googleapiclient.errors import HttpError
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

# sheets.py — обновлённая функция save_receipt

# sheets.py
from datetime import datetime
import logging

logger = logging.getLogger("AccountingBot")

async def save_receipt(
    data_or_parsed=None,
    user_name: str = "",
    customer: str | None = None,
    receipt_type: str = "Покупка",
    delivery_date: str | None = None,
    operation_type: int | None = None,
    **kwargs
):
    """Сохраняет чек в Google Sheets:
    - Все товары пишутся в 'Чеки'
    - Все товары параллельно пишутся в 'Сводка'
    - Исключённые товары пишутся только в 'Сводка' как 'Услуга'
    """

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

        if is_receipt_like:
            if not data.get("items") and data.get("excluded_sum", 0) <= 0:
                logger.error(f"save_receipt: нет товаров и нет исключённой суммы, user_name={user_name}")
                return False

            fiscal_doc = data.get("fiscal_doc", "")
            store = data.get("store", "Неизвестно")
            raw_date = data.get("date") or datetime.now().strftime("%Y.%m.%d")
            qr_string = data.get("qr_string", "")
            status = data.get("status", "Доставлено" if receipt_type in ("Покупка", "Полный") else "Ожидает")
            customer = data.get("customer", customer or "Неизвестно")
            delivery_date_final = data.get("delivery_date", delivery_date or "")
            type_for_sheet = data.get("receipt_type", receipt_type)

            # Приведение даты
            def _normalize_date(s: str) -> str:
                s = s.replace("-", ".")
                try:
                    if len(s.split(".")) == 3:
                        if len(s.split(".")[0]) == 4:
                            return datetime.strptime(s, "%Y.%m.%d").strftime("%d.%m.%Y")
                        return datetime.strptime(s, "%d.%m.%Y").strftime("%d.%m.%Y")
                except Exception:
                    pass
                return datetime.now().strftime("%d.%m.%Y")

            date_for_sheet = _normalize_date(raw_date)
            added_at = datetime.now().strftime("%d.%m.%Y")

            # Сохраняем обычные товары
            for item in data.get("items", []):
                item_name = item["name"]
                item_sum = float(item.get("sum", 0))
                row = [
                    added_at,
                    date_for_sheet,
                    item_sum,
                    user_name,
                    store,
                    delivery_date_final or "",
                    status,
                    customer,
                    item_name,
                    type_for_sheet,
                    str(fiscal_doc),
                    qr_string,
                    "",
                    data.get("link", "")
                ]
                sheets_service.spreadsheets().values().append(
                    spreadsheetId=SHEET_NAME,
                    range="Чеки!A:N",
                    valueInputOption="RAW",
                    insertDataOption="INSERT_ROWS",
                    body={"values": [row]},
                ).execute()

                # 👇 Дублируем в Сводка
                await save_receipt_summary(
                    date=date_for_sheet,
                    operation_type="Покупка" if type_for_sheet in ("Покупка", "Полный") else type_for_sheet,
                    sum_value=-abs(item_sum),
                    note=f"{fiscal_doc} - {item_name}"
                )

            # Исключённые товары — только в Сводка
            if data.get("excluded_sum", 0) > 0:
                await save_receipt_summary(
                    date=date_for_sheet,
                    operation_type="Услуга",
                    sum_value=-abs(data["excluded_sum"]),
                    note=f"{fiscal_doc} - Исключённые позиции: {', '.join(data.get('excluded_items', []))}"
                )
                logger.info(
                    f"Исключённые товары записаны в Сводка: сумма={data['excluded_sum']}, "
                    f"позиции={data.get('excluded_items', [])}, user_name={user_name}"
                )

            logger.info(f"Чек подтвержден: fiscal_doc={fiscal_doc}, user_name={user_name}")
            return True

        else:
            logger.error(f"save_receipt: неподдерживаемый формат данных, user_name={user_name}")
            return False

    except Exception as e:
        logger.error(f"Неожиданная ошибка сохранения чека: {str(e)}, user_name={user_name}")
        return False




async def save_receipt_summary(date, operation_type, sum_value, note):
    print(f"Сохранение: {sum_value}, note: {note}")
    try:
        # Приведение даты
        def _normalize_date(s: str) -> str:
            s = s.replace("-", ".")
            try:
                if len(s.split(".")) == 3:
                    if len(s.split(".")[0]) == 4:
                        return datetime.strptime(s, "%Y.%m.%d").strftime("%d.%m.%Y")
                    return datetime.strptime(s, "%d.%m.%Y").strftime("%d.%m.%Y")
            except Exception:
                pass
            return datetime.now().strftime("%d.%m.%Y")

        formatted_date = _normalize_date(date)

        # Получаем текущие данные
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Сводка!A:E"
        ).execute()
        rows = result.get("values", [])
        
        # Определяем текущий баланс
        current_balance = 0.0
        if len(rows) > 1:
            last_row = rows[-1]
            if len(last_row) > 4 and last_row[4]:
                try:
                    current_balance = float(last_row[4])
                except ValueError:
                    pass

        # Для услуг делаем sum_value отрицательным (расход)
        if operation_type == "Услуга":
            sum_value = -abs(sum_value)

        # Определяем, куда писать сумму
        income = sum_value if sum_value > 0 else ""
        expense = abs(sum_value) if sum_value < 0 else ""

        new_balance = current_balance + sum_value

        summary_row = [
            formatted_date,
            operation_type,
            income,
            expense,
            note
        ]

        sheets_service.spreadsheets().values().append(
            spreadsheetId=SHEET_NAME,
            range="Сводка!A:E",
            valueInputOption="RAW",
            body={"values": [summary_row]}
        ).execute()

        logger.info(f"Запись в Сводка: {summary_row}, баланс: {new_balance:.2f}")

    except HttpError as e:
        logger.error(f"Ошибка записи в Сводка: {e.status_code} - {e.reason}")
        raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка записи в Сводка: {str(e)}.")
        raise
    
    
def normalize_amount(value: str) -> float:
    """
    Приводит значение из Google Sheets к float (поддержка ',' и пробелов).
    Если значение некорректное — возвращает 0.0.
    """
    if not value:
        return 0.0
    try:
        return float(value.replace(" ", "").replace(",", "."))
    except (ValueError, AttributeError):
        logger.error(f"Некорректное число: {value}")
        return 0.0


async def get_monthly_balance():
    """
    Получает начальный баланс из C2, остаток из I1, потрачено из L1, возвраты из O1.
    """
    try:
        # Получаем данные из диапазона Сводка!C1:O2
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Сводка!C1:O2"
        ).execute()
        values = result.get("values", [])

        # Начальный баланс (C2)
        initial_balance_value = values[1][0] if len(values) > 1 and len(values[1]) > 0 else "0"
        initial_balance = normalize_amount(initial_balance_value)

        # Остаток (I1, индекс 6)
        balance_value = values[0][6] if len(values) > 0 and len(values[0]) > 6 else "0"
        balance = normalize_amount(balance_value)

        # Потрачено (L1, индекс 9)
        spent_value = values[0][9] if len(values) > 0 and len(values[0]) > 9 else "0"
        spent = normalize_amount(spent_value)

        # Возвраты (O1, индекс 12)
        returned_value = values[0][12] if len(values) > 0 and len(values[0]) > 12 else "0"
        returned = normalize_amount(returned_value)

        return {
            "spent": round(spent, 2),
            "returned": round(returned, 2),
            "balance": round(balance, 2),
            "initial_balance": round(initial_balance, 2),
        }

    except HttpError as e:
        logger.error(f"Ошибка получения баланса: {e.status_code} - {e.reason}")
        return {"spent": 0.0, "returned": 0.0, "balance": 0.0, "initial_balance": 0.0}
    except Exception as e:
        logger.error(f"Ошибка получения баланса: {str(e)}")
        return {"spent": 0.0, "returned": 0.0, "balance": 0.0, "initial_balance": 0.0}