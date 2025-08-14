from googleapiclient.discovery import build
from google.oauth2 import service_account
import logging
from config import SHEET_NAME, GOOGLE_CREDENTIALS
from datetime import datetime  # Импорт datetime
from googleapiclient.errors import HttpError

logger = logging.getLogger("AccountingBot")

# Инициализация Google Sheets API
creds = service_account.Credentials.from_service_account_info(
    GOOGLE_CREDENTIALS, scopes=['https://www.googleapis.com/auth/spreadsheets']
)
sheets_service = build('sheets', 'v4', credentials=creds)

async def is_user_allowed(user_id):
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="AllowedUsers!A:A"
        ).execute()
        allowed_users = [int(row[0]) for row in result.get("values", [])[1:] if row and row[0].isdigit()]
        logger.info(f"Проверка пользователя {user_id}. Разрешенные пользователи: {allowed_users}")
        return user_id in allowed_users
    except HttpError as e:
        logger.error(f"Ошибка получения списка пользователей: {e.status_code} - {e.reason}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка получения списка пользователей: {str(e)}")
        return False

async def is_fiscal_doc_unique(fiscal_doc):
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Чеки!K:K"  # Диапазон столбца с fiscal_doc
        ).execute()
        fiscal_docs = [str(row[0]).strip() for row in result.get("values", []) if row]  # Преобразуем в строки и убираем пробелы
        is_unique = str(fiscal_doc).strip() not in fiscal_docs
        logger.info(f"Проверка уникальности fiscal_doc {fiscal_doc}: {'уникален' if is_unique else 'уже существует'} (найдено {len(fiscal_docs)} записей, список: {fiscal_docs})")
        return is_unique
    except Exception as e:
        logger.error(f"Ошибка проверки уникальности fiscal_doc {fiscal_doc}: {str(e)}")
        return False

# sheets.py — обновлённая функция save_receipt

# sheets.py
from datetime import datetime
import logging

logger = logging.getLogger("AccountingBot")

async def save_receipt(
    data_or_parsed=None,
    username: str = "",
    user_id: int | str = None,
    customer: str | None = None,
    receipt_type: str = "Покупка",
    delivery_date: str | None = None,
    operation_type: int | None = None,
    **kwargs
):
    """Сохраняет чек в Google Sheets:
    - Исключённые товары не попадают в 'Чеки', но идут в 'Сводка'
    - Остальные товары сохраняются в оба листа
    """

    if data_or_parsed is None:
        if "parsed_data" in kwargs:
            data_or_parsed = kwargs["parsed_data"]
        elif "receipt" in kwargs:
            data_or_parsed = kwargs["receipt"]

    try:
        # Определяем формат входных данных
        is_receipt_like = isinstance(data_or_parsed, dict) and (
            "status" in data_or_parsed
            or "receipt_type" in data_or_parsed
            or "customer" in data_or_parsed
        )
        data = data_or_parsed or {}

        if is_receipt_like:
            if not data.get("items") and data.get("excluded_sum", 0) <= 0:
                logger.error(f"save_receipt: нет товаров и нет исключённой суммы, user_id={user_id}")
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
                    str(username or user_id),
                    store,
                    delivery_date_final or "",
                    status,
                    customer,
                    item_name,
                    type_for_sheet,
                    str(fiscal_doc),
                    qr_string,
                    ""
                ]
                sheets_service.spreadsheets().values().append(
                    spreadsheetId=SHEET_NAME,
                    range="Чеки!A:M",
                    valueInputOption="RAW",
                    insertDataOption="INSERT_ROWS",
                    body={"values": [row]},
                ).execute()

                # Запись в Сводка
                await save_receipt_summary(
                    date=date_for_sheet,
                    operation_type="Покупка",
                    sum_value=-abs(item_sum),
                    note=f"{fiscal_doc} - {item_name}"
                )

            # Если есть исключённые товары — только в Сводка
            if data.get("excluded_sum", 0) > 0:
                await save_receipt_summary(
                    date=date_for_sheet,
                    operation_type="Услуга",
                    sum_value=abs(data["excluded_sum"]),
                    note=f"{fiscal_doc} - Исключённые позиции: {', '.join(data.get('excluded_items', []))}"
                )
                logger.info(
                    f"Исключённые товары записаны в Сводка: сумма={data['excluded_sum']}, "
                    f"позиции={data.get('excluded_items', [])}, user_id={user_id}"
                )

            logger.info(f"Чек подтвержден: fiscal_doc={fiscal_doc}, user_id={user_id}")
            return True

        else:
            logger.error(f"save_receipt: неподдерживаемый формат данных, user_id={user_id}")
            return False

    except Exception as e:
        logger.error(f"Неожиданная ошибка сохранения чека: {str(e)}, user_id={user_id}")
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
            spreadsheetId=SHEET_NAME, range="Сводка!A:F"
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
            new_balance,
            note
        ]

        sheets_service.spreadsheets().values().append(
            spreadsheetId=SHEET_NAME,
            range="Сводка!A:F",
            valueInputOption="RAW",
            body={"values": [summary_row]}
        ).execute()

        logger.info(f"Запись в Сводка: {summary_row}, баланс: {new_balance:.2f}")

    except HttpError as e:
        logger.error(f"Ошибка записи в Сводка: {e.status_code} - {e.reason}")
        raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка записи в Сводка: {str(e)}")
        raise
    
    
async def get_monthly_balance():
    """
    Получает общие траты, возвраты и баланс из листа 'Сводка'.
    """
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Сводка!A:E"
        ).execute()

        rows = result.get("values", [])[1:]  # Пропускаем заголовок

        spent = 0.0
        returned = 0.0
        balance = 0.0

        for row in rows:
            if len(row) < 4:
                continue  # Слишком мало столбцов

            # Приход (столбец C)
            income_str = row[2] if len(row) > 2 else ""
            # Расход (столбец D)
            expense_str = row[3] if len(row) > 3 else ""

            operation_type = row[1] if len(row) > 1 else ""

            # Обрабатываем приход
            if income_str:
                try:
                    income = float(income_str)
                    balance += income
                    if operation_type == "Возврат":
                        returned += income
                except ValueError:
                    logger.error(f"Некорректный приход: {income_str}, строка: {row}")

            # Обрабатываем расход
            if expense_str:
                try:
                    expense = float(expense_str)
                    balance -= expense
                    if operation_type == "Покупка":
                        spent += expense
                except ValueError:
                    logger.error(f"Некорректный расход: {expense_str}, строка: {row}")

        return {
            "spent": round(spent, 2),
            "returned": round(returned, 2),
            "balance": round(balance, 2)
        }

    except HttpError as e:
        logger.error(f"Ошибка получения баланса: {e.status_code} - {e.reason}")
        return {"spent": 0.0, "returned": 0.0, "balance": 0.0}
    except Exception as e:
        logger.error(f"Ошибка получения баланса: {str(e)}")
        return {"spent": 0.0, "returned": 0.0, "balance": 0.0}