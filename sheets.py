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
    allowed_users = [860613320, 803109062, 400996041, 1059161513]
    logger.info(f"Проверка пользователя {user_id}. Разрешенные пользователи: {allowed_users}")
    return user_id in allowed_users

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

async def save_receipt(parsed_data, username, user_id, customer, receipt_type="Доставка", delivery_date=None, operation_type=None):
    try:
        fiscal_doc = parsed_data["fiscal_doc"]
        if not await is_fiscal_doc_unique(fiscal_doc):
            logger.info(f"Чек уже существует: fiscal_doc={fiscal_doc}, user_id={user_id}")
            return False

        date_str = parsed_data["date"]
        date = datetime.strptime(date_str, "%Y.%m.%d").strftime("%d.%m.%Y")
        shop = parsed_data.get("store", "Unknown")
        qr_string = parsed_data["qr_string"]

        if operation_type is None:
            operation_type = "Доставка" if receipt_type == "Доставка" else "Покупка"

        items = parsed_data["items"]
        excluded_sum = parsed_data.get("excluded_sum", 0.0)
        excluded_items = parsed_data.get("excluded_items", [])

        total_sum = sum(item["sum"] for item in items) + excluded_sum

        for item in items:
            row = [
                datetime.now().strftime("%d.%m.%Y %H:%M"),
                date,
                str(item["sum"]),
                username,
                shop,
                delivery_date or "",  # ← теперь используется переданная дата
                "Ожидает" if receipt_type == "Доставка" else "Доставлено",  # ← статус зависит от типа
                customer,
                item["name"],
                receipt_type,
                fiscal_doc,
                qr_string,
                ""
            ]
            sheets_service.spreadsheets().values().append(
                spreadsheetId=SHEET_NAME,
                range="Чеки!A:M",
                valueInputOption="RAW",
                body={"values": [row]}
            ).execute()
            logger.info(f"Чек сохранен: fiscal_doc={fiscal_doc}, item={item['name']}, delivery_date={delivery_date}, user_id={user_id}")

        # Запись в сводку
        summary_note = f"{fiscal_doc} - {', '.join(item['name'] for item in items)}"
        if excluded_items:
            summary_note += f" (+ {', '.join(excluded_items)})"

        if receipt_type == "Полный":
            await save_receipt_summary(date, "Покупка", total_sum, f"{summary_note} (Полный расчет)")
        else:
            await save_receipt_summary(date, "Покупка", -total_sum, summary_note)

        return True

    except HttpError as e:
        logger.error(f"Ошибка сохранения чека: {e.status_code} - {e.reason}, user_id={user_id}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка сохранения чека: {str(e)}, user_id={username}")
        return False

async def save_receipt_summary(date, operation_type, sum_value, note):
    try:
        # Получаем текущий баланс
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Сводка!A:E"
        ).execute()
        rows = result.get("values", [])[1:]  # Пропускаем заголовок
        current_balance = 0
        if rows:
            current_balance = float(rows[-1][3]) if len(rows[-1]) > 3 and rows[-1][3] else 0
        new_balance = current_balance + sum_value
        summary_row = [
            date,
            operation_type,
            f"{sum_value:.2f}",
            f"{new_balance:.2f}",
            note
        ]
        sheets_service.spreadsheets().values().append(
            spreadsheetId=SHEET_NAME,
            range="Сводка!A:E",
            valueInputOption="RAW",
            body={"values": [summary_row]}
        ).execute()
        logger.info(f"Запись в Сводка: date={date}, operation_type={operation_type}, sum={sum_value}, balance={new_balance}")
    except HttpError as e:
        logger.error(f"Ошибка записи в Сводка: {e.status_code} - {e.reason}")
        raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка записи в Сводка: {str(e)}")
        raise

async def get_monthly_balance():
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Сводка!A:E"
        ).execute()
        rows = result.get("values", [])[1:]  # Пропускаем заголовок
        total_balance = 0
        monthly_spent = 0
        monthly_returned = 0
        current_month = datetime.now().strftime("%m.%Y")
        for row in rows:
            if len(row) < 3 or not row[2]:  # Пропускаем строки без суммы
                continue
            try:
                amount = float(row[2])
                date = row[0] if len(row) > 0 else ""
                operation_type = row[1] if len(row) > 1 else ""
                # Общий баланс
                if operation_type in ["Покупка", "Начальный баланс"]:
                    total_balance += amount  # Отрицательные для покупок, положительные для начального баланса
                elif operation_type == "Возврат":
                    total_balance += amount  # Положительные для возвратов
                # Траты и возвраты за текущий месяц
                if date and date.endswith(current_month):
                    if operation_type == "Покупка":
                        monthly_spent += abs(amount)  # Абсолютная сумма для трат
                    elif operation_type == "Возврат":
                        monthly_returned += amount
            except ValueError:
                logger.error(f"Некорректная сумма в строке: {row}")
                continue
        return {
            "total_balance": total_balance,
            "monthly_spent": monthly_spent,
            "monthly_returned": monthly_returned
        }
    except HttpError as e:
        logger.error(f"Ошибка получения баланса из Google Sheets: {e.status_code} - {e.reason}")
        return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка получения баланса: {str(e)}")
        return None