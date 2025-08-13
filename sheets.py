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
    """
    Универсальная запись одной позиции чека в лист 'Чеки' + строка в 'Сводка'.
    Поддерживает два способа вызова:
      1) save_receipt(parsed_data=..., username=..., user_id=..., customer=..., receipt_type=..., delivery_date=...)
      2) save_receipt(receipt, username, user_id, receipt_type="Покупка")   # где receipt['items'] = [одна позиция]

    Возвращает: True/False
    """

    # Совместимость со старыми вызовами
    if data_or_parsed is None:
        if "parsed_data" in kwargs:
            data_or_parsed = kwargs["parsed_data"]
        elif "receipt" in kwargs:
            data_or_parsed = kwargs["receipt"]

    try:
        # Определяем, что нам передали: уже «собранный» receipt или «сырой» parsed_data
        is_receipt_like = isinstance(data_or_parsed, dict) and (
            "status" in data_or_parsed or "receipt_type" in data_or_parsed or "customer" in data_or_parsed
        )
        data = data_or_parsed or {}

        # Нормализация полей
        if is_receipt_like:
            # Ожидаем одну позицию в data["items"]
            if not data.get("items"):
                logger.error(f"save_receipt: пустой список items, user_id={user_id}")
                return False
            item = data["items"][0]
            item_name = item["name"]
            item_sum = float(item.get("sum", 0))
            store = data.get("store", "Неизвестно")
            raw_date = data.get("date") or datetime.now().strftime("%Y.%m.%d")
            qr_string = data.get("qr_string", "")
            fiscal_doc = data.get("fiscal_doc", "")
            status = data.get("status", "Доставлено" if receipt_type in ("Покупка", "Полный") else "Ожидает")
            customer = data.get("customer", customer or "Неизвестно")
            # при многотоварной доставке сюда уже попадает индивидуальная дата
            delivery_date_final = data.get("delivery_date", delivery_date or "")
            # «Тип чека» в табличной терминологии
            type_for_sheet = data.get("receipt_type", receipt_type)
        else:
            # parsed_data с proverkacheka
            fiscal_doc = data.get("fiscal_doc", "")
            if not await is_fiscal_doc_unique(fiscal_doc):
                logger.info(f"Чек уже существует: fiscal_doc={fiscal_doc}, user_id={user_id}")
                return False

            raw_date = data.get("date") or datetime.now().strftime("%Y.%m.%d")
            store = data.get("store", "Неизвестно")
            qr_string = data.get("qr_string", "")
            # ожидаем, что вызывающая сторона передаёт одну позицию → прокинем delivery_date параметром
            # и внешним циклом вызывают функцию по каждой позиции
            # но на всякий случай возьмём первую
            items = data.get("items", [])
            if not items:
                logger.error(f"save_receipt: пустой список items (parsed_data), user_id={user_id}")
                return False
            item = items[0]
            item_name = item["name"]
            item_sum = float(item.get("sum", 0))
            status = "Ожидает" if receipt_type in ("Доставка", "Предоплата") else "Доставлено"
            type_for_sheet = "Доставка" if receipt_type in ("Доставка", "Предоплата") else ("Полный" if receipt_type == "Полный" else "Покупка")
            delivery_date_final = delivery_date or ""
            customer = customer or "Неизвестно"

        # Приведение даты чека к dd.mm.yyyy
        def _normalize_date(s: str) -> str:
            s = s.replace("-", ".")
            # Встречаются форматы: YYYY.MM.DD, DD.MM.YYYY
            try:
                if s.count(".") == 2:
                    parts = s.split(".")
                    if len(parts[0]) == 4:  # YYYY.MM.DD
                        return datetime.strptime(s, "%Y.%m.%d").strftime("%d.%m.%Y")
                    else:                   # DD.MM.YYYY
                        return datetime.strptime(s, "%d.%m.%Y").strftime("%d.%m.%Y")
            except Exception:
                pass
            # fallback: сегодня
            return datetime.now().strftime("%d.%m.%Y")

        date_for_sheet = _normalize_date(raw_date)
        added_at = datetime.now().strftime("%d.%m.%Y")

        # Формирование строки для листа "Чеки"
        # Колонки: A..M
        row = [
            added_at,                 # A: Дата добавления
            date_for_sheet,           # B: Дата чека
            f"{item_sum:.2f}",        # C: Сумма
            str(username or user_id), # D: Пользователь (логин или id)
            store,                    # E: Магазин
            delivery_date_final or "",# F: Дата доставки
            status,                   # G: Статус
            customer,                 # H: Заказчик
            item_name,                # I: Товар
            type_for_sheet,           # J: Тип чека (Доставка/Покупка/Полный)
            str(fiscal_doc),          # K: Фискальный номер
            qr_string,                # L: QR-строка
            ""                        # M: QR-строка возврата (позже)
        ]

        # Пишем в "Чеки"
        sheets_service.spreadsheets().values().append(
            spreadsheetId=SHEET_NAME,
            range="Чеки!A:M",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]},
        ).execute()

        # Сводка: «Покупка» → отрицательная сумма
        from math import copysign
        sum_for_summary = -abs(item_sum)   # покупка/предоплата всегда минус
        await save_receipt_summary(
            date=date_for_sheet,
            operation_type="Покупка",
            sum_value=sum_for_summary,
            note=f"{fiscal_doc} - {item_name}"
        )

        logger.info(
            "Чек сохранен: fiscal_doc=%s, item=%s, delivery_date=%s, user_id=%s",
            fiscal_doc, item_name, delivery_date_final or "", user_id
        )
        return True

    except Exception as e:
        logger.error(f"Неожиданная ошибка сохранения чека: {str(e)}, user_id={user_id}")
        return False


async def save_receipt_summary(date, operation_type, sum_value, note):
    print(f"Сохранение: {sum_value}, note: {note}")
    try:
        # Получаем текущие данные
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Сводка!A:E"
        ).execute()
        rows = result.get("values", [])
        
        # Определяем текущий баланс
        current_balance = 0.0
        if len(rows) > 1:
            last_row = rows[-1]
            # Баланс — в 4-м столбце (D)
            if len(last_row) > 3 and last_row[3]:
                try:
                    current_balance = float(last_row[3])
                except ValueError:
                    pass

        # Определяем, куда писать сумму
        income = ""
        expense = ""
        if sum_value > 0:
            income = f"{sum_value:.2f}"
            expense = ""
        else:
            income = ""
            expense = f"{abs(sum_value):.2f}"  # только положительное число в расход

        new_balance = current_balance + sum_value

        summary_row = [
            date,
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