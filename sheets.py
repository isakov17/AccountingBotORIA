from googleapiclient.discovery import build
from google.oauth2 import service_account
import logging
import asyncio
from config import SHEET_NAME, GOOGLE_CREDENTIALS
from datetime import datetime
from googleapiclient.errors import HttpError
from utils import redis_client, cache_get, cache_set, safe_float, normalize_date

logger = logging.getLogger("AccountingBot")

# Инициализация Google Sheets API
creds = service_account.Credentials.from_service_account_info(
    GOOGLE_CREDENTIALS, scopes=['https://www.googleapis.com/auth/spreadsheets']
)
sheets_service = build('sheets', 'v4', credentials=creds)

async def async_sheets_call(method_callable, *args, **kwargs):
    """
    Async wrapper для Google API calls.
    Фикс: Вызывает .execute() внутри executor.
    """
    loop = asyncio.get_event_loop()
    def make_call():
        request = method_callable(*args, **kwargs)
        return request.execute()
    try:
        result = await loop.run_in_executor(None, make_call)
        return result
    except Exception as e:
        logger.error(f"Async sheets call error: {str(e)}")
        raise

async def is_user_allowed(user_id: int) -> str | None:
    cache_key = f"user_allowed:{user_id}"
    cached = await cache_get(cache_key)
    if cached is not None:
        logger.info(f"Cache hit for user_id={user_id}")
        return cached

    list_key = "allowed_users_list"
    allowed_list = await cache_get(list_key)
    if allowed_list is None:
        try:
            result = await async_sheets_call(
                sheets_service.spreadsheets().values().get,
                spreadsheetId=SHEET_NAME, range="AllowedUsers!A:B"
            )
            rows = result.get("values", [])[1:]
            allowed_list = [(int(row[0]), row[1] if len(row) > 1 else f"User_{row[0]}") for row in rows if len(row) > 0 and row[0].isdigit()]
            await cache_set(list_key, allowed_list, expire=300)
            logger.info(f"Allowed users list cached: {len(allowed_list)} users")
        except Exception as e:
            logger.error(f"Error loading allowed users: {str(e)}")
            allowed_list = []

    for uid, user_name in allowed_list:
        if uid == user_id:
            await cache_set(cache_key, user_name, expire=86400)
            logger.info(f"User allowed: user_id={user_id}, name={user_name}")
            return user_name

    await cache_set(cache_key, None, expire=86400)
    logger.info(f"User not allowed: user_id={user_id}")
    return None

fiscal_cache_key = "fiscal_docs_set"
fiscal_cache_expire = 300

async def is_fiscal_doc_unique(fiscal_doc: str) -> bool:
    cached_docs = await cache_get(fiscal_cache_key)
    if cached_docs is None:
        try:
            result = await async_sheets_call(
                sheets_service.spreadsheets().values().get,
                spreadsheetId=SHEET_NAME, range="Чеки!M:M"
            )
            cached_docs = {str(row[0]).strip() for row in result.get("values", []) if row and row[0]}
            await cache_set(fiscal_cache_key, list(cached_docs), expire=fiscal_cache_expire)
            logger.info(f"Fiscal docs cache updated: {len(cached_docs)} docs")
        except Exception as e:
            logger.error(f"Error caching fiscal docs: {str(e)}")
            cached_docs = set()

    is_unique = str(fiscal_doc).strip() not in cached_docs
    logger.info(f"Unique check fiscal_doc {fiscal_doc}: {'unique' if is_unique else 'exists'}")
    return is_unique

async def update_fiscal_cache(new_doc: str):
    docs = set(await cache_get(fiscal_cache_key) or [])
    docs.add(new_doc)
    await cache_set(fiscal_cache_key, list(docs), expire=fiscal_cache_expire)

async def save_receipt(
    data_or_parsed=None,
    user_name: str = "",
    customer: str | None = None,
    receipt_type: str = "Покупка",
    delivery_date: str | None = None,
    operation_type: int | None = None,
    **kwargs
) -> bool:
    if data_or_parsed is None:
        data_or_parsed = kwargs.get("parsed_data") or kwargs.get("receipt")

    try:
        data = data_or_parsed or {}
        if not isinstance(data, dict) or not data.get("items"):
            logger.error(f"save_receipt: нет товаров для сохранения, user={user_name}")
            return False

        fiscal_doc = data.get("fiscal_doc", "")
        store = data.get("store", "Неизвестно")
        raw_date = data.get("date") or datetime.now().strftime("%Y.%m.%d")
        qr_string = data.get("qr_string", "")
        status = data.get("status", "Доставлено" if receipt_type in ("Покупка", "Полный") else "Ожидает")
        customer = data.get("customer", customer or "Неизвестно")
        delivery_dates = data.get("delivery_dates", [])
        links = data.get("links", []) or []
        comments = data.get("comments", []) or []
        type_for_sheet = data.get("receipt_type", receipt_type)

        date_for_sheet = normalize_date(raw_date)
        added_at = datetime.now().strftime("%d.%m.%Y")

        rows_checks = []
        rows_summary = []

        items = data.get("items", [])

        for i, item in enumerate(items):
            item_name = item.get("name", "Неизвестно")
            item_sum = safe_float(item.get("sum", 0))
            item_qty = float(item.get("quantity", 1)) or 1
            item_price = safe_float(item.get("price", 0)) or (item_sum / item_qty if item_qty else 0)

            item_link = (links[i] if i < len(links) else "") or item.get("link", "")
            item_comment = (comments[i] if i < len(comments) else "") or item.get("comment", "")

            logger.info(f"save_receipt: item[{i}] name={item_name!r} link={item_link!r} comment={item_comment!r}")
            
            row = [
                added_at,  # A
                date_for_sheet,  # B
                item_sum,  # C
                item_price,  # D
                item_qty,  # E
                user_name,  # F
                store,  # G
                delivery_dates[i] if i < len(delivery_dates) else "",  # H
                status,  # I
                customer,  # J
                item_name,  # K
                type_for_sheet,  # L
                str(fiscal_doc),  # M
                qr_string,  # N
                "",  # O
                item_link,  # P
                item_comment  # Q
            ]
            rows_checks.append(row)

            rows_summary.append([
                date_for_sheet,
                "Покупка" if type_for_sheet in ("Покупка", "Полный") else type_for_sheet,
                "",  # Доход
                abs(item_sum),  # Расход
                f"{fiscal_doc} - {item_name}"
            ])

        if data.get("excluded_sum", 0) > 0:
            rows_summary.append([
                date_for_sheet,
                "Услуга",
                "", 
                abs(data["excluded_sum"]),
                f"{fiscal_doc} - Исключённые: {', '.join(data.get('excluded_items', []))}"
            ])

        await async_sheets_call(
            sheets_service.spreadsheets().values().append,
            spreadsheetId=SHEET_NAME,
            range="Чеки!A:Q",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows_checks}
        )

        if rows_summary:
            await async_sheets_call(
                sheets_service.spreadsheets().values().append,
                spreadsheetId=SHEET_NAME,
                range="Сводка!A:E",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": rows_summary}
            )

        await update_fiscal_cache(str(fiscal_doc))

        logger.info(f"✅ Чек сохранён: fiscal_doc={fiscal_doc}, позиций={len(rows_checks)}, user={user_name}")
        return True

    except Exception as e:
        logger.error(f"❌ Ошибка сохранения чека: {e}, user={user_name}")
        return False

async def save_receipt_summary(date: str, operation_type: str, sum_value: float, note: str):
    logger.info(f"Сохранение summary: {sum_value}, note: {note}, type: {operation_type}")
    try:
        formatted_date = normalize_date(date)

        result = await async_sheets_call(
            sheets_service.spreadsheets().values().get,
            spreadsheetId=SHEET_NAME, range="Сводка!A:E"
        )
        rows = result.get("values", [])
        
        current_balance = 0.0
        if len(rows) > 1:
            last_row = rows[-1]
            if len(last_row) > 4 and last_row[4]:
                current_balance = safe_float(last_row[4])

        if operation_type == "Возврат":
            adjusted_value = abs(sum_value)
            income = str(adjusted_value)
            expense = ""
        elif operation_type == "Услуга":
            adjusted_value = -abs(sum_value)
            income = ""
            expense = str(abs(sum_value))
        else:
            adjusted_value = -abs(sum_value)
            income = ""
            expense = str(abs(sum_value))

        new_balance = current_balance + adjusted_value

        summary_row = [
            formatted_date,
            operation_type,
            income,
            expense,
            note
        ]

        await async_sheets_call(
            sheets_service.spreadsheets().values().append,
            spreadsheetId=SHEET_NAME,
            range="Сводка!A:E",
            valueInputOption="RAW",
            body={"values": [summary_row]}
        )

        await async_sheets_call(
            sheets_service.spreadsheets().values().update,
            spreadsheetId=SHEET_NAME,
            range="Сводка!I1",
            valueInputOption="RAW",
            body={"values": [[f"{new_balance:.2f}"]]}
        )

        logger.info(f"Summary saved: {summary_row}, new_balance={new_balance:.2f}")

    except HttpError as e:
        logger.error(f"Ошибка записи в Сводка: {e.status_code} - {e.reason}")
        raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка записи в Сводка: {str(e)}")
        raise

def normalize_amount(value: str) -> float:
    if not value:
        return 0.0
    try:
        return safe_float(value.replace(" ", "").replace(",", "."))
    except (ValueError, AttributeError):
        logger.error(f"Некорректное число: {value}")
        return 0.0

async def get_monthly_balance() -> dict:
    try:
        # Расширенный range для capture всех rows (A1:O100) — захватит initial C2, fixed I1/L1/O1, и данные ниже
        result = await async_sheets_call(
            sheets_service.spreadsheets().values().get,
            spreadsheetId=SHEET_NAME, range="Сводка!A1:O100"  # Полный: A-E data + fixed I/L/O
        )
        values = result.get("values", [])
        logger.info(f"Balance values raw (first 3 rows): {values[:3] if values else 'EMPTY'}")  # Debug на INFO

        # Fallback если пусто
        if not values:
            logger.warning("No values in Сводка!A1:O100 — using defaults")
            return {"spent": 0.0, "returned": 0.0, "balance": 0.0, "initial_balance": 0.0}

        # Initial balance: C2 (row1=header, row2=data; col C=index2 in full row, but wait — range A1:O → col A=0, B=1, C=2
        # Правильные индексы для A1:O (A=0, B=1, C=2, D=3, E=4, ..., I=8, L=11, O=14)
        initial_balance_value = values[1][2] if len(values) > 1 and len(values[1]) > 2 else "0"  # C2 (row1 col2)
        initial_balance = normalize_amount(initial_balance_value)

        # Fixed cells in row0 (headers + formulas)
        balance_value = values[0][8] if len(values) > 0 and len(values[0]) > 8 else "0"  # I1 (row0 col8)
        balance = normalize_amount(balance_value.replace("=", "").strip())  # Убираем = из формул

        spent_value = values[0][11] if len(values) > 0 and len(values[0]) > 11 else "0"  # L1 (row0 col11)
        spent = normalize_amount(spent_value.replace("=", "").strip())

        returned_value = values[0][14] if len(values) > 0 and len(values[0]) > 14 else "0"  # O1 (row0 col14)
        returned = normalize_amount(returned_value.replace("=", "").strip())

        # Если fixed пустые — посчитай из данных (fallback: sum расход/приход ниже row2)
        if balance == 0.0 or spent == 0.0:
            total_income = sum(normalize_amount(row[2]) for row in values[2:] if len(row) > 2)  # Приход C (col2)
            total_expense = sum(normalize_amount(row[3]) for row in values[2:] if len(row) > 3)  # Расход D (col3)
            calculated_balance = initial_balance + total_income - total_expense
            calculated_spent = total_expense
            calculated_returned = total_income  # Assuming returns in income

            balance = calculated_balance if balance == 0.0 else balance
            spent = calculated_spent if spent == 0.0 else spent
            returned = calculated_returned if returned == 0.0 else returned

            logger.info(f"Calculated fallback: initial={initial_balance}, income={total_income}, expense={total_expense}, balance={calculated_balance}")

        logger.info(f"Balance fetched: initial={initial_balance}, spent={spent}, returned={returned}, balance={balance}")

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