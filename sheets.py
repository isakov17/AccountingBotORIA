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
        logger.debug(f"Cache hit for user_id={user_id}")
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
    logger.debug(f"User not allowed: user_id={user_id}")
    return None

async def is_fiscal_doc_unique(fiscal_doc: str) -> bool:
    try:
        result = await async_sheets_call(
            sheets_service.spreadsheets().values().get,
            spreadsheetId=SHEET_NAME, range="Чеки!M:M"
        )
        raw_values = result.get("values", [])
        logger.debug(f"Direct fetch Чеки!M:M: total rows={len(raw_values)}")  # Minimal: no raw

        existing_docs = {
            str(row[0]).strip() 
            for row in raw_values 
            if row and row[0] and str(row[0]).strip().isdigit()
        }
        if existing_docs:
            logger.debug(f"Filtered fiscal docs: {len(existing_docs)} unique")  # Quiet, no sample
        else:
            logger.debug(f"Filtered fiscal docs: 0 unique")

        is_unique = str(fiscal_doc).strip() not in existing_docs
        status = 'unique ✅' if is_unique else 'exists ❌'
        logger.info(f"is_fiscal_doc_unique '{fiscal_doc}': {status}")
        return is_unique

    except Exception as e:
        logger.error(f"Error fetching fiscal docs M:M: {str(e)}")
        logger.warning(f"Fallback: assume unique for '{fiscal_doc}' due to error")
        return True

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
        rows_summary = []  # Keep for data append (formulas sum C/D)

        items = data.get("items", [])
        for i, item in enumerate(items):
            item_name = item.get("name", "Неизвестно")
            item_sum = float(safe_float(item.get("sum", 0)))  # FIX: Pure float for summable
            item_qty = float(item.get("quantity", 1)) or 1.0
            item_price = float(safe_float(item.get("price", 0))) or (item_sum / item_qty if item_qty else 0.0)

            item_link = (links[i] if i < len(links) else "") or item.get("link", "")
            item_comment = (comments[i] if i < len(comments) else "") or item.get("comment", "")

            logger.debug(f"save_receipt: item[{i}] name={item_name}, sum={item_sum}")  # Minimal: no !r, debug

            row = [
                added_at,  # A
                date_for_sheet,  # B
                item_sum,  # C: float (number)
                item_price,  # D: float
                item_qty,  # E: float
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

            # Summary data row (for formulas sum C/D; no fixed)
            rows_summary.append([
                date_for_sheet,
                "Покупка" if type_for_sheet in ("Покупка", "Полный") else type_for_sheet,
                0.0,  # C: float 0 (no income)
                abs(item_sum),  # D: float expense
                f"{fiscal_doc} - {item_name}"
            ])

        excluded_sum = float(safe_float(data.get("excluded_sum", 0)))  # FIX: float
        if excluded_sum > 0:
            rows_summary.append([
                date_for_sheet,
                "Услуга",
                0.0,  # C: 0
                excluded_sum,  # D: float
                f"{fiscal_doc} - Исключённые: {', '.join(data.get('excluded_items', []))}"
            ])

        # Append to Чеки (only)
        await async_sheets_call(
            sheets_service.spreadsheets().values().append,
            spreadsheetId=SHEET_NAME,
            range="Чеки!A:Q",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows_checks}
        )

        # Append summary data (for formulas; no updates)
        if rows_summary:
            await async_sheets_call(
                sheets_service.spreadsheets().values().append,
                spreadsheetId=SHEET_NAME,
                range="Сводка!A:E",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": rows_summary}
            )
            logger.debug(f"Appended {len(rows_summary)} summary rows for formulas")

        # REMOVED: All fixed updates (J1/M1/P1) — formulas handle

        logger.info(f"✅ Чек сохранён: fiscal_doc={fiscal_doc}, позиций={len(rows_checks)}, user={user_name}")
        return True

    except Exception as e:
        logger.error(f"❌ Ошибка сохранения чека: {e}, user={user_name}")
        return False

async def save_receipt_summary(date: str, operation_type: str, sum_value: float, note: str):
    """Append only data row for formulas (no fixed updates)."""
    logger.debug(f"Summary append: {sum_value}, type: {operation_type}")  # Minimal
    try:
        formatted_date = normalize_date(date)
        adjusted_value = float(abs(sum_value))  # FIX: float

        if operation_type == "Возврат":
            income = adjusted_value  # C: float return
            expense = 0.0  # D: 0
        elif operation_type == "Услуга":
            income = 0.0
            expense = adjusted_value
        else:  # Расход
            income = 0.0
            expense = adjusted_value

        # Append data row only (for formulas)
        summary_row = [
            formatted_date,
            operation_type,
            income,  # C: float
            expense,  # D: float
            note
        ]

        await async_sheets_call(
            sheets_service.spreadsheets().values().append,
            spreadsheetId=SHEET_NAME,
            range="Сводка!A:E",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [summary_row]}
        )

        logger.debug(f"Summary row appended: {summary_row[:2]}... (formulas will update)")
        return True  # Success

    except HttpError as e:
        logger.error(f"Ошибка append summary: {e.status_code} - {e.reason}")
        raise
    except Exception as e:
        logger.error(f"Ошибка summary: {str(e)}")
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
        # Fetch only fixed (formulas in J1/M1/P1)
        result = await async_sheets_call(
            sheets_service.spreadsheets().values().get,
            spreadsheetId=SHEET_NAME, range="Сводка!A1:Q1"  # Only row1 (fixed)
        )
        values = result.get("values", [])
        logger.debug(f"Balance fixed row1 fetched")  # Minimal, no raw

        if not values:
            logger.warning("No fixed row in Сводка!A1:Q1 — using defaults")
            return {"spent": 0.0, "returned": 0.0, "balance": 0.0, "initial_balance": 0.0}

        row0 = values[0]

        # Initial from C2 (separate fetch, or assume set manually)
        init_result = await async_sheets_call(
            sheets_service.spreadsheets().values().get,
            spreadsheetId=SHEET_NAME, range="Сводка!C2"
        )
        initial_balance_value = init_result.get("values", [[0]])[0][0]
        initial_balance = normalize_amount(str(initial_balance_value))

        # Fixed from formulas
        balance_value = row0[9] if len(row0) > 9 else "0"  # J1
        balance = normalize_amount(str(balance_value).replace("=", "").strip())

        spent_value = row0[12] if len(row0) > 12 else "0"  # M1
        spent = normalize_amount(str(spent_value).replace("=", "").strip())

        returned_value = row0[15] if len(row0) > 15 else "0"  # P1
        returned = normalize_amount(str(returned_value).replace("=", "").strip())

        # REMOVED: Fallback sum — trust formulas

        # Override if mismatch (safety)
        computed_balance = initial_balance + returned - spent
        if abs(balance - computed_balance) > 0.01:
            logger.warning(f"Balance mismatch: formula={balance:.2f} ≠ computed={computed_balance:.2f}; using computed")
            balance = computed_balance

        logger.info(f"Balance from formulas: initial={initial_balance}, spent={spent}, returned={returned}, balance={balance}")

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