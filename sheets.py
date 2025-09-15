from googleapiclient.discovery import build
from google.oauth2 import service_account
import json
import logging
import asyncio
from config import SHEET_NAME, GOOGLE_CREDENTIALS
from datetime import datetime
from googleapiclient.errors import HttpError
from utils import redis_client, cache_get, cache_set, safe_float, normalize_date

logger = logging.getLogger("AccountingBot")
# NOVOYE: Ключ для кэша баланса и время жизни (TTL)
BALANCE_CACHE_KEY = "monthly_balance"  # Имя ключа в Redis
BALANCE_EXPIRE = 30  # 30 секунд — баланс не меняется часто, но обновляем timely

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

# NOVOYE: Внутренняя функция — проверяет кэш баланса
async def _get_cached_balance() -> dict | None:
    """Получает кэшированный баланс или None, если нет."""
    cached = await cache_get(BALANCE_CACHE_KEY)  # Читаем из Redis по ключу
    if cached:  # Если есть данные
        logger.debug("Balance cache hit")  # Лог: "Кэш попал" (для отладки)
        return json.loads(cached)  # Разбираем JSON-строку обратно в словарь
    return None  # Нет кэша — вернём None

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

async def get_monthly_balance(force_refresh: bool = False, use_computed: bool = False) -> dict:
    """Получает баланс: Кэш или 1 запрос A1:Q2. use_computed=True — fallback расчёт, если mismatch."""
    if not force_refresh:
        cached = await _get_cached_balance()
        if cached:
            logger.info(f"Balance from cache: {cached['balance']:.2f}")
            return cached

    try:
        # 1 запрос на A1:Q2 (I1=8 balance, L1=11 spent, O1=14 returned + C2 initial)
        result = await async_sheets_call(
            sheets_service.spreadsheets().values().get,
            spreadsheetId=SHEET_NAME, range="Сводка!A1:Q2"
        )
        values = result.get("values", [])
        logger.debug("Balance A1:Q2 fetched")

        if len(values) < 2:
            logger.warning("No data in Сводка!A1:Q2 — defaults")
            return {"spent": 0.0, "returned": 0.0, "balance": 0.0, "initial_balance": 0.0}

        row0 = values[0]  # Строка 1: I1=8 (баланс), L1=11 (расходы), O1=14 (возвраты)
        row1 = values[1]  # Строка 2: C2 initial = row1[2]

        # Initial из C2 (row1[2])
        initial_balance = normalize_amount(str(row1[2]) if len(row1) > 2 else "0")

        # Фиксированные из row0 (ТВОИ ИНДЕКСЫ!)
        balance_value = row0[8] if len(row0) > 8 else "0"  # I1=8 (остаток)
        balance = normalize_amount(str(balance_value).replace("=", "").strip())

        spent_value = row0[11] if len(row0) > 11 else "0"  # L1=11 (расходы)
        spent = normalize_amount(str(spent_value).replace("=", "").strip())

        returned_value = row0[14] if len(row0) > 14 else "0"  # O1=14 (возвраты)
        returned = normalize_amount(str(returned_value).replace("=", "").strip())

        # Опциональный fallback: Если use_computed=True, проверяем и считаем
        if use_computed:
            computed_balance = initial_balance + returned - spent
            if abs(balance - computed_balance) > 0.01:
                logger.warning(f"Balance mismatch: formula={balance:.2f} ≠ computed={computed_balance:.2f}; using computed")
                balance = computed_balance
            # Можно добавить spent/returned computed, но по умолчанию — из таблицы

        result_data = {
            "spent": round(spent, 2),
            "returned": round(returned, 2),
            "balance": round(balance, 2),
            "initial_balance": round(initial_balance, 2),
        }
        await cache_set(BALANCE_CACHE_KEY, json.dumps(result_data), expire=BALANCE_EXPIRE)
        logger.info(f"Balance fetched/cached: {result_data['balance']:.2f} (from I1={balance_value}, L1={spent_value}, O1={returned_value})")
        return result_data

    except HttpError as e:
        logger.error(f"Ошибка получения баланса: {e.status_code} - {e.reason}")
        return {"spent": 0.0, "returned": 0.0, "balance": 0.0, "initial_balance": 0.0}
    except Exception as e:
        logger.error(f"Ошибка получения баланса: {str(e)}")
        return {"spent": 0.0, "returned": 0.0, "balance": 0.0, "initial_balance": 0.0}

# NOVOYE: Обновляет кэш баланса (для будущих этапов, после изменений)
async def update_balance_cache(balance_data: dict):
    """Обновляет кэш новыми данными баланса (после add/return)."""
    await cache_set(BALANCE_CACHE_KEY, json.dumps(balance_data), expire=BALANCE_EXPIRE)
    logger.debug("Balance cache updated")  # Лог: "Кэш обновлён"

# NOVOYE: Helper для delta-расчёта баланса (для confirm)
async def compute_delta_balance(operation_type: str, total_sum: float, old_balance_data: dict | None = None) -> dict:
    """
    Вычисляет новый баланс по delta (без API).
    operation_type: 'add' (расход, -sum), 'return' (доход, +sum), 'delivery' (0, no change).
    old_balance_data: Из кэша (если None — get cached).
    Возвращает новый dict для кэша/уведомлений.
    """
    if old_balance_data is None:
        old_balance_data = await _get_cached_balance() or {"balance": 0.0, "spent": 0.0, "returned": 0.0, "initial_balance": 0.0}

    old_balance = old_balance_data["balance"]
    old_spent = old_balance_data["spent"]
    old_returned = old_balance_data["returned"]
    initial = old_balance_data["initial_balance"]

    new_balance = old_balance
    new_spent = old_spent
    new_returned = old_returned

    if operation_type == "add":  # Покупка/расход
        new_balance = old_balance - total_sum
        new_spent = old_spent + total_sum
    elif operation_type == "return":  # Возврат/доход
        new_balance = old_balance + total_sum
        new_returned = old_returned + total_sum
    elif operation_type == "delivery":  # Подтверждение — no change (status only)
        new_balance = old_balance  # Или + доплата, если есть
        # new_spent/returned unchanged
    else:
        logger.warning(f"Unknown operation_type: {operation_type}, no delta")

    # Computed check (safety, optional)
    computed = initial + new_returned - new_spent
    if abs(new_balance - computed) > 0.01:
        logger.debug(f"Delta mismatch: {new_balance:.2f} ≠ computed {computed:.2f}; using delta")
        new_balance = computed

    new_data = {
        "spent": round(new_spent, 2),
        "returned": round(new_returned, 2),
        "balance": round(new_balance, 2),
        "initial_balance": round(initial, 2),
    }
    logger.info(f"Delta computed: op={operation_type}, sum={total_sum:.2f}, old_balance={old_balance:.2f} → new={new_balance:.2f}")
    return new_data

# NOVOYE: Update cache with new data (after delta)
async def update_balance_cache_with_delta(new_balance_data: dict):
    """Обновляет кэш новым балансом после операции."""
    await cache_set(BALANCE_CACHE_KEY, json.dumps(new_balance_data), expire=BALANCE_EXPIRE)
    logger.debug("Balance cache updated with delta")

    # NOVOYE: Batch update для нескольких строк (1 API call вместо N)
async def batch_update_sheets(updates: list):
    """Batch update values в sheets (list of {'range': 'A1:Q1', 'values': [[...]]})."""
    try:
        body = {
            "valueInputOption": "RAW",
            "data": updates  # [{'range': ..., 'values': [[row]]}, ...]
        }
        result = await async_sheets_call(
            sheets_service.spreadsheets().values().batchUpdate,
            spreadsheetId=SHEET_NAME,
            body=body
        )
        logger.debug(f"Batch update: {len(updates)} ranges, updated {result.get('totalUpdatedRows', 0)} rows")
        return True
    except HttpError as e:
        logger.error(f"Batch update error: {e.status_code} - {e.reason}")
        return False
    except Exception as e:
        logger.error(f"Batch update exception: {str(e)}")
        return False