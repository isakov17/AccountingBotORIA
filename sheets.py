from googleapiclient.discovery import build
from google.oauth2 import service_account
import logging
from config import SHEET_NAME, GOOGLE_CREDENTIALS, USERS, sheets_service, USERS

from datetime import datetime
from googleapiclient.errors import HttpError
from utils import cache_get, cache_set  # Импорт утилит Redis
import time

logger = logging.getLogger("AccountingBot")

# Инициализация Google Sheets API
creds = service_account.Credentials.from_service_account_info(
    GOOGLE_CREDENTIALS, scopes=['https://www.googleapis.com/auth/spreadsheets']
)
sheets_service = build('sheets', 'v4', credentials=creds)

async def is_user_allowed(user_id: int) -> str | None:
    start_time = time.time()
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
                await cache_set(cache_key, user_name, expire=10000)  # Кэш на 1 час
                return user_name
        await cache_set(cache_key, None, expire=3600)
        logger.info(f"is_user_allowed took {time.time() - start_time:.3f}s")
        logger.info(f"Пользователь не найден в AllowedUsers: user_id={user_id}")
        return None
    except HttpError as e:
        logger.error(f"Google Sheets error: {str(e)}, user_id={user_id}")
        return None
    except Exception as e:
        logger.error(f"Ошибка проверки пользователя: {str(e)}, user_id={user_id}")
        return None

async def is_fiscal_doc_unique(fiscal_doc: str) -> bool:
    cache_key = f"fiscal_doc:{fiscal_doc}"
    cached = await cache_get(cache_key)
    if cached is not None:
        logger.info(f"Cache hit for fiscal_doc={fiscal_doc}")
        return cached

    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Чеки!K:K"
        ).execute()
        fiscal_docs = [str(row[0]).strip() for row in result.get("values", []) if row]
        is_unique = str(fiscal_doc).strip() not in fiscal_docs
        await cache_set(cache_key, is_unique, expire=86400)  # Кэш на 24 часа
        logger.info(
            f"Проверка уникальности fiscal_doc {fiscal_doc}: {'уникален' if is_unique else 'уже существует'} "
            f"(найдено {len(fiscal_docs)} записей)"
        )
        return is_unique
    except HttpError as e:
        logger.error(f"Google Sheets error: {str(e)}, fiscal_doc={fiscal_doc}")
        return False
    except Exception as e:
        logger.error(f"Ошибка проверки уникальности fiscal_doc {fiscal_doc}: {str(e)}")
        return False

# sheets.py — обновлённая функция save_receipt

# sheets.py
from datetime import datetime
import logging

logger = logging.getLogger("AccountingBot")

from googleapiclient.errors import HttpError
import time
from config import sheets_service, SHEET_NAME
from utils import redis_client, cache_get, cache_set
import logging
from notifications import send_notifications, notified_items
from datetime import datetime

logger = logging.getLogger("AccountingBot")

async def is_user_allowed(user_id: int) -> bool:
    start_time = time.time()
    cache_key = f"user_allowed:{user_id}"
    cached = await cache_get(cache_key)
    if cached is not None:
        logger.info(f"Cache hit for user_id={user_id}")
        return cached

    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="AllowedUsers!A:A"
        ).execute()
        allowed_users = [int(row[0]) for row in result.get("values", [])[1:] if row[0].isdigit()]
        is_allowed = user_id in allowed_users
        await cache_set(cache_key, is_allowed, expire=86400)  # Кэш на 24 часа
        logger.info(f"User check: user_id={user_id}, allowed={is_allowed}, time={time.time() - start_time:.2f}s")
        return is_allowed
    except HttpError as e:
        logger.error(f"Ошибка проверки пользователя {user_id}: {e.status_code} - {e.reason}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка проверки пользователя {user_id}: {str(e)}")
        return False

async def invalidate_balance_cache():
    """Инвалидирует кэш баланса."""
    await redis_client.delete("balance_data")
    logger.info("Кэш баланса инвалидирован: balance_data")

async def save_receipt(
    data_or_parsed=None,
    user_name: str = "",
    customer: str | None = None,
    receipt_type: str = "Покупка",
    delivery_date: str | None = None,
    operation_type: int | None = None,
    **kwargs
):
    """Сохраняет чек в Google Sheets и отправляет уведомления."""
    try:
        # Получаем текущую длину таблицы для определения row_index
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Чеки!A:A"
        ).execute()
        row_index = len(result.get("values", []))  # Индекс новой строки (1-based)

        # Извлекаем данные чека
        fiscal_doc = data_or_parsed.get("fiscal_doc", "") if data_or_parsed else ""
        amount = data_or_parsed.get("amount", "") if data_or_parsed else ""
        purchase_date = data_or_parsed.get("date", "") if data_or_parsed else datetime.now().strftime("%d.%m.%Y")
        qr_string = data_or_parsed.get("qr_string", "") if data_or_parsed else ""
        item_name = data_or_parsed.get("item_name", "") if data_or_parsed else ""
        shop = data_or_parsed.get("shop", "") if data_or_parsed else ""
        status = "Ожидает" if delivery_date else "Доставлено"

        # Формируем строку для добавления
        row_data = [
            datetime.now().strftime("%d.%m.%Y"),  # Дата добавления
            purchase_date,                        # Дата покупки
            str(amount),                          # Сумма
            user_name,                            # Имя пользователя
            shop,                                 # Магазин
            delivery_date or "",                  # Дата доставки
            status,                               # Статус
            customer or "",                       # Заказчик
            item_name,                            # Товар
            receipt_type,                         # Тип чека
            fiscal_doc,                           # Фискальный номер
            qr_string,                            # QR-строка
            ""                                    # QR-строка возврата
        ]

        # Добавляем чек в Google Sheets
        sheets_service.spreadsheets().values().append(
            spreadsheetId=SHEET_NAME,
            range="Чеки!A:M",
            valueInputOption="RAW",
            body={"values": [row_data]}
        ).execute()
        logger.info(f"Чек сохранён: fiscal_doc={fiscal_doc}, user_name={user_name}, row_index={row_index}")

        # Инвалидация кэша баланса для всех чеков
        await invalidate_balance_cache()
        # Инвалидация кэша уведомлений и отправка уведомлений для чеков "Ожидает"
        notification_key = f"{fiscal_doc}_{row_index}"
        if status == "Ожидает" and notification_key not in notified_items:
            await redis_client.delete("notifications:pending_checks")
            logger.info(f"Кэш уведомлений инвалидирован: notification_key={notification_key}")
            # Отправляем уведомления в будний день
            if datetime.now().weekday() < 5:
                await send_notifications(kwargs.get("bot"))

        return True
    except HttpError as e:
        logger.error(f"Ошибка сохранения чека: {e.status_code} - {e.reason}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка сохранения чека: {str(e)}")
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
    """Получает баланс и дату обновления из Сводка!A1:O2."""
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_NAME, range="Сводка!A1:O2"
        ).execute()
        values = result.get("values", [])

        update_date = values[0][0] if values and len(values[0]) > 0 else datetime.now().strftime("%d.%m.%Y")
        initial_balance_value = values[1][2] if len(values) > 1 and len(values[1]) > 2 else "0"  # C2
        balance_value = values[0][8] if len(values) > 0 and len(values[0]) > 8 else "0"  # I1
        spent_value = values[0][11] if len(values) > 0 and len(values[0]) > 11 else "0"  # L1
        returned_value = values[0][14] if len(values) > 0 and len(values[0]) > 14 else "0"  # O1

        return {
            "spent": round(normalize_amount(spent_value), 2),
            "returned": round(normalize_amount(returned_value), 2),
            "balance": round(normalize_amount(balance_value), 2),
            "initial_balance": round(normalize_amount(initial_balance_value), 2),
            "update_date": update_date
        }
    except HttpError as e:
        logger.error(f"Ошибка получения баланса: {e.status_code} - {e.reason}")
        return {"spent": 0.0, "returned": 0.0, "balance": 0.0, "initial_balance": 0.0, "update_date": datetime.now().strftime("%d.%m.%Y")}
    except Exception as e:
        logger.error(f"Ошибка получения баланса: {str(e)}")
        return {"spent": 0.0, "returned": 0.0, "balance": 0.0, "initial_balance": 0.0, "update_date": datetime.now().strftime("%d.%m.%Y")}