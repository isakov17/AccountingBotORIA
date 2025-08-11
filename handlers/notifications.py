from googleapiclient.errors import HttpError
from aiogram import Bot
from sheets import sheets_service
from config import SHEET_NAME, YOUR_ADMIN_ID
from datetime import datetime, timedelta
import asyncio
import logging

logger = logging.getLogger("AccountingBot")
notified_items = set()  # Глобальная переменная для отслеживания уведомлений

async def send_notifications(bot: Bot):
    global notified_items
    allowed_users = [int(uid) for row in sheets_service.spreadsheets().values().get(
        spreadsheetId=SHEET_NAME, range="AllowedUsers!A:A"
    ).execute().get("values", [])[1:] for uid in row if uid and uid.isdigit()]
    while True:
        try:
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=SHEET_NAME, range="Чеки!A:M"
            ).execute()
            receipts = result.get("values", [])[1:]
            today = datetime.now().strftime("%d.%m.%Y")
            three_days_ago = (datetime.now() - timedelta(days=3)).strftime("%d.%m.%Y")
            for row_index, row in enumerate(receipts, start=2):
                if len(row) > 6 and row[6].lower() == "ожидает" and len(row) > 5 and row[5]:
                    fiscal_doc = row[10] if len(row) > 10 else ""
                    item_name = row[8] if len(row) > 8 else ""
                    delivery_date = row[5]
                    notification_key = f"{fiscal_doc}_{row_index-2}"
                    if delivery_date in [today, three_days_ago] and notification_key not in notified_items:
                        for user_id in allowed_users:
                            try:
                                await bot.send_message(
                                    user_id,
                                    f"Напоминание: товар {item_name} из чека {fiscal_doc} ожидает доставку {delivery_date}. "
                                    f"Отключить: /disable_notifications {fiscal_doc}_{row_index-2}. "
                                    f"Подтвердить доставку: /expenses"
                                )
                                logger.info(f"Отправлено уведомление: fiscal_doc={fiscal_doc}, item={item_name}, user_id={user_id}")
                            except Exception as e:
                                logger.error(f"Ошибка отправки уведомления пользователю {user_id}: {str(e)}")
                        notified_items.add(notification_key)
        except HttpError as e:
            logger.error(f"Ошибка получения данных из Google Sheets: {e.status_code} - {e.reason}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка отправки уведомлений: {str(e)}")
        await asyncio.sleep(3600)