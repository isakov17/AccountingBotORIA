import os
import json
from dotenv import load_dotenv
import logging

logger = logging.getLogger("AccountingBot")

load_dotenv()

YOUR_ADMIN_ID = int(os.getenv("YOUR_ADMIN_ID")) if os.getenv("YOUR_ADMIN_ID") else None
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
SHEET_NAME = os.getenv("SHEET_NAME")
PROVERKACHEKA_TOKEN = os.getenv("PROVERKACHEKA_TOKEN")
OCR_API_KEY = os.getenv("OCR_API_KEY")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEETS_LINK= os.getenv("spreadsheet_link")

# Загрузка Google Credentials из файла credentials.json
try:
    with open("credentials.json", "r") as f:
        GOOGLE_CREDENTIALS = json.load(f)
    logger.info("Google Credentials загружены из credentials.json")
except FileNotFoundError:
    logger.error("Файл credentials.json не найден")
    raise SystemExit("Файл credentials.json не найден")
except json.JSONDecodeError:
    logger.error("Некорректный формат credentials.json")
    raise SystemExit("Некорректный формат credentials.json")

# Проверка обязательных переменных
for var, name in [
    (TELEGRAM_TOKEN, "TELEGRAM_TOKEN"),
    (SHEET_NAME, "SHEET_NAME"),
    (PROVERKACHEKA_TOKEN, "PROVERKACHEKA_TOKEN")
]:
    if not var:
        logger.error(f"{name} не задан в .env")
        raise SystemExit(f"{name} не задан в .env")