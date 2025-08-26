import os
import json
from dotenv import load_dotenv
import logging

logger = logging.getLogger("AccountingBot")

load_dotenv()

TELEGRAM_TOKEN = get_env_var("TELEGRAM_TOKEN")
SHEET_NAME = get_env_var("SHEET_NAME")
SPREADSHEETS_LINK = get_env_var("SPREADSHEETS_LINK")
PROVERKACHEKA_TOKEN = get_env_var("PROVERKACHEKA_TOKEN")
OCR_API_KEY = get_env_var("OCR_API_KEY")
YOUR_ADMIN_ID = get_env_var("YOUR_ADMIN_ID", convert_type=int)
USER_ID_1 = get_env_var("USER_ID_1", convert_type=int)
USER_ID_2 = get_env_var("USER_ID_2", convert_type=int)
USERS = get_env_var("USERS", convert_type=json.loads)
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

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