import os
import json
from dotenv import load_dotenv
import logging

logger = logging.getLogger("AccountingBot")

load_dotenv()

YOUR_ADMIN_ID = int(os.getenv("YOUR_ADMIN_ID", 0)) if os.getenv("YOUR_ADMIN_ID") else 0
USER_ID_1 = int(os.getenv("USER_ID_1", 0)) if os.getenv("USER_ID_1") else 0
USER_ID_2 = int(os.getenv("USER_ID_2", 0)) if os.getenv("USER_ID_2") else 0
GROUP_CHAT_ID = int(os.getenv("GROUP_CHAT_ID", 0)) if os.getenv("GROUP_CHAT_ID") else 0
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
SHEET_NAME = os.getenv("SHEET_NAME", "").strip()
PROVERKACHEKA_TOKEN = os.getenv("PROVERKACHEKA_TOKEN", "").strip()
OCR_API_KEY = os.getenv("OCR_API_KEY", "").strip()
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEETS_LINK = os.getenv("SPREADSHEETS_LINK", "https://docs.google.com/spreadsheets/d/example").strip()

# Warnings для optional
if not OCR_API_KEY:
    logger.warning("OCR_API_KEY not set, OCR features disabled")
if GROUP_CHAT_ID == 0:
    logger.warning("GROUP_CHAT_ID not set, group notifications disabled")

# Загрузка Google Credentials
try:
    with open("credentials.json", "r") as f:
        GOOGLE_CREDENTIALS = json.load(f)
    logger.info("Google Credentials loaded")
except FileNotFoundError:
    logger.error("credentials.json not found")
    raise SystemExit("credentials.json not found")
except json.JSONDecodeError:
    logger.error("Invalid credentials.json")
    raise SystemExit("Invalid credentials.json")

# Обязательные checks
required = [
    (TELEGRAM_TOKEN, "TELEGRAM_TOKEN"),
    (SHEET_NAME, "SHEET_NAME"),
    (PROVERKACHEKA_TOKEN, "PROVERKACHEKA_TOKEN"),
    (YOUR_ADMIN_ID > 0, "YOUR_ADMIN_ID"),
    (USER_ID_1 > 0, "USER_ID_1"),
    (USER_ID_2 > 0, "USER_ID_2")
]
for var, name in required:
    if not var:
        logger.error(f"{name} not set in .env")
        raise SystemExit(f"{name} not set in .env")