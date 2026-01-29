
# 📊 Accounting Bot ORIA  
> Telegram-бот для учёта расходов по корпоративной карте с автоматической обработкой чеков и интеграцией в Google Sheets.

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python)](https://python.org)
[![aiogram](https://img.shields.io/badge/aiogram-3.21+-teal?logo=telegram)](https://docs.aiogram.dev/)
[![Google Sheets](https://img.shields.io/badge/Google_Sheets-API_v4-green?logo=google-sheets)](https://developers.google.com/sheets)

---

## 🚀 Функционал

- ✅ Добавление чеков:  
  `/add` — по QR-коду (через [Proverkacheka API](https://proverkacheka.com))  
  `/add_manual` — вручную (с валидацией `fiscal_doc`)
- ✅ Подтверждение доставки (`/expenses`) — по QR второго чека с **проверкой наименования товара**
- ✅ Возвраты (`/return`) — только при совпадении `fiscal_doc` **и названия товара**
- ✅ Уведомления: напоминания о поставках (сегодня / 3 дня назад), гибкое отключение
- ✅ Управление пользователями (`/add_user`, `/remove_user`) — только для админа
- ✅ Логирование ошибок в `logs/bot.log` и отдельный лист *Receipt Errors*
- ✅ Интеграция с Google Sheets — автоматическая запись в структурированную таблицу

---

## 🛠 Установка

```bash
git clone https://github.com/isakov17/AccountingBotORIA.git
cd AccountingBotORIA
pip install -r requirements.txt
```

### Настройка

1. Создайте Google-таблицу со следующими листами:
   - `Transactions` (A:M):  
     `Date`, `Amount`, `User ID`, `Store`, `Items JSON`, `Status`, `Customer`, `Items Copy`, `Type`, `fiscal_doc`, `QR Input`, `QR Return`, **`Project`**
   - `AllowedUsers` (A:A): Telegram ID админов
   - `Receipt Errors` (A:D): ошибки парсинга
   - `Summary`: для баланса

2. Включите **Google Sheets API** и **Google Drive API**, получите `credentials.json`

3. Закодируйте его:  
   ```bash
   base64 credentials.json | tr -d '\n' > credentials.b64
   ```

4. Настройте `.env` из `.env.example`:
   ```env
   BOT_TOKEN=XXX
   GOOGLE_CREDENTIALS_BASE64=eyJ0eX...  # содержимое credentials.b64
   SPREADSHEET_ID=1xYz...
   ADMIN_ID=860613320
   # NOTIFICATION_CHAT_ID=-1001234567890  # ← раскомментируйте для групповых уведомлений
   ```

5. Запустите:
   ```bash
   python main.py
   ```


