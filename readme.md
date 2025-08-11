
Accounting Bot
Телеграм-бот для учета расходов отдела с использованием корпоративной карты. Интегрируется с Google Sheets и API Proverkacheka для обработки чеков.
Функционал

Добавление чеков (/add, /add_manual): через QR-код или вручную с валидацией и уникальностью fiscal_doc.
Подтверждение доставки (/expenses): для предоплаченных чеков с подтверждением через QR-код полного чека.
Обработка возвратов (/return): обновление статуса чека и сохранение возврата.
Уведомления: автоматические уведомления о доставке товаров сегодня или 3 дня назад.
Отключение уведомлений (/disable_notifications): для конкретных товаров.
Управление пользователями (/add_user, /remove_user): ограничено первым пользователем.
Логирование: все операции и ошибки записываются в logs/bot.log и лист "Receipt Errors".

Требования

Python 3.8+
Библиотеки: aiogram==3.21, google-api-python-client, google-auth, aiohttp, pyqrcode, python-dotenv

Установка

Клонируйте репозиторий:git clone https://github.com/your_repo/accounting_bot.git
cd accounting_bot


Установите зависимости:pip install -r requirements.txt


Настройте Google Sheets:
Создайте Google Sheet с листами: Transactions (A:L - Date, Amount, User ID, Store, JSON items, Status, Customer, JSON items copy, Receipt type, fiscal_doc, Original QR, Return QR), AllowedUsers (A:A - Telegram IDs), Receipt Errors (A:D - fiscal_doc, timestamp, error, response).
Настройте Google API (Sheets и Drive) и получите credentials.json.
Закодируйте credentials.json в base64: base64 credentials.json.


Настройте переменные окружения в .env (см. .env.example).
Запустите бота:python accounting_bot.py



Безопасность

Доступ ограничен списком в AllowedUsers.
Google credentials хранятся в base64 в переменной окружения.
Исключены файлы .env, credentials.json из Git через .gitignore.
Предотвращение множественных экземпляров через /tmp/accounting_bot.lock.

Логирование

Логи в logs/bot.log с ротацией (10MB, 10 резервных копий).
Формат: %(asctime)s - %(name)s - %(levelname)s - %(message)s.

Замечания

Функция распознавания QR-кода (parse_qr_from_photo) является заглушкой и требует интеграции с OCR API.
Убедитесь, что Google Sheet настроен с правильной структурой колонок.










INFO:AccountingBot:Проверка пользователя 1059161513. Разрешенные пользователи: [860613320, 803109062, 400996041, 1059161513]
INFO:AccountingBot:Нет ожидающих чеков: user_id=1059161513
INFO:aiogram.event:Update id=58286906 is handled. Duration 606 ms by bot id=7756177382

если пропустить дату доставки, нельзя подтвердить получение товара.
