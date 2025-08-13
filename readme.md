
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











2.при добавлении товаров с разными датами доставки в таблицу записывается только дата для первого введенного товара.
(venv) user@user-B460MDS3H:~/project/test_bot/AccountingBotORIA$ python main.py
INFO:AccountingBot:Запуск уведомлений
INFO:aiogram.dispatcher:Start polling
INFO:aiogram.dispatcher:Run polling for bot @AccountingORIABot id=7756177382 - 'Бухгалтерия ОРИА'
INFO:AccountingBot:Проверка пользователя 1059161513. Разрешенные пользователи: [860613320, 803109062, 400996041, 1059161513]
INFO:AccountingBot:Начало добавления чека по QR: user_id=1059161513
INFO:aiogram.event:Update id=58287297 is handled. Duration 278 ms by bot id=7756177382
INFO:AccountingBot:Загружены исключения: ['Сервисный сбор', 'Обработка заказа в пункте выдачи', 'Доставка', 'Пункт выдачи']
INFO:AccountingBot:Загружены исключения: ['Сервисный сбор', 'Обработка заказа в пункте выдачи', 'Доставка', 'Пункт выдачи']
INFO:AccountingBot:Загружены исключения: ['Сервисный сбор', 'Обработка заказа в пункте выдачи', 'Доставка', 'Пункт выдачи']
INFO:AccountingBot:Загружены исключения: ['Сервисный сбор', 'Обработка заказа в пункте выдачи', 'Доставка', 'Пункт выдачи']
INFO:AccountingBot:Загружены исключения: ['Сервисный сбор', 'Обработка заказа в пункте выдачи', 'Доставка', 'Пункт выдачи']
INFO:AccountingBot:Загружены исключения: ['Сервисный сбор', 'Обработка заказа в пункте выдачи', 'Доставка', 'Пункт выдачи']
INFO:AccountingBot:Загружены исключения: ['Сервисный сбор', 'Обработка заказа в пункте выдачи', 'Доставка', 'Пункт выдачи']
INFO:AccountingBot:Проверка уникальности fiscal_doc 199977: уникален (найдено 1 записей, список: ['Фискальный номер'])
INFO:AccountingBot:QR-код обработан: fiscal_doc=199977, user_id=1059161513
INFO:aiogram.event:Update id=58287298 is handled. Duration 1588 ms by bot id=7756177382
INFO:AccountingBot:Заказчик принят: герман, user_id=1059161513
INFO:aiogram.event:Update id=58287299 is handled. Duration 168 ms by bot id=7756177382
INFO:aiogram.event:Update id=58287300 is handled. Duration 173 ms by bot id=7756177382
INFO:aiogram.event:Update id=58287301 is handled. Duration 148 ms by bot id=7756177382
INFO:aiogram.event:Update id=58287302 is handled. Duration 98 ms by bot id=7756177382
INFO:aiogram.event:Update id=58287303 is handled. Duration 229 ms by bot id=7756177382
INFO:AccountingBot:Проверка уникальности fiscal_doc 199977: уникален (найдено 1 записей, список: ['Фискальный номер'])
INFO:AccountingBot:Чек сохранен: fiscal_doc=199977, item=Доплеровский радар RCWL-0516, delivery_date=, user_id=1059161513
INFO:AccountingBot:Чек сохранен: fiscal_doc=199977, item=24G HLK-LD2450 Модуль радарного датчика для отслеживания движения человека в миллиметровом диапазоне LD2450 Расстояние срабатыва, delivery_date=, user_id=1059161513
INFO:AccountingBot:Чек сохранен: fiscal_doc=199977, item=LD2410B Радарный модуль интеллектуального обнаружения присутствия человека, 24G, delivery_date=, user_id=1059161513
INFO:AccountingBot:Запись в Сводка: date=12.05.2025, operation_type=Покупка, sum=-961.0, balance=15377.0
INFO:AccountingBot:Проверка уникальности fiscal_doc 199977: уже существует (найдено 4 записей, список: ['Фискальный номер', '199977', '199977', '199977'])
INFO:AccountingBot:Чек уже существует: fiscal_doc=199977, user_id=1059161513
INFO:AccountingBot:Проверка уникальности fiscal_doc 199977: уже существует (найдено 4 записей, список: ['Фискальный номер', '199977', '199977', '199977'])
INFO:AccountingBot:Чек уже существует: fiscal_doc=199977, user_id=1059161513
INFO:AccountingBot:Чек подтвержден: fiscal_doc=199977, user_id=1059161513
INFO:aiogram.event:Update id=58287304 is handled. Duration 7504 ms by bot id=7756177382



3.при возврате можно скинуть любой чек с возвратом. он все равно подвтердит возврат. нужна проверка по названию товара. 

как это реализовано при подтверждении товара.

NFO:AccountingBot:Список ожидающих чеков выведен: user_id=1059161513
INFO:aiogram.event:Update id=58287316 is handled. Duration 571 ms by bot id=7756177382
INFO:AccountingBot:Запрос QR-кода полного расчета: fiscal_doc=199977, item=Доплеровский радар RCWL-0516, user_id=1059161513
INFO:aiogram.event:Update id=58287317 is handled. Duration 177 ms by bot id=7756177382
INFO:AccountingBot:Загружены исключения: ['Сервисный сбор', 'Обработка заказа в пункте выдачи', 'Доставка', 'Пункт выдачи']
INFO:AccountingBot:Загружены исключения: ['Сервисный сбор', 'Обработка заказа в пункте выдачи', 'Доставка', 'Пункт выдачи']
INFO:AccountingBot:Загружены исключения: ['Сервисный сбор', 'Обработка заказа в пункте выдачи', 'Доставка', 'Пункт выдачи']
INFO:AccountingBot:Проверка уникальности fiscal_doc 34450: уникален (найдено 4 записей, список: ['Фискальный номер', '199977', '103185', '199977'])
INFO:AccountingBot:Товар не найден в чеке полного расчета: item=Доплеровский радар RCWL-0516, fiscal_doc=34450, user_id=1059161513
INFO:aiogram.event:Update id=58287318 is handled. Duration 1532 ms by bot id=7756177382
INFO:AccountingBot:Состояние сброшено: user_id=1059161513
INFO:aiogram.event:Update id=58287319 is handled. Duration 129 ms by bot id=7756177382
INFO:AccountingBot:Проверка пользователя 1059161513. Разрешенные пользователи: [860613320, 803109062, 400996041, 1059161513]
INFO:AccountingBot:Команда /start выполнена: user_id=1059161513
INFO:aiogram.event:Update id=58287320 is handled. Duration 93 ms by bot id=7756177382
INFO:AccountingBot:Проверка пользователя 1059161513. Разрешенные пользователи: [860613320, 803109062, 400996041, 1059161513]
INFO:AccountingBot:Список ожидающих чеков выведен: user_id=1059161513
INFO:aiogram.event:Update id=58287321 is handled. Duration 1185 ms by bot id=7756177382
INFO:AccountingBot:Запрос QR-кода полного расчета: fiscal_doc=199977, item=LD2410B Радарный модуль интеллектуального обнаружения присутствия человека, 24G, user_id=1059161513
INFO:aiogram.event:Update id=58287322 is handled. Duration 192 ms by bot id=7756177382
INFO:AccountingBot:Загружены исключения: ['Сервисный сбор', 'Обработка заказа в пункте выдачи', 'Доставка', 'Пункт выдачи']
INFO:AccountingBot:Загружены исключения: ['Сервисный сбор', 'Обработка заказа в пункте выдачи', 'Доставка', 'Пункт выдачи']
INFO:AccountingBot:Загружены исключения: ['Сервисный сбор', 'Обработка заказа в пункте выдачи', 'Доставка', 'Пункт выдачи']
INFO:AccountingBot:Проверка уникальности fiscal_doc 34450: уникален (найдено 4 записей, список: ['Фискальный номер', '199977', '103185', '199977'])
INFO:AccountingBot:Доставка подтверждена: old_fiscal_doc=199977, new_fiscal_doc=34450, item=LD2410B Радарный модуль интеллектуального обнаружения присутствия человека, 24G, user_id=1059161513
INFO:aiogram.event:Update id=58287323 is handled. Duration 1562 ms by bot id=7756177382

тут видно что в первый раз он отклонил, так как товары не совпадают.



sum_value
note
print(sum_value, note)




INFO:AccountingBot:Начало добавления чека по QR: user_id=1059161513
INFO:aiogram.event:Update id=58287499 is handled. Duration 280 ms by bot id=7756177382
INFO:AccountingBot:Загружены исключения: ['Сервисный сбор', 'Обработка заказа в пункте выдачи', 'Доставка', 'Пункт выдачи']
INFO:AccountingBot:Загружены исключения: ['Сервисный сбор', 'Обработка заказа в пункте выдачи', 'Доставка', 'Пункт выдачи']
INFO:AccountingBot:Загружены исключения: ['Сервисный сбор', 'Обработка заказа в пункте выдачи', 'Доставка', 'Пункт выдачи']
INFO:AccountingBot:Загружены исключения: ['Сервисный сбор', 'Обработка заказа в пункте выдачи', 'Доставка', 'Пункт выдачи']
INFO:AccountingBot:Найден исключённый товар: 'Обработка заказа в пункте выдачи' (цена: 75.1)
INFO:AccountingBot:Загружены исключения: ['Сервисный сбор', 'Обработка заказа в пункте выдачи', 'Доставка', 'Пункт выдачи']
INFO:AccountingBot:Загружены исключения: ['Сервисный сбор', 'Обработка заказа в пункте выдачи', 'Доставка', 'Пункт выдачи']
INFO:AccountingBot:Загружены исключения: ['Сервисный сбор', 'Обработка заказа в пункте выдачи', 'Доставка', 'Пункт выдачи']
INFO:AccountingBot:Проверка уникальности fiscal_doc 168756: уникален (найдено 6 записей, список: ['Фискальный номер', '199977', '103185', '34450', '30683', '6236'])
INFO:AccountingBot:QR-код обработан: fiscal_doc=168756, user_id=1059161513
INFO:aiogram.event:Update id=58287500 is handled. Duration 1075 ms by bot id=7756177382
INFO:AccountingBot:Заказчик принят: Ксения, user_id=1059161513
INFO:aiogram.event:Update id=58287501 is handled. Duration 193 ms by bot id=7756177382
INFO:aiogram.event:Update id=58287502 is handled. Duration 188 ms by bot id=7756177382
INFO:aiogram.event:Update id=58287503 is handled. Duration 111 ms by bot id=7756177382
INFO:aiogram.event:Update id=58287504 is handled. Duration 267 ms by bot id=7756177382
-400.0 note: 168756 - Адаптер переходник с HDMI на VGA + AUX кабель, черный / Конвертер для монитора, проектора, компьютера, ноутбука / Адаптер видеос
INFO:AccountingBot:Запись в Сводка: date=17.06.2025, operation_type=Покупка, sum=-400.0, balance=-4273.0
INFO:AccountingBot:Чек сохранен: fiscal_doc=168756, item=Адаптер переходник с HDMI на VGA + AUX кабель, черный / Конвертер для монитора, проектора, компьютера, ноутбука / Адаптер видеос, delivery_date=13.08.2025, user_id=1059161513
-1101.9 note: 168756 - Конвертер аудио HDMI - HDMI (HDTV) Extractor Audio 4k/2k
INFO:AccountingBot:Запись в Сводка: date=17.06.2025, operation_type=Покупка, sum=-1101.9, balance=-5374.9
INFO:AccountingBot:Чек сохранен: fiscal_doc=168756, item=Конвертер аудио HDMI - HDMI (HDTV) Extractor Audio 4k/2k, delivery_date=15.08.2025, user_id=1059161513
INFO:AccountingBot:Чек подтвержден: fiscal_doc=168756, saved=2, failed=0, user_id=1059161513
INFO:aiogram.event:Update id=58287505 is handled. Duration 2215 ms by bot id=7756177382

INFO:AccountingBot:Найден исключённый товар: 'Обработка заказа в пункте выдачи' (цена: 75.1) этот товар не попадает в сводку. хотя должен.
