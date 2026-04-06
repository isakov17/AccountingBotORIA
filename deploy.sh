#!/bin/bash

# --- НАСТРОЙКИ ---
PROJECT_DIR="$HOME/project/AccountingBotORIA"
SERVICE_NAME="accountingbot"
SERVICE_FILE="/etc/systemd/system/$SERVICE_NAME.service"
PYTHON_BIN="$PROJECT_DIR/venv/bin/python"
BRANCH="main"

echo "🚀 Начинаю полную установку/обновление $SERVICE_NAME..."

# 1. Системные зависимости (Python Venv, Pip, Redis, Git)
echo "📦 Проверка системных пакетов..."
sudo apt update
sudo apt install -y python3-venv python3-pip redis-server git btop

# 2. Проверка Redis
echo "🔍 Настройка Redis..."
sudo systemctl enable redis-server
sudo systemctl start redis-server

# 3. Переход в папку проекта
if [ ! -d "$PROJECT_DIR" ]; then
    echo "❌ Ошибка: Директория $PROJECT_DIR не найдена!"
    exit 1
fi
cd "$PROJECT_DIR"

# 4. Обновление кода через Git
echo "📥 Получаю обновления из GitHub..."
git fetch origin
git checkout "$BRANCH"
# Сохраняем локальные конфиги (.env, credentials), если они не в гите
git stash
git pull origin "$BRANCH" --rebase
git stash pop || echo "⚠️ Сташ пуст или нет конфликтов"

# 5. Создание виртуального окружения
if [ ! -d "venv" ]; then
    echo "🛠 Создаю виртуальное окружение..."
    python3 -m venv venv
fi

# 6. Установка библиотек Python
echo "🐍 Установка зависимостей Python..."
./venv/bin/pip install --upgrade pip
if [ -f requirements.txt ]; then
    ./venv/bin/pip install -r requirements.txt
else
    echo "⚠️ Файл requirements.txt не найден! Устанавливаю базу..."
    ./venv/bin/pip install aiogram aiohttp google-api-python-client google-auth-oauthlib apscheduler redis
fi

# 7. Проверка секретных файлов
if [ ! -f ".env" ]; then
    echo "❗ ВНИМАНИЕ: Файл .env не найден! Бот может не запуститься."
fi
if [ ! -f "credentials.json" ]; then
    echo "❗ ВНИМАНИЕ: Файл credentials.json не найден!"
fi

# 8. Автоматическое создание/обновление SYSTEMD сервиса
echo "⚙️ Проверка конфигурации службы..."
cat <<EOF | sudo tee $SERVICE_FILE > /dev/null
[Unit]
Description=Accounting Bot Service
After=network.target redis.service

[Service]
User=root
WorkingDirectory=$PROJECT_DIR
ExecStart=$PYTHON_BIN main.py
Restart=always
EnvironmentFile=$PROJECT_DIR/.env

[Install]
WantedBy=multi-user.target
EOF

# 9. Перезапуск бота
echo "🔄 Перезапуск службы..."
sudo systemctl daemon-reload
sudo systemctl enable $SERVICE_NAME
sudo systemctl restart $SERVICE_NAME

# 10. Финальная проверка
echo "🔍 Статус системы:"
sudo systemctl status redis-server --no-pager | grep "Active"
sudo systemctl status $SERVICE_NAME --no-pager | grep "Active"

echo "✅ Всё готово! Проверь логи: journalctl -u $SERVICE_NAME -f"
