#!/bin/bash

# ИСПРАВЛЕННЫЙ ПУТЬ
PROJECT_DIR="$HOME/german/AccountingBotORIA"
SERVICE_NAME="accountingbot"
BRANCH="main"

echo "🚀 Обновление AccountingBotORIA в директории $PROJECT_DIR..."

# 0. Проверка Redis
if ! command -v redis-server >/dev/null 2>&1; then
    echo "📦 Установка Redis..."
    sudo apt update && sudo apt install -y redis-server
fi

# 1. Запуск Redis
sudo systemctl start redis
if ! redis-cli ping | grep -q "PONG"; then
    echo "❌ Redis не отвечает."
    exit 1
fi

# 2. Переход в папку
cd "$PROJECT_DIR" || { echo "❌ Директория не найдена"; exit 1; }

# 3. Обновление кода (Hard Reset)
echo "📥 Синхронизация с GitHub (ветка $BRANCH)..."
git fetch origin
git reset --hard origin/"$BRANCH"

# 4. Обновление зависимостей
echo "📦 Обновление библиотек..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
deactivate

# 5. Перезагрузка сервиса
echo "🔄 Перезапуск бота..."
sudo systemctl daemon-reload
sudo systemctl restart "$SERVICE_NAME"

# 6. Проверка
echo "🔍 Статус:"
sudo systemctl status "$SERVICE_NAME" --no-pager | grep "Active:"

echo "✅ Готово!"
