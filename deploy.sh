#!/bin/bash

PROJECT_DIR="/home/khadas/project/AccountingBotORIA"
VENV_PYTHON="$PROJECT_DIR/venv/bin/python"
SERVICE_NAME="accountingbot"
BRANCH="main-notification+cache"

echo "🚀 Обновление бота..."

# 1. Проверка Redis
echo "🔍 Проверяю Redis..."
if ! systemctl is-active --quiet redis; then
    echo "⚠️ Redis не запущен, запускаю..."
    sudo systemctl start redis
fi
if ! redis-cli ping | grep -q "PONG"; then
    echo "❌ Redis не отвечает, проверьте конфигурацию"
    exit 1
fi

# 2. Переходим в директорию проекта
cd "$PROJECT_DIR" || { echo "❌ Не найдена директория $PROJECT_DIR"; exit 1; }

# 3. Сохраняем локальные изменения
echo "🔍 Проверяю локальные изменения..."
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "📝 Сохраняю локальные изменения в stash..."
    git add .
    git stash
fi

# 4. Получаем последнюю версию кода
echo "📥 Получаю обновления из GitHub..."
git fetch origin
git checkout "$BRANCH"
git pull origin "$BRANCH" --rebase
if [ $? -ne 0 ]; then
    echo "❌ Ошибка при git pull, возможны конфликты. Разрешите их вручную."
    exit 1
fi

# 5. Применяем сохранённые изменения
if git stash list | grep -q "stash"; then
    echo "🔄 Применяю сохранённые изменения..."
    git stash pop
    if [ $? -ne 0 ]; then
        echo "❌ Конфликт при применении stash. Разрешите конфликты вручную."
        exit 1
    fi
fi


# 7. Устанавливаем зависимости
echo "📦 Устанавливаю зависимости..."
source "$PROJECT_DIR/venv/bin/activate"
pip install --upgrade pip
pip install -r requirements.txt

# 8. Перезапускаем сервис
echo "🔄 Перезапускаю сервис бота..."
sudo systemctl restart "$SERVICE_NAME"

# 9. Проверяем статус
echo "🔍 Проверяю статус сервисов..."
sudo systemctl status redis --no-pager
sudo systemctl status "$SERVICE_NAME" --no-pager

echo "✅ Обновление завершено!"