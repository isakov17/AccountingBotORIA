#!/bin/bash

   PROJECT_DIR="/home/khadas/project/AccountingBotORIA"
   VENV_PYTHON="$PROJECT_DIR/venv/bin/python"
   SERVICE_NAME="accountingbot"

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

   # 3. Проверяем и добавляем redis и apscheduler в requirements.txt
   echo "🔍 Проверяю requirements.txt..."
   if ! grep -q "redis==4.5.4" requirements.txt; then
       echo "redis==4.5.4" >> requirements.txt
       echo "Добавлен redis==4.5.4 в requirements.txt"
   fi
   if ! grep -q "apscheduler==3.10.4" requirements.txt; then
       echo "apscheduler==3.10.4" >> requirements.txt
       echo "Добавлен apscheduler==3.10.4 в requirements.txt"
   fi

   # 4. Получаем последнюю версию кода
   echo "📥 Получаю обновления из GitHub..."
   git reset --hard
   git pull origin main

   # 5. Активируем виртуальное окружение и ставим зависимости
   echo "📦 Устанавливаю зависимости..."
   source "$PROJECT_DIR/venv/bin/activate"
   pip install --upgrade pip
   pip install -r requirements.txt
   # Проверяем установку redis
   if ! pip show redis > /dev/null; then
       echo "❌ Не удалось установить redis, пытаюсь снова..."
       pip install redis==4.5.4
       if ! pip show redis > /dev/null; then
           echo "❌ Ошибка установки redis, проверьте pip и requirements.txt"
           exit 1
       fi
   fi
   # Проверяем установку apscheduler
   if ! pip show apscheduler > /dev/null; then
       echo "❌ Не удалось установить apscheduler, пытаюсь снова..."
       pip install apscheduler==3.10.4
       if ! pip show apscheduler > /dev/null; then
           echo "❌ Ошибка установки apscheduler, проверьте pip и requirements.txt"
           exit 1
       fi
   fi

   # 6. Перезапускаем сервис бота
   echo "🔄 Перезапускаю сервис бота..."
   sudo systemctl restart "$SERVICE_NAME"

   # 7. Проверяем статус
   echo "🔍 Проверяю статус сервисов..."
   sudo systemctl status redis --no-pager
   sudo systemctl status "$SERVICE_NAME" --no-pager

   echo "✅ Обновление завершено!"