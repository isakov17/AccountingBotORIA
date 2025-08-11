# exceptions.py
import json
import os
from typing import List
import logging

logger = logging.getLogger("AccountingBot")

# Путь к файлу с исключениями
EXCEPTIONS_FILE = "excluded_items.json"

# Список по умолчанию
DEFAULT_EXCLUDED_ITEMS = [
    "Сервисный сбор",
    "Обработка заказа в пункте выдачи",
    "Доставка"
]


def load_excluded_items() -> List[str]:
    """Загружает список исключённых товаров из файла или возвращает дефолтный."""
    if os.path.exists(EXCEPTIONS_FILE):
        try:
            with open(EXCEPTIONS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    logger.info(f"Загружены исключения: {data}")
                    return data
                else:
                    logger.warning("Файл исключений содержит некорректные данные, используем дефолтные.")
        except Exception as e:
            logger.error(f"Ошибка загрузки исключений: {e}")
    # Возвращаем дефолтный список
    save_excluded_items(DEFAULT_EXCLUDED_ITEMS)
    return DEFAULT_EXCLUDED_ITEMS.copy()


def save_excluded_items(items: List[str]):
    """Сохраняет список исключённых товаров в файл."""
    try:
        with open(EXCEPTIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        logger.info(f"Сохранены исключения: {items}")
    except Exception as e:
        logger.error(f"Ошибка сохранения исключений: {e}")


def add_excluded_item(item: str) -> bool:
    """Добавляет элемент в список исключений."""
    items = load_excluded_items()
    if item in items:
        return False
    items.append(item)
    save_excluded_items(items)
    return True


def remove_excluded_item(item: str) -> bool:
    """Удаляет элемент из списка исключений."""
    items = load_excluded_items()
    if item not in items:
        return False
    items.remove(item)
    save_excluded_items(items)
    return True


def get_excluded_items() -> List[str]:
    """Возвращает текущий список исключённых товаров."""
    return load_excluded_items()


def is_excluded(item_name: str) -> bool:
    """Проверяет, является ли товар исключённым (полное совпадение)."""
    return item_name.strip() in get_excluded_items()