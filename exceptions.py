# exceptions.py
import json
import os
from typing import List
import logging

logger = logging.getLogger("AccountingBot")

EXCEPTIONS_FILE = "excluded_items.json"
DEFAULT_EXCLUDED_ITEMS = [
    "Сервисный сбор",
    "Обработка заказа в пункте выдачи",
    "Доставка"
]

def load_excluded_items() -> List[str]:
    """Загружает список, case-insensitive для match."""
    if os.path.exists(EXCEPTIONS_FILE):
        try:
            with open(EXCEPTIONS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    logger.info(f"Loaded exclusions: {data}")
                    return [item.lower() for item in data]  # Lower для case-insensitive
                else:
                    logger.warning("Invalid exclusions file, using defaults")
        except Exception as e:
            logger.error(f"Error loading exclusions: {e}")
    save_excluded_items(DEFAULT_EXCLUDED_ITEMS)
    return [item.lower() for item in DEFAULT_EXCLUDED_ITEMS]

def save_excluded_items(items: List[str]):
    """Сохраняет список (original case)."""
    try:
        with open(EXCEPTIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved exclusions: {items}")
    except Exception as e:
        logger.error(f"Error saving exclusions: {e}")

def add_excluded_item(item: str) -> bool:
    items = load_excluded_items()
    lower_item = item.lower().strip()
    if lower_item in items:
        return False
    items.append(lower_item)  # Save lower? No, save original, but load lowers
    save_excluded_items([item] + [orig for orig in items if orig != lower_item])  # Mix, but better save original
    # Actually, save originals, check lower in load
    orig_items = load_excluded_items()  # Reload orig? Simplify: save as is, check lower
    if item.lower() in [i.lower() for i in orig_items]:
        return False
    orig_items.append(item)
    save_excluded_items(orig_items)
    return True

def remove_excluded_item(item: str) -> bool:
    orig_items = load_excluded_items()  # Orig list
    lower_item = item.lower().strip()
    for i, orig in enumerate(orig_items):
        if orig.lower() == lower_item:
            orig_items.pop(i)
            save_excluded_items(orig_items)
            return True
    return False

def get_excluded_items() -> List[str]:
    return load_excluded_items()  # Returns lowered, but for list — orig from file

def is_excluded(item_name: str) -> bool:
    """Case-insensitive match."""
    lowered_excluded = load_excluded_items()  # lowered
    return item_name.lower().strip() in lowered_excluded