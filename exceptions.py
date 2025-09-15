import json
import os
from typing import List, Set
import logging

logger = logging.getLogger("AccountingBot")

EXCEPTIONS_FILE = "excluded_items.json"
DEFAULT_EXCLUDED_ITEMS = [
    "Сервисный сбор",
    "Обработка заказа в пункте выдачи",
    "Доставка",
    "Пункт выдачи"  # Added from your log
]

# Global cache: ORIGINALS for display/save, LOWERED_SET for fast case-insensitive match
ORIGINALS: List[str] | None = None
LOWERED_SET: Set[str] | None = None

def _load_excluded_items() -> tuple[List[str], Set[str]]:
    """Internal: Load once from file, return originals + lowered set. Log only first."""
    global ORIGINALS, LOWERED_SET
    if ORIGINALS is not None:
        return ORIGINALS, LOWERED_SET  # Already cached

    if os.path.exists(EXCEPTIONS_FILE):
        try:
            with open(EXCEPTIONS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    ORIGINALS = data
                    LOWERED_SET = {item.lower().strip() for item in data if item.strip()}
                    logger.info(f"Loaded exclusions: {len(ORIGINALS)} items (sample: {ORIGINALS[:2]})")  # FIX: Once, short sample
                    return ORIGINALS, LOWERED_SET
                else:
                    logger.warning("Invalid exclusions file, using defaults")
        except Exception as e:
            logger.error(f"Error loading exclusions: {e}")
    else:
        logger.debug("No exclusions file, using defaults")

    # Fallback: Save defaults
    save_excluded_items(DEFAULT_EXCLUDED_ITEMS)
    ORIGINALS = DEFAULT_EXCLUDED_ITEMS
    LOWERED_SET = {item.lower().strip() for item in ORIGINALS}
    return ORIGINALS, LOWERED_SET

def load_excluded_items() -> List[str]:
    """Public: Returns originals (for display/save). Cached."""
    originals, _ = _load_excluded_items()
    return originals

def get_excluded_items() -> List[str]:
    """Returns originals (for UI/list). Cached."""
    return load_excluded_items()  # Now cached, no log

def save_excluded_items(items: List[str]):
    """Saves originals. Updates cache. Log always (rare)."""
    global ORIGINALS, LOWERED_SET
    try:
        with open(EXCEPTIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        ORIGINALS = items
        LOWERED_SET = {item.lower().strip() for item in items if item.strip()}
        logger.info(f"Saved exclusions: {len(items)} items (sample: {items[:2]})")  # Short
    except Exception as e:
        logger.error(f"Error saving exclusions: {e}")

def add_excluded_item(item: str) -> bool:
    """Adds if not exists (case-insensitive). Saves originals."""
    if not item or not item.strip():
        return False
    item = item.strip()  # Original case

    _, lowered_set = _load_excluded_items()  # Cache safe
    lower_item = item.lower()

    if lower_item in lowered_set:
        logger.debug(f"Excluded item '{item}' already exists (case-insensitive)")
        return False

    # Add original
    ORIGINALS.append(item)  # Assume loaded, append to global
    lowered_set.add(lower_item)
    save_excluded_items(ORIGINALS)  # Save + log
    logger.info(f"Added excluded item: '{item}'")
    return True

def remove_excluded_item(item: str) -> bool:
    """Removes by case-insensitive match. Saves originals."""
    if not item or not item.strip():
        return False
    lower_item = item.lower().strip()

    originals, lowered_set = _load_excluded_items()  # Cache safe
    removed = False
    new_originals = []
    for orig in originals:
        if orig.lower().strip() != lower_item:
            new_originals.append(orig)
        else:
            removed = True
            logger.debug(f"Removing excluded item: '{orig}'")

    if removed:
        ORIGINALS = new_originals
        lowered_set.discard(lower_item)
        save_excluded_items(ORIGINALS)  # Save + log
        logger.info(f"Removed excluded item: '{item}'")
        return True
    logger.debug(f"Excluded item '{item}' not found")
    return False

def is_excluded(item_name: str) -> bool:
    """Case-insensitive match. Cached, no log."""
    if not item_name or not item_name.strip():
        return False
    _, lowered_set = _load_excluded_items()  # Cache safe, no log
    return item_name.lower().strip() in lowered_set