from aiogram.fsm.state import State, StatesGroup

class AddReceipt(StatesGroup):
    UPLOAD_QR = State()
    CUSTOMER = State()
    DELIVERY_STATUS = State()
    DELIVERY_DATE = State()
    TYPE = State()
    FISCAL_DOC = State()
    DATE = State()
    AMOUNT = State()
    STORE = State()
    ITEMS = State()
    CONFIRM_ACTION = State()
    CANCEL_ACTION = State()

class ConfirmDelivery(StatesGroup):
    SELECT_RECEIPT = State()
    UPLOAD_FULL_QR = State()
    CONFIRM_ACTION = State()

class ReturnReceipt(StatesGroup):
    SELECT_ITEM = State()
    UPLOAD_RETURN_QR = State()
    CONFIRM_ACTION = State()