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
    ENTER_FISCAL_DOC = State()
    SELECT_ITEM = State()
    UPLOAD_RETURN_QR = State()
    CONFIRM_ACTION = State()
    
class StatusMessenger:
    def __init__(self, bot, chat_id):
        self.bot = bot
        self.chat_id = chat_id
        self.message = None

    async def start(self, text: str):
        """Отправляет первое сообщение"""
        self.message = await self.bot.send_message(self.chat_id, text)

    async def update(self, text: str):
        """Обновляет сообщение"""
        if self.message:
            await self.bot.edit_message_text(chat_id=self.chat_id, message_id=self.message.message_id, text=text)

    async def finish(self, text: str):
        """Завершает процесс финальным сообщением"""
        if self.message:
            await self.bot.edit_message_text(chat_id=self.chat_id, message_id=self.message.message_id, text=text)
            self.message = None
