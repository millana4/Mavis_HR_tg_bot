from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton

BTN_SHARE_CONTACT = "☎️ Поделиться контактом"

share_contact_kb = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text=BTN_SHARE_CONTACT, request_contact=True)]],
    resize_keyboard=True,
    one_time_keyboard=True,
)

# Клавиатура для выбора типа поиска в контактах
SEARCH_TYPE_KEYBOARD = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Сотрудники", callback_data="search_by_name")],
    [InlineKeyboardButton(text="Подразделения", callback_data="search_company_group")],
    [InlineKeyboardButton(text="⬅️ Назад", callback_data="back")]
])

# Клавиатура для выбора подразделений
SEARCH_COMPANY_GROUP = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Телефоны отделов «Мавис»", callback_data="search_by_department")],
    [InlineKeyboardButton(text="Магазины «Вотоня»", callback_data="search_shop")],
    [InlineKeyboardButton(text="Аптеки «Имбирь»", callback_data="search_drugstore")],
    [InlineKeyboardButton(text="⬅️ Назад", callback_data="search_back")]
])

# Клавиатура с одной кнопкой Назад -> Возвращает к выбору типа поиска SEARCH_TYPE_KEYBOARD
BACK_TO_SEARCH_TYPE = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="⬅️ Назад", callback_data="search_back")],
])

# Клавиатура с одной кнопкой Назад -> Возвращает к выбору типа поиска SEARCH_TYPE_KEYBOARD
BACK_TO_DEPARTMENT_TYPE = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="⬅️ Назад", callback_data="department_back")],
])