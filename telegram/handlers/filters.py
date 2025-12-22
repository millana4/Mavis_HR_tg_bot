from aiogram.filters import Filter
from aiogram import types

from app.services.fsm import state_manager, AppStates


class FormFilter(Filter):
    def __init__(self, state: str):
        self.state = state

    async def __call__(self, message: types.Message) -> bool:
        data = await state_manager.get_data(message.from_user.id)
        return data.get('current_state') == self.state


class NameSearchFilter(Filter):
    async def __call__(self, message: types.Message) -> bool:
        user_data = await state_manager.get_data(message.from_user.id)
        return user_data.get('current_state') == AppStates.WAITING_FOR_NAME_SEARCH


class SearchTypeFilter(Filter):
    async def __call__(self, message: types.Message) -> bool:
        user_data = await state_manager.get_data(message.from_user.id)
        return user_data.get('current_state') == AppStates.WAITING_FOR_SEARCH_TYPE


class ShopSearchFilter(Filter):
    async def __call__(self, message: types.Message) -> bool:
        user_data = await state_manager.get_data(message.from_user.id)
        return user_data.get('current_state') == AppStates.WAITING_FOR_SHOP_TITLE_SEARCH


class DrugstoreSearchFilter(Filter):
    async def __call__(self, message: types.Message) -> bool:
        user_data = await state_manager.get_data(message.from_user.id)
        return user_data.get('current_state') == AppStates.WAITING_FOR_DRUGSTORE_TITLE_SEARCH