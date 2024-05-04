from settings import TOKEN
from aiogram import Bot, Dispatcher, types, executor
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aggregation_algorythm import Aggregator

class AggregatorBot(Bot):
    
    def __init__(self) -> None:
        super().__init__(token=TOKEN)


class AggrState(StatesGroup):
    wait_for_json = State()


bot = AggregatorBot()
storage = MemoryStorage()
dp = Dispatcher(bot=bot, storage=storage)

@dp.message_handler(commands=['start'])
async def handle_start(message: types.Message, state: FSMContext) -> None:
    await AggrState.wait_for_json.set()

@dp.message_handler(state=AggrState.wait_for_json)
async def get_aggr_json(message: types.Message, state: FSMContext) -> None:
    json = message.text
    aggr = Aggregator(json)
    
    await message.answer(aggr.get_json())

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)