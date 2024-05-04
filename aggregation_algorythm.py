from pymongo import MongoClient
from enum import Enum
from settings import MDB_URL
from datetime import datetime, timedelta


class Aggregator:
    
    def __init__(self, data: dict) -> None:
        self.__down_date = datetime.strptime(data['dt_from'], '%Y-%m-%dT%H:%M:%S')
        self.__up_date = datetime.strptime(data['dt_upto'], '%Y-%m-%dT%H:%M:%S')
        
        try:
            self.__aggregation = data['group_type']
            if self.__aggregation not in ['hour', 'day', 'month']:
                raise ValueError('Need correct value like: hour, day, month')
        except ValueError as e:
            print(str(e))
        
        self.__create_connection()
    
    def __create_connection(self) -> None:
        self.__client = MongoClient(MDB_URL)
        self.__collection = self.__client['salary_data']['sample_collection']
    
    def __aggregate_data(self) -> None:
        self.results = []
        self.dates = []
        match self.__aggregation:
            case 'month':
                for month in range(self.__down_date.month, self.__up_date.month + 1):
                    summ = 0
                    days = 1
                    deltayear = 0
                    if self.__down_date.day != 1:
                        days = self.__down_date.day
                    d_dt = self.__down_date.replace(month=month, day=days, hour=0, minute=0, second=0)
                    if month % 12 == 0:
                        deltayear = 1
                    u_dt = self.__down_date.replace(year=self.__down_date.year + deltayear, month=month % 12 + 1, day=1, hour=23, minute=59, second=0) - timedelta(days=1)
                    collection = self.__collection.find({
                        'dt': {
                            '$gte': d_dt,
                            '$lte': u_dt
                            }
                        },  {
                            '_id': 0,
                            'value': 1
                        })
                    for val in collection:
                        summ += val['value']
                    self.dates.append(datetime.isoformat(d_dt))
                    self.results.append(summ)
            case 'day':
                pass

                
    def get_json(self) -> str:
        self.__aggregate_data()

a = Aggregator({
"dt_from":"2022-09-01T00:00:00",
"dt_upto":"2022-12-31T23:59:00",
"group_type":"month"
})
a.get_json()
print(a.results, a.dates)

    
        
