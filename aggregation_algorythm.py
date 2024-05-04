from pymongo import MongoClient
from enum import Enum
from settings import MDB_URL
from datetime import datetime, timedelta

import json

class Aggregator:
    
    def __init__(self, data: dict) -> None:
        try:
            data = json.loads(data)
            self.__down_date = datetime.strptime(data['dt_from'], '%Y-%m-%dT%H:%M:%S')
            self.__up_date = datetime.strptime(data['dt_upto'], '%Y-%m-%dT%H:%M:%S')
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
        self.__results = []
        self.__dates = []
        match self.__aggregation:
            case 'month':
                current_year = self.__down_date.year
                for month in range(self.__down_date.month, 12 * (self.__up_date.year - self.__down_date.year) + self.__up_date.month + 1):
                    summ = 0
                    days = 1
                    deltayear = 0
                    next_year = False
                    if self.__up_date.year - self.__down_date.year != 0:
                        if month % 12 != 0:
                            month %= 12
                        else:
                            month = 12
                            next_year = True
                    if self.__down_date.day != 1:
                        days = self.__down_date.day
                    if month % 12 == 0:
                        deltayear = 1
                    d_dt = self.__down_date.replace(year=current_year, month=month, day=days, hour=0, minute=0, second=0)
                    u_dt = self.__down_date.replace(year=current_year + deltayear, month=month % 12 + 1, day=1, hour=23, minute=59, second=0) - timedelta(days=1)
                    collection = self.__collection.find({
                        'dt': {
                            '$gte': d_dt,
                            '$lte': u_dt
                            }
                        },  {
                            '_id': 0,
                            'value': 1,
                            'dt': 1
                        })
                    for val in collection:
                        summ += val['value']
                    self.dates.append(datetime.isoformat(d_dt))
                    self.results.append(summ)
                    if next_year:
                        current_year += 1
            case 'day': 
                current_year = self.__down_date.year
                for month in range(self.__down_date.month, 12 * (self.__up_date.year - self.__down_date.year) + self.__up_date.month + 1):  
                    next_year = False
                    if self.__up_date.year - self.__down_date.year != 0:
                        if month % 12 != 0:
                            month %= 12
                        else:
                            month = 12
                            next_year = True
                    if month == self.__down_date.month:
                        down_day = self.__down_date.day
                    else:
                        down_day = 1
                    if month != self.__up_date.month:
                        up_day = (self.__down_date.replace(year=current_year, month=month % 12 + 1, day=1) - timedelta(days=1)).day + 1
                    else:
                        up_day = self.__up_date.day + 1
                    for day in range(down_day, up_day): 
                        summ = 0                        
                        d_dt = self.__down_date.replace(year=current_year, month=month, day=day, hour=0, minute=0, second=0)
                        u_dt = self.__down_date.replace(year=current_year, month=month, day=day, hour=23, minute=59, second=0)
                        collection = self.__collection.find({
                            'dt': {
                                '$gte': d_dt,
                                '$lte': u_dt
                                }
                            },  {
                                '_id': 0,
                                'value': 1,
                                'dt': 1
                            })
                        for val in collection:
                            summ += val['value']
                        self.__dates.append(datetime.isoformat(d_dt))
                        self.__results.append(summ)
                    if next_year:
                        current_year += 1
            case 'hour':
                current_year = self.__down_date.year
                for month in range(self.__down_date.month, 12 * (self.__up_date.year - self.__down_date.year) + self.__up_date.month + 1):  
                    next_year = False
                    if self.__up_date.year - self.__down_date.year != 0:
                        if month % 12 != 0:
                            month %= 12
                        else:
                            month = 12
                            next_year = True
                    if month == self.__down_date.month:
                        down_day = self.__down_date.day
                    else:
                        down_day = 1
                    if month != self.__up_date.month:
                        up_day = (self.__down_date.replace(year=current_year, month=month % 12 + 1, day=1) - timedelta(days=1)).day + 1
                    else:
                        up_day = self.__up_date.day + 1
                    for day in range(down_day, up_day): 
                        hours_d = 0
                        minutes_d = 0
                        hours_u = 23
                        minutes_u = 59
                        if day == self.__down_date.day and month == self.__down_date.month:
                            if self.__down_date.hour != 0 and self.__down_date.minute != 0:
                                hours_d = self.__down_date.hour
                                minutes_d = self.__down_date.minute
                        if day == self.__up_date.day and month == self.__up_date.month:       
                            if self.__up_date.hour != 0:
                                hours_u = self.__up_date.hour
                                minutes_u = self.__up_date.minute
                            else:
                                hours_u = 0
                                minutes_u = 0
                        for hour in range(hours_d, hours_u + 1):
                            summ = 0
                                
                            d_dt = self.__down_date.replace(year=current_year, month=month, day=day, hour=hour, minute=minutes_d, second=0)
                            u_dt = self.__down_date.replace(year=current_year, month=month, day=day, hour=hour, minute=minutes_u, second=59)
                            
                            collection = self.__collection.find({
                                'dt': {
                                    '$gte': d_dt,
                                    '$lte': u_dt
                                    }
                                },  {
                                    '_id': 0,
                                    'value': 1,
                                    'dt': 1
                                })
                            for val in collection:
                                summ += val['value']
                            self.__dates.append(datetime.isoformat(d_dt))
                            self.__results.append(summ)
                    if next_year:
                        current_year += 1
                 

                
    def get_json(self) -> str:
        self.__aggregate_data()
        self.__client.close()
        return {"dataset": self.__results, "labels": self.__dates}
