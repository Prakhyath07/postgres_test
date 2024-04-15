import datetime as dt
import json
import os
from multiprocessing.pool import ThreadPool


from postgres_test.current_approach.enums.ProductType import ProductType
from postgres_test.current_approach.models import Candle
import decimal


class FnoCSV:
    def __init__(self, data) -> None:
        super().__init__()
        self.scrip_name = data[0]
        try:
            self.date = dt.datetime.strptime(data[1], "%d/%m/%Y").strftime("%Y-%m-%d")
        except Exception as e:
            self.date = dt.datetime.strptime(data[1], "%d-%m-%Y").strftime("%Y-%m-%d")
        self.time = data[2]
        self.open = decimal.Decimal(data[3])
        self.high = decimal.Decimal(data[4])
        self.low = decimal.Decimal(data[5])
        self.close = decimal.Decimal(data[6])
        self.volume = decimal.Decimal(data[7])
        self.oi = decimal.Decimal(data[8])

    @staticmethod
    def from_csv_file(file_name, prefix_inclusions, prefix_exclusions):
        fno_csv = []
        with open(file_name, "r") as f:
            data = f.read().split('\n')
            headers = data.pop(0).split(',')
            if not data[-1]:
                data.pop()
            data = [x.split(',') for x in data]
            if FnoCSV.validate_data(headers, data):
                filtered_data = [x for x in data if FnoCSV.if_data_is_valid(prefix_inclusions, prefix_exclusions, x[0])]
                len(filtered_data)
                fno_csv = fno_csv + [FnoCSV([y.strip() for y in x]) for x in filtered_data]
            else:
                raise Exception("Data not valid")

        return fno_csv

    @staticmethod
    def if_data_is_valid(prefix_inclusions, prefix_exclusions, scrip_name):
        for pattern in prefix_exclusions:
            if scrip_name.startswith(pattern):
                return False

        for pattern in prefix_inclusions:
            if not pattern.strip():
                continue
            if scrip_name.startswith(pattern):
                return True
        return False

    @staticmethod
    def validate_data(headers, data):
        for i, x in enumerate(data):
            if x and len(x) != len(headers):
                return False
        return True

    def to_dict(self):
        return {
            "time": self.time,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "oi": self.oi
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            time=data["time"],
            open_price=data["open"],
            high=data["high"],
            low=data["low"],
            close=data["close"],
            volume=data["volume"],
            oi=data["oi"]
        )
    def __str__(self) -> str:
        return "FNO CSV - " + str(self.date) + " " + str(self.time)


def add__and_get_remarks(prev_remarks, new_remarks):
    if prev_remarks:
        return prev_remarks + '\n' + new_remarks
    else:
        return new_remarks


def parse_csv_list_and_insert_into_db(all_csv_files, time_frame, inclusions, exclusions):
    for file in all_csv_files:
        parse_csv_and_insert_into_db(file, time_frame, inclusions, exclusions)


def candle_exists_in_db(candle, scrip_date_time_map):
    if scrip_date_time_map.get((candle.scrip_name, candle.date, candle.time), False):
        return True
    return False


def parse_csv_and_insert_into_db(file, time_frame, inclusions, exclusions):
    remarks = ''
    start = dt.datetime.now()
    candle_bulk_create = CandleBulkCreate()
    try:
        fno_data_list = FnoCSV.from_csv_file(file, inclusions, exclusions)
        print("total data - " + str(len(fno_data_list)))
        remarks = add__and_get_remarks(remarks, "total data - " + str(len(fno_data_list)))
        print("Fetching candles for file {0}".format(file))
        remarks = add__and_get_remarks(remarks, "Fetching candles for file {0}".format(file))
        scrip_date_time_map = CandlesInDb(time_frame).get_results(fno_data_list)
        print("Fetching candles done.....")
        remarks = add__and_get_remarks(remarks, "Fetching candles done.....")
        for idx, candle in enumerate(fno_data_list):
            if not candle_exists_in_db(candle, scrip_date_time_map):
                insert_fno_data_into_db(candle, candle_bulk_create, time_frame)
            # else:
            #     self.stdout.write("Skipped for: {0}, {1}".format(candle.date, candle.time))
        candle_bulk_create.join()
        print("Candles created....!!!!!")
        remarks = add__and_get_remarks(remarks, "Candles created....!!!!!")
        os.remove(file)
        print("Time Consumed - {0}".format(dt.datetime.now() - start))
        remarks = add__and_get_remarks(remarks, "Time Consumed - {0}".format(dt.datetime.now() - start))
    except Exception as e:
        print(e)
        print([json.dumps(obj, default=str) for obj in candle_bulk_create.objects])
        return "Error {0}".format(e), False

    return remarks, True


def insert_fno_data_into_db(candle, candle_bulk_create, time_frame):
    candle_bulk_create.add(Candle(
        scrip_name=candle.scrip_name, date=candle.date, time=candle.time, time_frame=time_frame.name,
        high=candle.high, low=candle.low, open=candle.open, close=candle.close, type=ProductType.NFO,
        oi=candle.oi, volume=candle.volume,
    ))


class CandleBulkCreate:
    objects = []
    size = 10000

    def __init__(self) -> None:
        super().__init__()

    def add(self, obj):
        self.objects.append(obj)
        self.check_and_create()

    def check_and_create(self):
        if len(self.objects) >= self.size:
            self.join()

    def join(self):
        if len(self.objects):
            for candle in self.objects:
                candle.save()
            self.objects = []


class CandlesInDb:

    def __init__(self, time_frame) -> None:
        super().__init__()
        self.time_frame = time_frame
        self.scrip_date_time_map = {}

    def fetch_candles_from_db(self, scrip_name, date):
        for candle in Candle.objects.filter(scrip_name=scrip_name, date=date, time_frame=self.time_frame.name):
            self.scrip_date_time_map[(scrip_name, date, candle.time.strftime('%H:%M:%S'))] = True

    def fetch_scrip_and_create_map(self, candles):
        unique_scrip_date_pairs = {}
        for candle in candles:
            unique_scrip_date_pairs[(candle.scrip_name, candle.date)] = 1

        scrips = []
        dates = []

        for scrip, date in unique_scrip_date_pairs.keys():
            scrips.append(scrip)
            dates.append(date)

        pool = ThreadPool(30)
        pool.starmap(self.fetch_candles_from_db, zip(scrips, dates))

    def candle_exists_in_db(self, candle, percent):
        # self.stdout.write("Fetching for : {0}, {1}, {2}%".format(candle.date, candle.time, percent))
        # print(candle)
        try:
            if self.scrip_date_time_map.get((candle.scrip_name, candle.date, candle.time), False):
                return True
            # return Candle.objects.filter(scrip_name=candle.scrip_name, date=candle.date, time=candle.time,
            #                              time_frame=self.time_frame.name).first() is not None
            return False
        except Exception as e:
            print(candle, e)
            1 / 0

    def get_results(self, candles):
        self.fetch_scrip_and_create_map(candles)
        return self.scrip_date_time_map
