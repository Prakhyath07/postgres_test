import datetime as dt
import json
import os
from multiprocessing.pool import ThreadPool
from collections import defaultdict

from current_approach.enums.ProductType import ProductType
from current_approach.enums.TimeFrame import EncodingType
from current_approach.utils import FnoCSV
from new_approach.models import CandleBytes
import zlib
import msgpack
import decimal
import base64

enc_type = EncodingType.ZLib
# class CandleToEncode:
#     def __init__(self, time, open_price, high, low, close, volume, oi):
#         self.time = time
#         self.open = open_price
#         self.high = high
#         self.low = low
#         self.close = close
#         self.volume = volume
#         self.oi = oi
#
#     def to_dict(self):
#         return {
#             "time": self.time,
#             "open": self.open,
#             "high": self.high,
#             "low": self.low,
#             "close": self.close,
#             "volume": self.volume,
#             "oi": self.oi
#         }
#
#     @classmethod
#     def from_dict(cls, data):
#         return cls(
#             time=data["time"],
#             open_price=data["open"],
#             high=data["high"],
#             low=data["low"],
#             close=data["close"],
#             volume=data["volume"],
#             oi=data["oi"]
#         )

class CandlesListToEncode:
    def __init__(self):
        self.candles = []

    def encode(self, encodingMethod :EncodingType):
        if encodingMethod == EncodingType.JSON:
            return json.dumps([candle.to_dict() for candle in self.candles])
        if encodingMethod == EncodingType.ZLib:
            return zlib_compress_and_encode(json.dumps([candle.to_dict() for candle in self.candles]).encode('utf-8'))
        if encodingMethod == EncodingType.MsgPack:
            return msgpack_serialize_and_encode([candle.to_dict() for candle in self.candles])



    def decode(self,data, encodingMethod :EncodingType):
        if encodingMethod == EncodingType.JSON:
            self.candles = [FnoCSV.from_dict(candle_data) for candle_data in json.loads(data)]
        if encodingMethod == EncodingType.ZLib:
            c =[FnoCSV.from_dict(candle_data) for candle_data in json.loads(zlib_decode_and_decompress(data).decode('utf-8'))]
            self.candles = c
        if encodingMethod == EncodingType.MsgPack:
            self.candles = [FnoCSV.from_dict(candle_data) for candle_data in msgpack_decode_and_deserialize(data)]


    def order_by_time(self):
        self.candles.sort(key=lambda x: x.time)

    def add_candle(self, candle):
        self.candles.append(candle)
        self.order_by_time()

def divide_by_scrip_and_date(fno_csv_list: list[FnoCSV]):
    divided_data = defaultdict(list)
    for fno_obj in fno_csv_list:
        key = (fno_obj.scrip_name, fno_obj.date)
        divided_data[key].append(fno_obj)
    return divided_data


def add__and_get_remarks(prev_remarks, new_remarks):
    if prev_remarks:
        return prev_remarks + '\n' + new_remarks
    else:
        return new_remarks


def parse_csv_list_and_insert_into_db(all_csv_files, time_frame, inclusions, exclusions):
    for file in all_csv_files:
        parse_csv_and_insert_into_db(file, time_frame, inclusions, exclusions)


def candle_exists_in_db(candle, scrip_date_time_map):
    # if scrip_date_time_map.get((candle.scrip_name, candle.date, candle.time), False):
    #     return True
    # return False
    # Check if the scrip_name and date exist in the map
    if (candle.scrip_name, candle.date) in scrip_date_time_map:
        # Check if the candle's time exists in the inner map
        if candle.time in scrip_date_time_map[(candle.scrip_name, candle.date)]:
            return True
    return False


def parse_csv_and_insert_into_db(file, time_frame, inclusions, exclusions):
    remarks = ''
    start = dt.datetime.now()
    candle_bulk_create = CandleBulkCreate()
    try:
        fno_data_list = FnoCSV.from_csv_file(file, inclusions, exclusions)
        print("total data - " + str(len(fno_data_list)))
        divided_data = divide_by_scrip_and_date(fno_data_list)
        remarks = add__and_get_remarks(remarks, "total data - " + str(len(fno_data_list)))
        print("Fetching candles for file {0}".format(file))
        remarks = add__and_get_remarks(remarks, "Fetching candles for file {0}".format(file))
        scrip_date_time_map = CandlesInDb(time_frame).get_results(fno_data_list)
        print("Fetching candles done.....")
        remarks = add__and_get_remarks(remarks, "Fetching candles done.....")
        for key, candles in divided_data.items():
            # if not candle_exists_in_db(candle, scrip_date_time_map):
            insert_fno_data_into_db(candles, scrip_date_time_map, candle_bulk_create, time_frame)
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


def insert_fno_data_into_db(candles, scrip_date_time_map, candle_bulk_create, time_frame):
    to_add = []
    for cand in candles:
        scrip_name = cand.scrip_name
        date = cand.date
        if not candle_exists_in_db(cand, scrip_date_time_map):
            to_add.append(cand)
    if len(to_add) > 0:
        existing_cand = CandleBytes.objects.filter(scrip_name=scrip_name, date=date, time_frame=time_frame.name,
                                            encoding_type=enc_type)
        data = CandlesListToEncode()
        if existing_cand:
            enum_type, enum_value = existing_cand.encoding_type.split('.')
            enum_type = getattr(EncodingType, enum_type)
            data.decode(existing_cand.candles_data, encodingMethod=enum_type)
        for cand in to_add:
            data.add_candle(cand)
        enc_data=data.encode(encodingMethod=enc_type)
        candle_bulk_create.add(CandleBytes(
            scrip_name=scrip_name, date=date, time_frame=time_frame.name, type=ProductType.NFO,
            candles_data=enc_data, encoding_type=enc_type
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
        # Initialize the inner map for the scrip and date if not present
        if (scrip_name, date) not in self.scrip_date_time_map:
            self.scrip_date_time_map[(scrip_name, date)] = {}

        # Fetch candles from the database
        for candle in CandleBytes.objects.filter(scrip_name=scrip_name, date=date,
                                            time_frame=self.time_frame.name,
                                            encoding_type=enc_type):
            data = CandlesListToEncode()
            enum_type, enum_value = candle.encoding_type.split('.')
            enum_type = getattr(EncodingType, enum_value)
            data.decode(candle.candles_data, encodingMethod=enum_type)
            for time_data in data.candles:
                # time_key = candle.time.strftime('%H:%M:%S')
                # Store the candle in the inner map
                time_key = time_data.time
                self.scrip_date_time_map[(scrip_name, date)][time_key] = True

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

    def candle_exists_in_db(self, candle):
        try:
            # Check if the scrip_name and date exist in the map
            if (candle.scrip_name, candle.date) in self.scrip_date_time_map:
                # Check if the candle's time exists in the inner map
                if candle.time.strftime('%H:%M:%S') in self.scrip_date_time_map[(candle.scrip_name, candle.date)]:
                    return True
            return False
        except Exception as e:
            print(candle, e)
            raise e

    def get_results(self, candles):
        self.fetch_scrip_and_create_map(candles)
        return self.scrip_date_time_map

def zlib_compress_and_encode(data):
    compressed_data = zlib.compress((data))
    return base64.b64encode(compressed_data)

def zlib_decode_and_decompress(encoded_data):
    compressed_data = base64.b64decode(encoded_data)
    return zlib.decompress(compressed_data)

def msgpack_serialize_and_encode(data):
    serialized_data = msgpack.packb(data)
    return base64.b64encode(serialized_data)

def msgpack_decode_and_deserialize(encoded_data):
    serialized_data = base64.b64decode(encoded_data)
    return msgpack.unpackb(serialized_data)