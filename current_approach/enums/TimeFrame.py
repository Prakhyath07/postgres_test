from enum import Enum


class TimeFrame(Enum):

    MIN1 = "minute"
    MIN2 = "2minute"
    MIN3 = "3minute"
    MIN4 = "4minute"
    MIN5 = "5minute"
    MIN10 = "10minute"
    MIN15 = "15minute"
    MIN30 = "30minute"
    MIN60 = "60minute"
    HOUR2 = "2hour"
    HOUR3 = "3hour"
    DAY   = "day"

    @classmethod
    def choices(cls):
        return tuple((i.name, i.value) for i in cls)

    @classmethod
    def time_frame_key_value_tuple(cls):
        return tuple((i, i.value) for i in cls)

    @staticmethod
    def from_str(time_frame_string):
        for time_frame_name, time_frame_value in TimeFrame.time_frame_key_value_tuple():
            if time_frame_value == time_frame_string:
                return time_frame_name
        raise Exception("Invalid Time frame values. Expected values {0}".format(
            [i[1] for i in TimeFrame.time_frame_key_value_tuple()]))

    @staticmethod
    def get_time_in_minutes(time_frame):
        if time_frame == TimeFrame.MIN5:
            return 5
        if time_frame == TimeFrame.MIN1:
            return 1
        if time_frame == TimeFrame.MIN2:
            return 2
        if time_frame == TimeFrame.MIN3:
            return 3
        if time_frame == TimeFrame.MIN4:
            return 4

        if time_frame == TimeFrame.MIN10:
            return 10
        if time_frame == TimeFrame.MIN15:
            return 15
        if time_frame == TimeFrame.MIN30:
            return 30
        if time_frame == TimeFrame.MIN60:
            return 60
        if time_frame == TimeFrame.HOUR2:
            return 120
        if time_frame == TimeFrame.HOUR3:
            return 180
        if time_frame == TimeFrame.DAY:
            return 1440
        raise Exception("Invalid Time frame values. Expected values {0}".format(
            [i[1] for i in TimeFrame.time_frame_key_value_tuple()]))

class EncodingType(Enum):

    JSON = "json"
    ZLib = "zlib"
    MsgPack = "msgpack"

    @classmethod
    def choices(cls):
        return tuple((i.name, i.value) for i in cls)