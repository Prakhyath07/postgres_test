from django.db import models

# from postgres_test.current_approach.constants import time_format_with_seconds, date_format
from current_approach.enums.ProductType import ProductType
from current_approach.enums.TimeFrame import TimeFrame,EncodingType
# import datetime as dt

class Candle(models.Model):
    """
    Model to store the candles
    """
    scrip_name = models.CharField(max_length=500, db_index=True)
    date = models.DateField(db_index=True)
    time_frame = models.CharField(max_length=255, choices=TimeFrame.choices(), db_index=True)
    type = models.CharField(max_length=255, choices=ProductType.choices(), db_index=True, default=ProductType.EQUITY)
    candles_data = models.JSONField()
    encoding_type = models.CharField(max_length=50, db_index=True, choices=EncodingType.choices())

    # objects = CandleManager()

    def __str__(self):
        return "{0} {1} {2} {3} {4}".format(self.id, self.scrip_name, self.date, self.time_frame, self.encoding_type)

class CandleBytes(models.Model):
    """
    Model to store the candles
    """
    scrip_name = models.CharField(max_length=500, db_index=True)
    date = models.DateField(db_index=True)
    time_frame = models.CharField(max_length=255, choices=TimeFrame.choices(), db_index=True)
    type = models.CharField(max_length=255, choices=ProductType.choices(), db_index=True, default=ProductType.EQUITY)
    candles_data = models.BinaryField()
    encoding_type = models.CharField(max_length=50, db_index=True, choices=EncodingType.choices())

    # objects = CandleManager()

    def __str__(self):
        return "{0} {1} {2} {3} {4}".format(self.id, self.scrip_name, self.date, self.time_frame, self.encoding_type)
