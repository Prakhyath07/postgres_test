from django.db import models

# from constants import time_format_with_seconds, date_format
from postgres_test.current_approach.enums.ProductType import ProductType
from postgres_test.current_approach.enums.TimeFrame import TimeFrame
# import datetime as dt

class Candle(models.Model):
    """
    Model to store the candles
    """
    scrip_name = models.CharField(max_length=500, db_index=True)
    date = models.DateField(db_index=True)
    time = models.TimeField(db_index=True)
    time_frame = models.CharField(max_length=255, choices=TimeFrame.choices(), db_index=True)
    high = models.DecimalField(decimal_places=2, max_digits=20, null=True, blank=True)
    low = models.DecimalField(decimal_places=2, max_digits=20, null=True, blank=True)
    open = models.DecimalField(decimal_places=2, max_digits=20, null=True, blank=True)
    close = models.DecimalField(decimal_places=2, max_digits=20, null=True, blank=True)
    type = models.CharField(max_length=255, choices=ProductType.choices(), db_index=True, default=ProductType.EQUITY)
    oi = models.DecimalField(decimal_places=2, max_digits=20, null=True, blank=True)
    volume = models.DecimalField(decimal_places=2, max_digits=20, null=True, blank=True)

    # objects = CandleManager()
    def __str__(self):
        return "{0} {1} {2} {3} {4}".format(self.id, self.scrip_name, self.date, self.time, self.time_frame)
