# Generated by Django 4.2.11 on 2024-04-15 12:36

import current_approach.enums.ProductType
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="Candle",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("scrip_name", models.CharField(db_index=True, max_length=500)),
                ("date", models.DateField(db_index=True)),
                (
                    "time_frame",
                    models.CharField(
                        choices=[
                            ("MIN1", "minute"),
                            ("MIN2", "2minute"),
                            ("MIN3", "3minute"),
                            ("MIN4", "4minute"),
                            ("MIN5", "5minute"),
                            ("MIN10", "10minute"),
                            ("MIN15", "15minute"),
                            ("MIN30", "30minute"),
                            ("MIN60", "60minute"),
                            ("HOUR2", "2hour"),
                            ("HOUR3", "3hour"),
                            ("DAY", "day"),
                        ],
                        db_index=True,
                        max_length=255,
                    ),
                ),
                (
                    "type",
                    models.CharField(
                        choices=[
                            ("INDEX", "INDEX"),
                            ("EQUITY", "EQUITY"),
                            ("NFO", "NFO"),
                        ],
                        db_index=True,
                        default=current_approach.enums.ProductType.ProductType[
                            "EQUITY"
                        ],
                        max_length=255,
                    ),
                ),
                ("candles_data", models.JSONField()),
            ],
        ),
    ]