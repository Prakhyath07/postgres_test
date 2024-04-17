from os import listdir
from os.path import join, isfile
import os

from django.core.management.base import BaseCommand

from current_approach.enums.TimeFrame import TimeFrame
from current_approach.utils import parse_csv_list_and_insert_into_db


class Command(BaseCommand):
    help = 'Displays current time'
    date_format = "%Y-%m-%d"
    scrip_name = "BANKNIFTY"

    data_folder = "data"
    time_frame = TimeFrame.MIN1

    def add_arguments(self, parser):
        parser.add_argument('--scrip_name', type=str)
        parser.add_argument('--scrip_name_exclude', type=str)
        parser.add_argument('--directory', type=str, nargs='?', default="")

    def handle(self, *args, **kwargs):
        self.scrip_name = kwargs['scrip_name']
        self.scrip_name_exclude = kwargs['scrip_name_exclude']
        self.directory = kwargs['directory']
        self.stdout.write("Scraping FNO")
        csv_files = self.get_all_csv_files()
        parse_csv_list_and_insert_into_db(csv_files, self.time_frame, self.scrip_name.split(','), self.scrip_name_exclude.split(','))
        self.stdout.write("%s" % csv_files)

    def get_all_csv_files(self):
        folders = [f.path for f in os.scandir(self.data_folder) if f.is_dir()]
        all_csv_files = []
        for folder in folders:
            print(folder, self.directory)
            if self.directory and self.directory not in folder:
                print(folder, self.directory, 'Not Matched')
                continue
            print(folder, self.directory, 'Matched')
            all_csv_files = all_csv_files + \
                            [join(folder, f) for f in listdir(folder) if isfile(join(folder, f)) and ('.csv' in f or '.CSV' in f)]
        return all_csv_files
