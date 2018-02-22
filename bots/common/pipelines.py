"""common pipelines"""
import calendar
import re
from collections import OrderedDict
from datetime import datetime

from dateutil import parser
from scrapy.exceptions import DropItem

import config
from configutil import *
from utils import *

logger = logging.getLogger(__name__)


class CsvWriterPipelineError(StandardError):
    pass


class DataCleanerPipeline(object):
    """
    Trimming the texts of unnecessary characters like tabs/newLines/spaces
    Decoding the texts
    """

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        middleware = cls(crawler.settings)
        return middleware

    def __init__(self, settings):
        self.settings = settings
        self.join_list = settings.getbool("JOIN_LIST", False)
        self.html_decode = settings.getbool("HTML_DECODE", False)

    def process_item(self, item, spider):
        for key, val in item.items():
            if type(val) is list:
                item[key] = [self.clean_text(x) for x in val]
                if self.join_list:
                    item[key] = WHITESPACE.join(item[key])
            else:
                item[key] = self.clean_text(val)
        return item

    def clean_text(self, text):
        text = trim(text)
        if self.html_decode:
            text = html_decode(text)
        return text


class DateFormatter(object):
    """
    1. Extracts date from a text, if date is present, based on the date-format configuration
    `EXTRACT_DATE_FORMAT. By default, if this pipeline included, extracts any supported format found in the text`
    2. Formats date based on the configuration `DATE_FORMAT` provided
    """
    delim_pattern = '(\s*[-/.\s]+\s*)'
    delim_space_pattern = '\s*'
    dd_pattern = '(3[01]|[12][0-9]|0?[1-9])'
    ddth_pattern = '(0?[1-9]|[12][0-9]|3[01])(st|nd|rd|th)?'
    mm_pattern = '(0?[1-9]|1[012])'
    mmm_pattern = '(Jan(uary)?|Feb(ruary)?|Mar(ch)?|Apr(il)?|May|Jun(e)?|Jul(y)?' \
                  '|Aug(ust)?|Sep(tember)?|Sept|Oct(ober)?|Nov(ember)?|Dec(ember)?)'
    yyyy_pattern = '((19|20)?\d{2})'
    YYYY_pattern = '\d{4}'
    YY_pattern = '\d{2}'
    quarter_pattern = '~?Q(uarter)?[1-4]\s*(of)?'
    extract_format_pattern_dict = OrderedDict([
        ("MMMDD,YYYY",
         r'{mmm}{delim}{ddth}[,.]{delim}{yyyy}'
         .format(mmm=mmm_pattern, ddth=ddth_pattern, yyyy=yyyy_pattern, delim=delim_pattern)),
        ("DDMMMYYYY",
         r'{dd}{delim}{mmm}{delim}{yyyy}'
         .format(mmm=mmm_pattern, dd=dd_pattern, yyyy=YYYY_pattern, delim=delim_pattern)),
        ("DDMMYYYY",
         r'{dd}{delim}{mm}{delim}{yyyy}'
         .format(mm=mm_pattern, dd=dd_pattern, yyyy=YYYY_pattern, delim=delim_pattern)),
        ("MMDDYYYY",
         r'{mm}{delim}{dd}{delim}{yyyy}'
         .format(mm=mm_pattern, dd=dd_pattern, yyyy=YYYY_pattern, delim=delim_pattern)),
        ("YYYYMMDD",
         r'{yyyy}{delim}{mm}{delim}{dd}'
         .format(mm=mm_pattern, dd=dd_pattern, yyyy=YYYY_pattern, delim=delim_pattern)),
        ("DDMMMYY",
         r'{dd}{delim}{mmm}{delim}{yy}'
         .format(mmm=mmm_pattern, dd=dd_pattern, yy=YY_pattern, delim=delim_pattern)),
        ("DDMMYY",
         r'{dd}{delim}{mm}{delim}{yy}'
         .format(mm=mm_pattern, dd=dd_pattern, yy=YY_pattern, delim=delim_pattern)),
        ("MMDDYY",
         r'{mm}{delim}{dd}{delim}{yy}'
         .format(mm=mm_pattern, dd=dd_pattern, yy=YY_pattern, delim=delim_pattern)),
        ("YYMMDD",
         r'{yy}{delim}{mm}{delim}{dd}'
         .format(mm=mm_pattern, dd=dd_pattern, yy=YY_pattern, delim=delim_pattern)),
        ("MMMYYYY",
         r'{mmm},?{delim}{yyyy}'
         .format(mmm=mmm_pattern, yyyy=YYYY_pattern, delim=delim_pattern)),
        ("YYYYMMM",
         r'{mmm},?{delim}{yyyy}'
         .format(mmm=mmm_pattern, yyyy=YYYY_pattern, delim=delim_pattern)),
        ("MMYYYY",
         r'{mm}{delim}{yyyy}'
         .format(mm=mm_pattern, yyyy=yyyy_pattern, delim=delim_pattern)),
        ("YYYYMM",
         r'{mm}{delim}{yyyy}'
         .format(mm=mm_pattern, yyyy=YYYY_pattern, delim=delim_pattern)),
        ("QYYYY", r'{quarter}{delim}{yyyy}'
         .format(quarter=quarter_pattern, yyyy=yyyy_pattern, delim=delim_pattern)),
        ("YYYY", r'{yyyy}'
         .format(yyyy=YYYY_pattern))
    ])

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        middleware = cls(crawler.settings)
        return middleware

    def __init__(self, settings):
        self.settings = settings
        self.extract_date_formats = settings.get("EXTRACT_DATE_FORMATS", self.extract_format_pattern_dict.keys())
        self.matchers = OrderedDict()
        self.date_format = settings.get("DATE_FORMAT", config.DEFAULT_DATE_FORMAT)
        self.mon_year_format = config.DEFAULT_MMYYYY_FORMAT
        if set(self.extract_date_formats) - set(self.extract_format_pattern_dict.keys()):
            raise NotImplementedError("Not implemented for {list}".format(
                list=set(self.extract_date_formats) - set(self.extract_format_pattern_dict.keys())))

        for key, value in self.extract_format_pattern_dict.items():
            if key in self.extract_date_formats:
                self.matchers[key] = re.compile(value, flags=re.IGNORECASE)

        self.quarter_matcher = re.compile(self.quarter_pattern)
        self.delim_matcher = re.compile(self.delim_pattern)
        self.yyyy_matcher = re.compile(self.yyyy_pattern)

    def process_item(self, item, spider):
        for field in item.fields:
            if field in item and item.fields[field].get('date_field', False):
                for pattern, matcher in self.matchers.items():
                    matched = matcher.search(item.get(field)) if item.get(field, None) else None
                    if matched:
                        matched_content = matched.group()
                        if self.date_format:  # output date formatting
                            try:
                                if pattern in ["MMMYYYY", "YYYYMMM", "MMYYYY", "YYYYMM"]:  # only month_year pattern
                                    date = parser.parse(matched_content)
                                    if self.start_or_end_date_field(field, item.fields[field], item[field],
                                                                    matched_content) is config.END:
                                        """assign last date of month"""
                                        date = date.replace(day=calendar.monthrange(date.year, date.month)[1])
                                    else:
                                        """assign first date of month"""
                                        date = date.replace(day=1)
                                elif pattern == "QYYYY":
                                    date = self.get_datetime_by_quarter_pattern(matched_content)
                                    if self.start_or_end_date_field(field, item.fields[field], item[field],
                                                                    date) is config.END:
                                        """assign last date of month"""
                                        date = date.replace(day=calendar.monthrange(date.year, date.month)[1])
                                    else:
                                        """assign first date of month"""
                                        date = date.replace(day=1)
                                elif pattern == "YYYY":  # only year pattern
                                    date = parser.parse(matched_content)
                                    if self.start_or_end_date_field(field, item.fields[field], item[field],
                                                                    matched_content) is config.END:
                                        """assign last date of last month of year"""
                                        date = date.replace(month=12, day=calendar.monthrange(date.year, 12)[1])
                                    else:
                                        """assign first date of first month of year"""
                                        date = date.replace(month=1, day=1)
                                else:
                                    date = parser.parse(matched_content)
                                item[field] = date.strftime(self.date_format)
                            except:  # invalid/un-parseable dates; leave it as it is
                                pass
                        break

        return item

    def get_datetime_by_quarter_pattern(self, value):
        month = self.quarter_matcher.search(value).group()
        month = re.search(r'[1-4]', month).group()
        year = self.quarter_matcher.sub(EMPTY_STRING, value)
        year = self.yyyy_matcher.search(year).group()
        if month and year:
            month, year = int(month), int(year)
            month = config.QUARTER_CONVENTION.get(month)
            day = calendar.monthrange(year, month)[1]
            return datetime(year, month, day)
        return None

    @staticmethod
    def start_or_end_date_field(field, field_prop, actual_content, matched_content):
        """
        It returns the date_field type whether it indicates start or end of an event
        and returns correspondingly config.START or, config.END.
        By default returns config.END
        """
        if field_prop.get("end_date_field"):
            return config.END
        if field_prop.get("start_date_field"):
            return config.START
        if "earlier" in actual_content:
            return config.START
        if "later" in actual_content:
            return config.END
        if "start" in field.lower():
            return config.START
        if "end" in field.lower():
            return config.END

        return config.END


class CsvWriterPipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        middleware = cls(crawler.settings)
        return middleware

    def __init__(self, settings):
        self.settings = settings
        self.items = []

    def process_item(self, item, spider):
        self.items.append(item)
        return item

    def close_spider(self, spider):
        if spider.driver:
            spider.driver.quit()

        if hasattr(spider, "data_file"):
            file_path = spider.data_file
        else:
            file_path = get_data_file_path(spider)
        headers = self.settings.get("HEADERS", None)
        if hasattr(spider, "headers"):
            headers = spider.headers
        elif hasattr(spider, "HEADERS"):
            headers = spider.HEADERS
        elif not headers and get_headers(spider.name):
            headers = get_headers(spider.name)

        if not headers:
            headers = self.items[0].fields.keys() if self.items else []
            logger.warning("Couldn't find headers configuration (either in settings.py -- HEADERS or, spider's "
                           "attribute -- headers). So, all the fields (in Item) are being "
                           "written in the CSV (order not guaranteed).", extra={'spider': spider})

        logger.debug("Writing data file to %(path)r", {'path': file_path}, extra={'spider': spider})
        write_to_csv_from_json(file_path, headers, self.items)

        # writing visited
        for item in self.items:
            if item[3] > self.loaded.get(item[0], ''):
                spider.loaded[item[0]] = item[3]
        with open(spider.data_dir + 'visited.csv', 'rb') as csv_file:
            reader = csv.reader(csv_file)
            for row in reader:
                if row[0] not in spider.loaded:
                    spider.loaded[row[0]] = row[1]
        with open(spider.data_dir + 'visited.csv', 'wb') as csv_file:
            writer = csv.writer(csv_file)
            for user in spider.loaded:
                writer.writerow([user, spider.loaded[user]])


class DuplicateItemFilter(object):
    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls(crawler.settings)
        return middleware

    def __init__(self, settings):
        self.settings = settings
        self.items_seen = set()

    def process_item(self, item, spider):
        unique_keys = self.settings.get("UNIQUE_KEY", None)
        if hasattr(spider, "UNIQUE_KEY"):
            unique_keys = spider.UNIQUE_KEY

        item_id = tuple(map((lambda key: item.get(key, None)), unique_keys))

        if item_id in self.items_seen:
            add_duplicates_to_spider_stats(item, spider)
            raise DropItem("Dropped duplicate item found: %s" % item)
        else:
            self.items_seen.add(item_id)
            return item
