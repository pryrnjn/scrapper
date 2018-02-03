import errno
import logging
import os
import time

from bots.common.config import HEADERS

logger = logging.getLogger(__name__)


def get_headers(vendor):
    return HEADERS.get(vendor, None)


def get_or_create_vendor_dir(spider, base_dir=""):
    dir_path = os.path.join(base_dir, spider.name)
    try:
        os.makedirs(dir_path)
    except OSError as e:
        if e.errno == errno.EEXIST:  # already exists
            pass  # good to know!
        else:
            raise IOError("Couldn't create vendor specific directory", e)
    except Exception as e:
        raise IOError("Couldn't create vendor specific directory", e)
    return dir_path


def get_data_file_path(spider, file_extension=None, appender=None):
    base_dir = spider.DATA_OUTPUT_DIR or "data"
    file_extension = spider.DATA_FORMAT if not file_extension else file_extension
    dir_path = get_or_create_vendor_dir(spider, base_dir)
    appender = "_{appender}".format(appender=appender) if appender else ""
    return os.path.join(dir_path, "%s%s_%s.%s"
                        % (spider.name, appender, get_date_time_stamp(spider), file_extension))


def get_error_file_path(spider, file_extension=None, appender=None):
    base_dir = spider.ERROR_OUTPUT_DIR or "errors"
    file_extension = spider.DATA_FORMAT if not file_extension else file_extension
    dir_path = get_or_create_vendor_dir(spider, base_dir)
    appender = "_{appender}".format(appender=appender) if appender else ""
    return os.path.join(dir_path, "%s%s_%s.%s"
                        % (spider.name, appender, get_date_time_stamp(spider), file_extension))


def get_log_file_path(spider, file_extension="log"):
    base_dir = spider.LOG_OUTPUT_DIR or "logs"
    dir_path = get_or_create_vendor_dir(spider, base_dir)
    return os.path.join(dir_path, "%s_%s.%s"
                        % (spider.name, get_date_time_stamp(spider), file_extension))


def get_start_time(spider):
    return spider.crawler.stats.get_value('start_time')


def get_date_stamp(spider):
    return get_start_time(spider).strftime("%Y%m%d")


def get_date_time_stamp(spider):
    return get_start_time(spider).strftime(spider.FILE_TIMESTAMP_FORMAT or "%Y%m%d_%H%M%S")


def log_spider_duration(spider):
    start_time = get_start_time(spider)
    end_time = time.time()
    logger.debug("Spider completed in %(time)r", {'time': int(end_time - start_time)},
                 extra={'spider': spider})


def add_failure_to_spider_stats(spider, request, response, reason, item=None):
    logger.error("Logged failure for %(url)r to scrapy-stats--failed_requests", {'url': request.url},
                 extra={'spider': spider})
    stats = spider.crawler.stats
    if stats.get_value('failed_requests') is None:
        stats.set_value('failed_requests', [])
    url = request.url
    if item and item.get("url", None):
        url = item.get("url")

    stats.get_value('failed_requests').append({'url': url,
                                               'status': response.status if response else 0,
                                               'retries': request.meta.get('retry_times', 0),
                                               'reason': reason,
                                               'meta': request.meta,
                                               'callback': request.callback,
                                               'item': item})


def add_duplicates_to_spider_stats(item, spider):
    logger.error("Logged failure for %(item)r", {'item': item},
                 extra={'spider': spider})
    spider.crawler.stats.inc_value('blazent/duplicate_items_count', spider=spider)
    if spider.crawler.stats.get_value('blazent/duplicate_items') is None:
        spider.crawler.stats.set_value('blazent/duplicate_items', [])
    spider.crawler.stats.get_value('blazent/duplicate_items').append(item)


def read_failures_from_spider_stats(spider):
    stats = spider.crawler.stats
    failed_requests = stats.get_value('failed_requests')
    field_headers = ['url', 'status', 'retries', 'reason', 'meta', 'callback', 'item']
    return field_headers, failed_requests
