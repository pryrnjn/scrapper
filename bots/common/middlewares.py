from scrapy import signals
from scrapy.http import HtmlResponse
from scrapy.utils.python import to_bytes

from bots.common.webkitdrivers import get_chrome_browser
from configutil import *
from utils import *

logger = logging.getLogger(__name__)


class SeleniumMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        middleware = cls(crawler.settings)
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware

    def __init__(self, settings):
        self.wait_time = settings.getint('WAIT_FOR_ASYNC_CALLS')
        self.is_headless = settings.getbool('SELENIUM_DRIVER_HEADLESS', True)
        self.implicit_wait = settings.getint('SELENIUM_IMPLICIT_WAIT')
        self.driver = None

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn't have a response associated.
        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def process_request(self, request, spider):
        # request.meta['driver'] = self.driver  # to access driver from response
        # commenting out above because providing access to this can introduce behaviour
        self.driver.get(request.url)
        if self.wait_time > 0:
            time.sleep(self.wait_time)
        body = to_bytes(self.driver.page_source)  # body must be of type bytes
        return HtmlResponse(self.driver.current_url, body=body, encoding='utf-8', request=request)

    def spider_opened(self, spider):
        self.driver = get_chrome_browser(self.is_headless, self.implicit_wait)

    def spider_closed(self, spider):
        if self.driver:
            self.driver.quit()


class DownloaderErrorReporter(object):
    """This adds failures to spider-stats with key `failed_requests`"""

    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls(crawler.settings)
        crawler.signals.connect(middleware.request_dropped, signal=signals.request_dropped)
        return middleware

    def __init__(self, settings):
        self.settings = settings

    def process_exception(self, request, exception, spider):
        """when a download handler or a process_request() (from a downloader middleware) raises an exception (including
        an IgnoreRequest exception)"""
        add_failure_to_spider_stats(spider, request, None, exception)

    def request_dropped(self, request, spider):
        """when a Request, scheduled by the engine to be downloaded later, is rejected by the scheduler."""
        add_failure_to_spider_stats(spider, request, {}, "Request dropped by Scrapy Scheduler")

    def is_retry_exhausted(self, request):
        self.retry_enabled = self.settings.getbool('RETRY_ENABLED')
        self.max_retry_times = self.settings.getint('RETRY_TIMES')

        return self.retry_enabled \
               and not request.meta.get('dont_retry', False) \
               and request.meta.get('retry_times', 0) >= self.get_max_retry_times(request)

    def get_max_retry_times(self, request):
        return request.meta.get('max_retry_times') or self.max_retry_times


class SpiderErrorReporter(object):
    """This adds failures to spider-stats with key `failed_requests`"""

    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls(crawler.settings)
        crawler.signals.connect(middleware.handle_spider_exception, signal=signals.spider_error)
        crawler.signals.connect(middleware.handle_item_dropped, signal=signals.item_dropped)
        return middleware

    def __init__(self, settings):
        self.settings = settings
        self.exclude_dropped_items = self.settings.getbool("EXCLUDE_DROPPED_ITEM_IN_ERROR_REPORT")

    def process_spider_exception(self, response, exception, spider):
        """when a spider or process_spider_input() method (from other spider middleware) raises an exception"""
        add_failure_to_spider_stats(spider, response.request, response, exception)

    def handle_spider_exception(self, failure, response, spider):
        """when a spider callback generates an error (ie. raises an exception)."""
        add_failure_to_spider_stats(spider, response.request, response, failure)

    def handle_item_dropped(self, item, response, exception, spider):
        """when an item has been dropped from the Item Pipeline when some stage raised a DropItem exception"""
        if self.exclude_dropped_items:
            add_failure_to_spider_stats(spider, response.request, response, exception, item)


class ErrorCsvWriter(object):
    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls()
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware

    @staticmethod
    def spider_closed(spider):
        """Dumping to csv or taking any other action can be moved to a separate middleware implementation"""
        fieldnames, failures = read_failures_from_spider_stats(spider)
        if failures:
            file_path = get_error_file_path(spider)
            logger.debug("Writing error file to %(path)r", {'path': file_path}, extra={'spider': spider})
            write_to_csv_from_json(file_path, fieldnames, failures)


class ErrorReportMailer(object):
    """Placeholder for any such requirements.
    HOW TODO: Like in ErrorCsvWriter get `failed_requests` from spider stats and do the needful.
    See `ErrorCsvWriter.spider_closed"""
    pass


class MultipleCsvMerger(object):
    """This middleware merges multiple CSVs To use this middleware spider must define `files_to_be_merged` as a list
        (and populate it inside your spider) and `final_headers`
    """

    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls(crawler.settings)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware

    def __init__(self, settings):
        self.settings = settings

    def spider_closed(self, spider):
        """Dumping to csv or taking any other action can be moved to a separate middleware implementation"""
        if hasattr(spider, "files_to_be_merged") and hasattr(spider, "final_headers") \
                and spider.files_to_be_merged and spider.final_headers:
            final_file = get_data_file_path(spider, file_extension="csv")
            merge_multiple_csv(spider.files_to_be_merged, final_file, spider.final_headers, True)
        else:
            logger.error('Middleware ignored! {reason}'.format(reason="Either define `files_to_be_merged`, "
                                                                      "`final_headers` and provide valid values in "
                                                                      "your spider OR remove `MultipleCsvMerger` from "
                                                                      "your `settings.py`"))
