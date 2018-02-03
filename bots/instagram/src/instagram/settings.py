
BOT_NAME = 'instagram'

SPIDER_MODULES = ['instagram.spiders']
NEWSPIDER_MODULE = 'instagram.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'instagram (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

SPIDER_MIDDLEWARES = {
    'bots.common.middlewares.SpiderErrorReporter': 898,
    'bots.common.middlewares.ErrorCsvWriter': 899
}

EXCLUDE_DROPPED_ITEM_IN_ERROR_REPORT = True
# SELENIUM_DRIVER_HEADLESS = False
# SELENIUM_IMPLICIT_WAIT = 10

#'bots.common.middlewares.SeleniumMiddleware': 543,
DOWNLOADER_MIDDLEWARES = {
    'bots.common.middlewares.DownloaderErrorReporter': 899
}

ITEM_PIPELINES = {
    'bots.common.pipelines.DuplicateItemFilter': 400,
    'bots.common.pipelines.CsvWriterPipeline': 600
}
