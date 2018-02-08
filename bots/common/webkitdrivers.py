from os.path import join, abspath, dirname

from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys

lib_path = abspath(join(dirname(__file__), "..", "..", "lib"))

CHROME_WEB_DRIVER_PATH = join("/home/scrapper/lib", "chromedriver")
PHANTOM_WEB_DRIVER_PATH = join(lib_path, "phantomjs-1.9.7-linux-x86_64/bin/phantomjs")


def get_chrome_browser(headless=True, implicit_wait_time=10):
    options = webdriver.ChromeOptions()

    # tell selenium to use the dev channel version of chrome
    # options.binary_location = '/usr/bin/google-chrome-stable'

    # set the window size
    # options.add_argument('window-size=1200x600')
    if headless:
        options.add_argument('headless')
    driver = webdriver.Chrome(CHROME_WEB_DRIVER_PATH, chrome_options=options)
    driver.implicitly_wait(implicit_wait_time)
    # driver.maximize_window()
    return driver


def get_mobile_browser():
    pass


def click_element(driver, element):
    ActionChains(driver).move_to_element(element).click().perform()


def click_element_using_key(element):
    element.send_keys(Keys.ENTER)


def scroll_to_element(driver, element):
    driver.execute_script("window.scrollTo(%d, %d);" % (element.location['x'], element.location['y']))


def scroll_to_element_using_key(element):
    element.send_keys(Keys.END)


def scroll_to_end(driver):
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")


def get_phantomjs_browser():
    return webdriver.PhantomJS(PHANTOM_WEB_DRIVER_PATH)
