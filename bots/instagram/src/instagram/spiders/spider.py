import re
import time

from instagram.items import InstagramItem
from scrapy.spiders import Spider
from selenium.common.exceptions import *

from bots.common.webkitdrivers import *


class InstagramSpider(Spider):
    name = "instagram"

    start_urls = [
        'https://www.instagram.com'
    ]

    HEADERS = ['user', 'link', 'posted_at', 'score']
    UNIQUE_KEY = ['link']
    data_dir = "/home/ubuntu/scrapper/data/instagram/"
    data_file = data_dir + "instagram.csv"

    def __init__(self, *args, **kwargs):
        super(InstagramSpider, self).__init__(*args, **kwargs)
        self.driver = get_chrome_browser(True, 5)
        self.max_count = 10
        self.requests_processed = dict()
        self.loaded = dict()
        import csv
        with open(self.data_dir + 'visited.csv', 'rb') as csv_file:
            reader = csv.reader(csv_file)
            for row in reader:
                self.loaded[row[0]] = row[1]

    def if_request_processed(self, url):
        # TODO: this only validates based on the url-hash, doesn't take care of other parameters of a Request,
        # needs to be taken care. See scrapy.utils.request.request_fingerprint for reference
        if url in self.requests_processed.keys():
            return self.requests_processed.get(url)
        else:
            self.requests_processed[url] = False
            return False

    def parse(self, response):
        self.login()
        self.driver.get("https://www.instagram.com/thegreatfollowr/")
        following_link = self.driver.find_element_by_xpath(".//a[@href='/thegreatfollowr/following/']")
        links_to_follow = []  # ["https://www.instagram.com/playmateiryna/"]
        # parsing following links
        try:
            click_element(self.driver, following_link)

            tries = 1
            loaded_divs = set()
            while True:
                divs = set(self.driver.find_elements_by_class_name("FsskP")) - loaded_divs
                if len(divs) == 0:
                    if tries > 3:
                        break
                    tries += 1
                loaded_divs.update(divs)
                for div in divs:
                    try:
                        links_to_follow.append(response.urljoin(div.find_element_by_xpath('a').get_attribute('href')))
                    except:
                        pass
                scroll_to_element_using_key(div.find_element_by_xpath('a'))
                time.sleep(tries)
        except StaleElementReferenceException:
            pass
        finally:
            self.dismiss_dialog_if_any()

        for link in links_to_follow:
            for item_or_request in self.parse_items(link):
                yield item_or_request

    def login(self):
        url = "https://www.instagram.com/accounts/login/"  # self.start_urls[0]
        username = "thegreatfollowr"
        password = "pr231158**"
        self.driver.get(url)
        self.driver.find_element_by_name("username").send_keys(username)
        self.driver.find_element_by_name("password").send_keys(password)

        login_btn = self.driver.find_element_by_class_name("_5f5mN")
        scroll_to_element(self.driver, login_btn)
        click_element(self.driver, login_btn)
        self.dismiss_dialog_if_any()

    def dismiss_dialog_if_any(self):
        try:
            click_element(self.driver, self.driver.find_element_by_xpath('.//div[@role="dialog"]')
                          .find_element_by_xpath('button[@class="ckWGn"]'))
        except:
            pass

    def parse_items(self, url):
        try:
            matched = re.match('(https://www.instagram.com/)(\\w+)(/)', url)
            if matched:
                user = matched.groups()[1]
                self.driver.get(url)
                # try:
                #     name = self.driver.find_element_by_xpath('.//div[@class="_tb97a"]/h1').text
                # except NoSuchElementException:
                #     name = ''
                num_posts = int(
                    self.driver.find_elements_by_class_name("g47SY")[0].text.replace(
                        ",",
                        ""))
                num_posts = min(num_posts, self.max_count)
                loaded_links = set()
                tries = 1
                while num_posts > 0:
                    links = self.driver.find_elements_by_xpath(".//div[@class='v1Nh3 kIKUG  _bz0w']/a")
                    # check for first post, if already scraped
                    if self.is_already_scraped(links[0], user):
                        break
                    links = set(links) - loaded_links
                    if len(links - loaded_links) == 0:
                        if tries > 3:
                            break
                        tries += 1

                    loaded_links.update(links)
                    for link_obj in links:
                        link = link_obj.get_attribute('href')
                        matched = re.match('https://www.instagram.com/p/\\w+/', link)
                        if matched:
                            num_posts -= 1
                            posted_at = self.get_posted_at_time(link_obj)
                            if posted_at > self.loaded.get(user, ''):
                                item = InstagramItem()
                                item['user'] = user
                                item['link'] = matched.group()
                                item["posted_at"] = posted_at
                                item['score'] = 10
                                yield item

                    scroll_to_end(self.driver)
                    time.sleep(tries)
        except StaleElementReferenceException:
            print "StaleElementReferenceException. url::", url
        except NoSuchElementException:
            print "NoSuchElementException. url::", url
        except Exception:
            print "Exception. url::", url

    def get_posted_at_time(self, element):
        posted_at = ''
        try:
            click_element_using_key(element)
            posted_at = self.driver.find_element_by_tag_name("time").get_attribute("datetime")
        except:
            pass
        finally:
            self.dismiss_dialog_if_any()
        return posted_at

    def is_already_scraped(self, link_obj, user):
        link = link_obj.get_attribute('href')
        matched = re.match('https://www.instagram.com/p/\\w+/', link)
        if matched:
            posted_at = self.get_posted_at_time(link_obj)
            if posted_at <= self.loaded.get(user, ''):
                return True
        return False
