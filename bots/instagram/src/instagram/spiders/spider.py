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
    data_file = "/home/ubuntu/scrapper/data/instagram/instagram.csv"

    def __init__(self, *args, **kwargs):
        super(InstagramSpider, self).__init__(*args, **kwargs)
        self.driver = get_chrome_browser(True, 5)
        self.max_count = 10
        self.requests_processed = dict()
        self.loaded = dict()
        import csv
        with open(self.data_file, 'rb') as csv_file:
            reader = csv.reader(csv_file)
            header = reader.next()
            for row in reader:
                self.loaded[row[1]] = row
                # with open("/home/pryrnjn/workspace/sdlc/bots/instagram/instagram_20180126_070846.csv", 'wb') as csv_file:
                #     writer = csv.writer(csv_file)
                #     writer.writerow(header)
                #     for row in self.loaded.values():
                #         writer.writerow(row)

    def __del__(self):
        self.driver.quit()
        Spider.__del__(self)

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
        following_link = self.driver.find_elements_by_xpath('.//article[@class="_mesn5"]//ul/li')[
            2].find_element_by_tag_name('a')
        links_to_follow = []  # ["https://www.instagram.com/playmateiryna/"]
        # parsing following links
        try:
            click_element(self.driver, following_link)

            tries = 1
            loaded_divs = set()
            while True:
                divs = set(self.driver.find_elements_by_xpath('.//div[@class="_2nunc"]')) - loaded_divs
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
        for row in self.loaded.values():
            yield InstagramItem(user=row[0], link=row[1], posted_at=row[2], score=row[3])

        for link in links_to_follow:
            for item_or_request in self.parse_items(link):
                yield item_or_request

    def login(self):
        url = self.start_urls[0]
        username = "thegreatfollowr"
        password = "pr231158**"
        self.driver.get(url)

        login_link = self.driver.find_element_by_xpath('.//p[@class="_g9ean"]/a')
        scroll_to_element(self.driver, login_link)
        # if login_link.is_displayed():
        click_element(self.driver, login_link)
        login_form = self.driver.find_element_by_xpath('.//form[@class="_3jvtb"]')
        box_divs = login_form.find_elements_by_xpath('.//div[@class="_ev9xl"]')
        box_divs[0].find_element_by_xpath('input').send_keys(username)
        box_divs[1].find_element_by_xpath('input').send_keys(password)
        login_btn = login_form.find_element_by_xpath('.//button[@class="_qv64e _gexxb _4tgw8 _njrw0"]')
        scroll_to_element(self.driver, login_btn)
        click_element(self.driver, login_btn)
        self.dismiss_dialog_if_any()

    def dismiss_dialog_if_any(self):
        try:
            click_element(self.driver, self.driver.find_element_by_xpath('.//div[@role="dialog"]')
                          .find_element_by_xpath('button[@class="_dcj9f"]'))
        except:
            pass

    def parse_items(self, url):
        matched = re.match('(https://www.instagram.com/)(\\w+)(/)', url)
        if matched:
            user = matched.groups()[1]
            self.driver.get(url)
            try:
                name = self.driver.find_element_by_xpath('.//div[@class="_tb97a"]/h1').text
            except NoSuchElementException:
                name = ''
            num_posts = int(
                self.driver.find_element_by_xpath('.//article[@class="_mesn5"]//ul/li[1]/span/span').text.replace(",",
                                                                                                                  ""))
            num_posts = min(num_posts, self.max_count)
            loaded_links = set()
            tries = 1
            while num_posts > 0:
                links = set(self.driver.find_elements_by_xpath('.//a')) - loaded_links
                if len(links - loaded_links) == 0:
                    if tries > 3:
                        break
                    tries += 1

                loaded_links.update(links)
                for link_obj in links:
                    link = link_obj.get_attribute('href')
                    matched = re.match('https://www.instagram.com/p/\\w+/', link)
                    if matched and matched.group() not in self.loaded:
                        item = InstagramItem()
                        item['user'] = user
                        item['link'] = matched.group()

                        item["posted_at"] = self.get_posted_at_time(link_obj)
                        item['score'] = 10
                        num_posts -= 1
                        yield item

                try:
                    scroll_to_end(self.driver)
                    time.sleep(tries)
                except StaleElementReferenceException:
                    pass

    def get_posted_at_time(self, element):
        click_element_using_key(element)
        posted_at = self.driver.find_element_by_tag_name("time").get_attribute("datetime")
        self.dismiss_dialog_if_any()
        return posted_at
