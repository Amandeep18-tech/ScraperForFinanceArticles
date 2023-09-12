import re
import time
import random
import logging
import traceback

from pathlib import Path
from tempfile import mkdtemp
from selenium import webdriver
from dateutil.parser import parse
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService


import constants
from sqs_utils import SQSUtils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(Path(__file__).stem)


def scrollPage(driver: webdriver.Chrome, N=7):
    """ Scroll a page down continuously
    :param driver: A webdriver object, already intialized to the page you want to scroll
    :type driver: webdriver.Chrome
    :param N: The number of times to scroll down, default is 7
    :type N: int

    :return: None
    :rtype: None"""
    last_height = driver.execute_script("return document.body.scrollHeight") # Get scroll height
    n = 0
    while n < N:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);") # Scroll down to bottom
        time.sleep(random.choice(range(1, 3))) # Wait to load page
        new_height = driver.execute_script("return document.body.scrollHeight") # Calculate new scroll height and compare with last scroll height
        if new_height == last_height:
            break
        last_height = new_height
        n += 1
        
def get_chrome_driver():
    try:
        options = webdriver.ChromeOptions()
        service = webdriver.ChromeService(constants.chrome_driver_path)
        options.binary_location = constants.binary_path
        options.add_argument('--no-sandbox')
        options.add_argument('--headless')
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1280x1696")
        options.add_argument("--single-process")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-dev-tools")
        options.add_argument("--no-zygote")
        options.add_argument(f"--user-data-dir={mkdtemp()}")
        options.add_argument(f"--data-path={mkdtemp()}")
        options.add_argument(f"--disk-cache-dir={mkdtemp()}")
        options.add_argument("--remote-debugging-port=9222")
        driver = webdriver.Chrome(options=options, service=service)

    except Exception as e:
        logger.error("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
    return driver

def finex_daily_extractor() -> dict:
    """Extracts the latest news from Financial Express. Extracts only financial news.
    :return: A list of dictionaries with json
    :rtype: dict
    """
    driver = get_chrome_driver()
    driver.get("https://www.financialexpress.com/market/")
    scrollPage(driver)
    max_retries = 5 # maximum number of retries
    retry_count = 0 # Initialize the retry counter
    while retry_count < max_retries:
        try:
            elem = WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, '/html/body/div[4]/div[3]/div/div[1]')))  # Wait for the page to load
            break
        except Exception as e:
            logger.error("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
            retry_count+=1
    ls = elem.find_elements(By.TAG_NAME, "a") # Get all li elements with class = "newsList" which is already ul
    links = []
    for i in ls:
        try:
            link = i.get_attribute("href")
            links.append(link)
        except Exception as e:
            logger.error("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
            continue
    links = list(set(links))
    links = [each for each in links if (each.lower().startswith('https://www.financialexpress.com') or each.lower().startswith('http://www.financialexpress.com')) and (not each.lower().startswith('https://www.financialexpress.com/author/') and not each.lower().startswith('https://www.financialexpress.com/market/page/') and not each.lower().startswith('http://www.financialexpress.com/author/') and not each.lower().startswith('http://www.financialexpress.com/market/page/'))]
    logger.info('URLs extracted. Number of URLS = {0}'.format(len(links)))
    data = []
    for link in links:
        logger.info("Crawling from URL: {0}".format(link))
        try:
            driver.get(link)
            res = page_parsing(driver)
            if res and res not in data:
                unique_id="finex_{0}".format(int(time.time()))
                data_dict={
                            'originalUrl':link,
                            'url':link,
                            'harvestDate': res[0],
                            'publishedDate': res[0],
                            'estimatedPublishedDate': res[0],
                            'duplicateGroupId': unique_id,  # set this value as needed
                            'id': unique_id,  # set this value as needed
                            'languageCode': "en",
                            'title': res[1],
                            'content': res[2],
                            'topics': [{
                                        "name": "Finance latest",
                                        "group": "Finance"
                                        }],
                            'source': {
                                        "homeUrl": "https://www.financialexpress.com/market/"
                                        },
                            'originalName': res[3] ,
                            'stockTicker': "",
                            'AssetClass': "",
                            'NerMessage': ""
                }
                data.append(data_dict)
            else:
                import pdb; pdb.set_trace()
        except Exception as e:
            logger.error("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
            driver.close()
            driver=get_chrome_driver()
            driver.set_page_load_timeout(15)
    driver.close()
    return data

def page_parsing(driver):
    try:
        ls1, dattimez, title, author = "", None, None, None
        headline = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "wp-block-post-title")))
        headline = driver.find_element(By.CLASS_NAME, "wp-block-post-title")
        title = headline.text
        news = driver.find_element(By.CLASS_NAME, "pcl-container")
        ls = news.find_elements(By.TAG_NAME, "p")
        for elem in ls:
            try:
                r = elem.find_elements(By.TAG_NAME, "a")
                if len(r) != 0:
                    pass
                else:
                    ls1 = ls1 + (elem.text)
            except Exception as e:
                logger.error("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
                ls1 = ls1 + (elem.text)
        if ls1 and len(ls1) > 150:
            dats = driver.find_element(By.ID, "author-link")
            author = dats.text
            dattimez = driver.find_element(
                By.CLASS_NAME, "ie-network-post-meta-date").text
            dattimez = clean_datetime_string(dattimez)
    except Exception as e:
        logger.error("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
    return dattimez, title, ls1, author

def clean_datetime_string(input_string):
    """A function to clean the date strings with unwanted characters
    Keyword arguments:input_string
    Return: cleaned string
    """
    try:
        cleaned_string = re.sub(r'Updated:', '', input_string)  # Remove 'Updated:'
        cleaned_string=date_parse(cleaned_string.strip())
    except Exception as e:
        logger.error("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
    return cleaned_string

def date_parse(input_string):
    try:
        tzinfos = {"IST": 19800}
        x = parse(input_string, tzinfos=tzinfos)
        x = x.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logger.error("Error : {0}\nException : {1}".format(e, traceback.format_exc()))
    return x

def lambda_handler(event=None,context=None):
    news_list = finex_daily_extractor()
    if news_list:
        logger.info(news_list)
        sqs_util_object = SQSUtils(constants.sqs_queue_name)
        sqs_util_object.batch_send_to_sqs(news_list)
    else:
        logger.error("Failed to get news from Financial Express ")

if __name__ == '__main__':
    lambda_handler()
