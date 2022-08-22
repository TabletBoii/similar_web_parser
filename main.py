import random
from datetime import datetime
from time import sleep
from loguru import logger
from urllib.parse import urlparse

from database.mysql_db import MySQLDB
from config.mysql_config import mysql
from seleniumwire import webdriver
from selenium.webdriver import FirefoxOptions
from selenium.webdriver.common.by import By

from bs4 import BeautifulSoup

mysql_instance = MySQLDB()
mysql_instance.create_connection(mysql)

logger.add("similar_web_parser_out.log", format="{time} {level} {message}")

USER_AGENT_LIST_PATH = '/home/danyldo/Загрузки/whatismybrowser-user-agent-database.txt'


def get_user_agent_list() -> list[str]:
    user_agent_list = []
    with open(USER_AGENT_LIST_PATH) as f:
        for l in f:
            user_agent_list.append(l)

    return user_agent_list


def get_current_date() -> str:
    current_date: str = datetime.today()
    current_date: str = current_date.strftime('%Y-%m-%d %H:%M:%S')

    return current_date


def resource_list_mysql(order="DESC") -> list[tuple]:
    resource_list_query = f"""
                            SELECT RESOURCE_ID, RESOURCE_NAME, RESOURCE_URL FROM resource ORDER BY RESOURCE_ID {order}
                          """

    resource_list = mysql_instance.query_get_data(resource_list_query)

    return resource_list


def get_hostname(url: str) -> str:
    parsed_uri = urlparse(url)
    return '{uri.netloc}'.format(uri=parsed_uri)


def insert_country_ranks_to_database(resource_id, top, country_name, percent, status):
    check_for_duplicates_query: str = """SELECT resource_id FROM resource_top3_by_countries WHERE resource_id=%s AND 
                                                                                                  top=%s 
                                      """
    check_for_duplicates = mysql_instance.query_get_data(check_for_duplicates_query, (resource_id, top))
    if len(check_for_duplicates) != 0:
        ranks_update_query = """
                                UPDATE resource_top3_by_countries SET country_name=%s,
                                                          percent=%s, metric_status=%s
                                                      WHERE resource_id=%s AND top=%s
                             """
        mysql_instance.query_set_data(ranks_update_query, (country_name,
                                                           percent, status, resource_id, top))
    else:
        ranks_insert_query = """
                                INSERT INTO resource_top3_by_countries(resource_id,
                                                                       top,
                                                                       country_name,
                                                                       percent,
                                                                       metric_status)
                                       VALUES(%s, %s, %s, %s, %s)
                             """
        mysql_instance.query_set_data(ranks_insert_query, (resource_id,
                                                           top,
                                                           country_name,
                                                           percent,
                                                           status))


def remove_vestige(target_variable: str) -> float:
    chars_for_replace = {
        ' ': '',
        '- -': '',
        '--': '',
        '#': '',
        ',': '.',
        '<': '',
        '<=': '',
        '>': '',
        '>=': '',
        '%': ''
    }

    for key, value in chars_for_replace.items():
        if key == '#':
            target_variable = target_variable.replace(',', '')
        target_variable = target_variable.replace(key, value)
    if "K" in target_variable:
        target_variable = target_variable.replace("K", "")
        target_variable = float(target_variable)
        target_variable = target_variable * 1000
    elif "M" in target_variable:
        target_variable = target_variable.replace("M", "")
        target_variable = float(target_variable)
        target_variable = target_variable * 1000 * 1000
    elif "B" in target_variable:
        target_variable = target_variable.replace("B", "")
        target_variable = float(target_variable)
        target_variable = target_variable * 1000 * 1000 * 1000
    elif target_variable == '':
        target_variable = 0
    else:
        target_variable = float(target_variable)
    return target_variable


def select_element_by_top(browser_driver: webdriver, css_selector: str) -> tuple[str, str]:
    top_country: str = browser_driver.find_element(By.CSS_SELECTOR, css_selector).text

    top_country_list: list = top_country.split('\n')

    top_country_name: str = top_country_list[0]
    top_country_value: str = top_country_list[1]

    return top_country_name, top_country_value


def get_metrics_from_html(page_str: BeautifulSoup) -> tuple[list[str], list[str]]:
    top_country_names: list = page_str.find_all(['span', 'a'], class_='wa-geography__country-name')
    top_country_values: list = page_str.find_all('span', class_='wa-geography__country-traffic-value')
    top_countries_list: list[tuple] = list()
    for i in range(len(top_country_values)):
        top_countries_list.append((top_country_names[i].getText(), top_country_values[i].getText()))

    return top_countries_list


def parse_country_rank_info_by_html():
    resource_list = resource_list_mysql(order="ASC")
    incrementor = 1
    user_agent_list = get_user_agent_list()
    for i in range(600, len(resource_list)):
        RESOURCE_ID = resource_list[i][0]
        RESOURCE_NAME = resource_list[i][1]
        RESOURCE_URL = resource_list[i][2]
        options = webdriver.FirefoxOptions()
        profile = webdriver.FirefoxProfile()

        profile.set_preference("browser.cache.disk.enable", False)
        profile.set_preference("browser.cache.memory.enable", False)
        profile.set_preference("browser.cache.offline.enable", False)
        profile.set_preference("network.http.use-cache", False)

        current_user_agent = user_agent_list[random.randint(0, len(user_agent_list) - 1)]
        options.add_argument(f'user-agent={current_user_agent}')
        # options_seleniumWire = {
        #     "proxies": {
        #         'http': f'socks5://193.33.188.116:57221:CiEEqvL1:5bZTwAHZ',
        #         'https': f'sock5://193.33.188.116:57221:CiEEqvL1:5bZTwAHZ',
        #     }
        # }

        browser_driver = webdriver.Firefox(options=options,
                                           firefox_profile=profile)

        SIMILAR_WEB_QUERY = f'https://www.similarweb.com/ru/website/{get_hostname(RESOURCE_URL)}/#geography'
        browser_driver.delete_all_cookies()
        if incrementor % 4 == 0:
            incrementor = 0
            sleep(80)
        browser_driver.get(SIMILAR_WEB_QUERY)

        logger.info(f'Start parsing the resource {RESOURCE_NAME}')

        browser_driver.set_page_load_timeout(200)
        page_str = BeautifulSoup(browser_driver.page_source, features="html.parser")
        sleep(15)

        browser_driver.close()
        browser_driver.quit()
        sleep(random.randint(5, 11))
        was_page_banned = len(page_str.find_all('p', class_='wa-overview__title'))

        if was_page_banned == 0:
            logger.error("Page was banned")
            status = "banned"
            resource_current_status_query = """SELECT metric_status FROM resource_top3_by_countries WHERE 
            resource_id=%s """
            resource_current_status = mysql_instance.query_get_data(resource_current_status_query, RESOURCE_ID)
            sleep(80)
            if len(resource_current_status) == 0:
                insert_country_ranks_to_database(RESOURCE_ID, 1, '',
                                                 0, status)
                insert_country_ranks_to_database(RESOURCE_ID, 2, '',
                                                 0, status)
                insert_country_ranks_to_database(RESOURCE_ID, 3, '',
                                                 0, status)
                incrementor += 1
                continue
            else:
                incrementor += 1
                continue

        else:
            top_countries_list = get_metrics_from_html(page_str)

            element_selector_length = len(top_countries_list)

            match element_selector_length:
                case 0:
                    status = "no metrics"
                    insert_country_ranks_to_database(RESOURCE_ID, 1, '',
                                                     0, status)
                    insert_country_ranks_to_database(RESOURCE_ID, 2, '',
                                                     0, status)
                    insert_country_ranks_to_database(RESOURCE_ID, 3, '',
                                                     0, status)
                case 1:
                    status = "done"

                    insert_country_ranks_to_database(RESOURCE_ID, 1, top_countries_list[0][0],
                                                     remove_vestige(top_countries_list[0][1]), status)
                    insert_country_ranks_to_database(RESOURCE_ID, 2, '',
                                                     0, status)
                    insert_country_ranks_to_database(RESOURCE_ID, 3, '',
                                                     0, status)
                case 2:
                    status = "done"

                    insert_country_ranks_to_database(RESOURCE_ID, 1, top_countries_list[0][0],
                                                     remove_vestige(top_countries_list[0][1]), status)
                    insert_country_ranks_to_database(RESOURCE_ID, 2, top_countries_list[1][0],
                                                     remove_vestige(top_countries_list[1][1]), status)
                    insert_country_ranks_to_database(RESOURCE_ID, 3, '',
                                                     0, status)
                case 7:
                    status = "done"

                    insert_country_ranks_to_database(RESOURCE_ID, 1, top_countries_list[1][0],
                                                     remove_vestige(top_countries_list[1][1]), status)
                    insert_country_ranks_to_database(RESOURCE_ID, 2, top_countries_list[2][0],
                                                     remove_vestige(top_countries_list[2][1]), status)
                    insert_country_ranks_to_database(RESOURCE_ID, 3, top_countries_list[3][0],
                                                     remove_vestige(top_countries_list[3][1]), status)
                case _:
                    status = "done"

                    insert_country_ranks_to_database(RESOURCE_ID, 1, top_countries_list[0][0],
                                                     remove_vestige(top_countries_list[0][1]), status)
                    insert_country_ranks_to_database(RESOURCE_ID, 2, top_countries_list[1][0],
                                                     remove_vestige(top_countries_list[1][1]), status)
                    insert_country_ranks_to_database(RESOURCE_ID, 3, top_countries_list[2][0],
                                                     remove_vestige(top_countries_list[2][1]), status)
            logger.info(f'Resource {RESOURCE_NAME} has been successfully added. '
                        f'Number of elements to parse is {element_selector_length}')
            incrementor += 1


if __name__ == "__main__":
    parse_country_rank_info_by_html()
