import utilities
import random
from time import sleep
from loguru import logger
from database.mysql_db import MySQLDB
from config.mysql_config import mysql
from seleniumwire import webdriver
from bs4 import BeautifulSoup

mysql_instance: MySQLDB = MySQLDB()
mysql_instance.create_connection(mysql)

logger.add("similar_web_parser_out.log", format="{time} {level} {message}")

USER_AGENT_LIST_PATH: str = '/home/danyldo/Загрузки/whatismybrowser-user-agent-database.txt'


def main():
    resource_list: list[tuple] = utilities.resource_list_mysql(mysql_instance, order="ASC")

    user_agent_list = utilities.get_user_agent_list(USER_AGENT_LIST_PATH)
    for i in range(839, len(resource_list)):
        RESOURCE_ID: int = resource_list[i][0]
        RESOURCE_NAME: str = resource_list[i][1]
        RESOURCE_URL: str = resource_list[i][2]
        options: webdriver.FirefoxOptions = webdriver.FirefoxOptions()
        profile: webdriver.FirefoxProfile = webdriver.FirefoxProfile()

        profile.set_preference("browser.cache.disk.enable", False)
        profile.set_preference("browser.cache.memory.enable", False)
        profile.set_preference("browser.cache.offline.enable", False)
        profile.set_preference("network.http.use-cache", False)

        current_user_agent: str = user_agent_list[random.randint(0, len(user_agent_list) - 1)]
        options.add_argument(f'user-agent={current_user_agent}')

        browser_driver: webdriver.Firefox = webdriver.Firefox(options=options,
                                                              firefox_profile=profile)

        SIMILAR_WEB_QUERY: str = f'https://www.similarweb.com/ru/website/{utilities.get_hostname(RESOURCE_URL)}/#geography'
        browser_driver.delete_all_cookies()

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
            resource_current_status: list[tuple] = mysql_instance.query_get_data(resource_current_status_query, RESOURCE_ID)
            sleep(80)
            if len(resource_current_status) == 0:
                utilities.insert_country_ranks_to_database(RESOURCE_ID, 1, '',
                                                           0, status)
                utilities.insert_country_ranks_to_database(RESOURCE_ID, 2, '',
                                                           0, status)
                utilities.insert_country_ranks_to_database(RESOURCE_ID, 3, '',
                                                           0, status)
                continue
            else:
                continue

        else:
            top_countries_list = utilities.get_metrics_from_html(page_str)
            top_countries_list = top_countries_list[-3:]

            element_selector_length = len(top_countries_list)

            match element_selector_length:
                case 0:
                    status = "no metrics"
                    top_countries_list = (['', 0], ['', 0], ['', 0])
                    utilities.insert_multiple_country_ranks_to_database(mysql_instance, RESOURCE_ID, top_countries_list,
                                                                        status)
                case 1:
                    status = "done"

                    top_countries_list = ([top_countries_list[0][0], utilities.remove_vestige(top_countries_list[0][1])], ['', 0], ['', 0])

                    utilities.insert_multiple_country_ranks_to_database(mysql_instance, RESOURCE_ID, top_countries_list,
                                                                        status)
                case 2:
                    status = "done"

                    top_countries_list = ([top_countries_list[0][0], utilities.remove_vestige(top_countries_list[0][1])],
                                          [top_countries_list[1][0], utilities.remove_vestige(top_countries_list[1][1])], ['', 0])

                    utilities.insert_multiple_country_ranks_to_database(mysql_instance, RESOURCE_ID, top_countries_list,
                                                                        status)
                case _:
                    status = "done"

                    utilities.insert_multiple_country_ranks_to_database(mysql_instance, RESOURCE_ID, top_countries_list,
                                                                        status)
            logger.info(f'Resource {RESOURCE_NAME} has been successfully added. '
                        f'Number of elements to parse is {element_selector_length}')


if __name__ == "__main__":
    main()
