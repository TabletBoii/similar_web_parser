from datetime import datetime
from urllib.parse import urlparse
from database.mysql_db import MySQLDB
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup


def get_user_agent_list(USER_AGENT_LIST_PATH: str) -> list[str]:
    user_agent_list = []
    with open(USER_AGENT_LIST_PATH) as file:
        for user_agent in file:
            user_agent_list.append(user_agent)

    return user_agent_list


def get_current_date() -> str:
    current_date: str = datetime.today()
    current_date: str = current_date.strftime('%Y-%m-%d %H:%M:%S')

    return current_date


def resource_list_mysql(mysql_instance: MySQLDB, order="DESC") -> list[tuple]:
    resource_list_query = f"""
                            SELECT RESOURCE_ID, RESOURCE_NAME, RESOURCE_URL FROM resource ORDER BY RESOURCE_ID {order}
                          """

    resource_list = mysql_instance.query_get_data(resource_list_query)

    return resource_list


def get_hostname(url: str) -> str:
    parsed_uri = urlparse(url)
    return '{uri.netloc}'.format(uri=parsed_uri)


def insert_country_ranks_to_database(mysql_instance: MySQLDB, resource_id: int, top: int, country_name: str, percent: float, status: str):
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


def insert_multiple_country_ranks_to_database(mysql_instance: MySQLDB, RESOURCE_ID: int, top_countries_list: tuple,
                                              status: str):
    insert_country_ranks_to_database(mysql_instance, RESOURCE_ID, 1, top_countries_list[0][0],
                                     top_countries_list[0][1],
                                     status)
    insert_country_ranks_to_database(mysql_instance, RESOURCE_ID, 2, top_countries_list[1][0],
                                     top_countries_list[1][1],
                                     status)
    insert_country_ranks_to_database(mysql_instance, RESOURCE_ID, 3, top_countries_list[2][0],
                                     top_countries_list[2][1],
                                     status)


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
    not_found_elements: list = page_str.find_all('p', class_='search-results__no-data-title')
    top_countries_list: list[tuple] = list()
    for i in range(len(top_country_values)):
        top_countries_list.append((top_country_names[i].getText(), top_country_values[i].getText()))

    return top_countries_list, not_found_elements
