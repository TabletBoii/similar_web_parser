from abc import ABC

from database.interface.db_interface import DatabaseInterface
import clickhouse_driver


class ClickHouseDB(DatabaseInterface, ABC):

    def __init__(self):
        self.clickhouse_connector = None

    def __del__(self):
        self.clickhouse_connector.close()

    def create_connection(self, kwargs):
        try:
            self.clickhouse_connector = clickhouse_driver.connect(**kwargs)
            return self.clickhouse_connector
        except Exception as e:
            if self.CONSOLE:
                print('[ERROR] ' + str(e))
            exit(0)

    def query_get_data(self, query: str, params: tuple = ()) -> tuple:
        try:
            with self.clickhouse_connector.cursor() as cur:
                if params:
                    cur.execute(query, params)
                else:
                    cur.execute(query)
                result = cur.fetchall()
            return result
        except Exception as e:
            print(e)

    def query_set_data(self, query: str, params: tuple = ()):
        pass

    def query_set_multiple_data(self, query: str, params: list[tuple]):
        try:
            with self.clickhouse_connector.cursor() as cur:
                cur.executemany(query, params)
                self.clickhouse_connector.commit()
        except Exception as exc:
            raise RuntimeError('Stack insert error') from exc

    def query_delete_data(self, query: str, params: tuple = ()):
        try:
            with self.mysql_connector.cursor() as cur:
                cur.execute(query, params)
                self.mysql_connector.commit()
        except Exception as exc:
            raise RuntimeError('Delete error') from exc
