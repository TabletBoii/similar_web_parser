from abc import ABC

import pymysql

from database.interface.db_interface import DatabaseInterface


class MySQLDB(DatabaseInterface, ABC):

    def __init__(self):
        self.mysql_connector = None

    def __del__(self):
        self.mysql_connector.close()

    def create_connection(self, db_config):
        try:
            self.mysql_connector = pymysql.connect(**db_config)
            return self.mysql_connector
        except pymysql.err.OperationalError as e:
            if self.CONSOLE:
                print('[ERROR] ' + str(e))
            exit(0)

    def query_get_data(self, query: str, params: tuple = ()) -> tuple:
        try:
            with self.mysql_connector.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchall()
                self.mysql_connector.commit()
        except Exception as exc:
            raise RuntimeError('Select error') from exc

        return result

    def query_set_data(self, query: str, params: tuple = ()):
        _id = 0

        try:
            with self.mysql_connector.cursor() as cur:
                cur.execute(query, params)
                self.mysql_connector.commit()
                _id = int(cur.lastrowid)
        except Exception as exc:
            raise RuntimeError('Single insert error') from exc

        return _id

    def query_set_multiple_data(self, query: str, params: list[tuple]):
        try:
            with self.mysql_connector.cursor() as cur:
                cur.executemany(query, params)
                self.mysql_connector.commit()
        except Exception as exc:
            raise RuntimeError('Stack insert error') from exc

    def query_delete_data(self, query: str, params: tuple = ()):
        try:
            with self.mysql_connector.cursor() as cur:
                cur.execute(query, params)
                self.mysql_connector.commit()
        except Exception as exc:
            raise RuntimeError('Delete error') from exc
