import abc


class DatabaseInterface(abc.ABC):

    def __init__(self):
        self.CONSOLE = True

    @abc.abstractmethod
    def create_connection(self, **kwargs):
        pass

    @abc.abstractmethod
    def query_get_data(self, query: str, params: tuple = ()) -> tuple:
        pass

    @abc.abstractmethod
    def query_set_data(self, query: str, params: tuple = ()):
        pass

    @abc.abstractmethod
    def query_set_multiple_data(self, query: str, params: list[tuple]):
        pass

    @abc.abstractmethod
    def query_delete_data(self, query: str, params):
        pass

