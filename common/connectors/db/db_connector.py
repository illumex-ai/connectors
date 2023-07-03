from common.connectors.connector import Connector


class DBConnector(Connector):
    def __init__(self, connection):
        super().__init__(connection)

    @property
    def databases(self):
        return self.__datbases

    @databases.setter
    def databases(self, value):
        self.__datbases = value
