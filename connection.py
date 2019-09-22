import pymssql
import pymysql
from settings import Settings


class Connection(object):

    def __init__(self, conn_type):
        self.conn_config = Settings.DATABASE_CONFIG[conn_type]
        self.host = self.conn_config['host']
        self.user = self.conn_config['user']
        self.password = self.conn_config['password']
        self.port = int(self.conn_config['port'])
        self.database = self.conn_config['database']

    def connect_mssql(self):
        conn = pymssql.connect(server=self.host, user=self.user, password=self.password, port=self.port)
        return conn

    def connect_mysql(self):
        conn = pymysql.connect(host=self.host, user=self.user, password=self.password, port=self.port,
                               database=self.database, charset='utf8mb4')
        return conn
