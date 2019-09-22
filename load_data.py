from settings import Settings


class LoadData(object):

    def __init__(self, conn, table, json_data,start_date,end_date,batch_size):
        self.table = table
        self.conn = conn
        self.json_data = json_data
        self.start_date=start_date
        self.end_date=end_date
        self.entity_mapping = Settings.TABLES_MAPPING[self.table]
        self.batch_size=batch_size

    def load_data(self, mo4jo_pac_clients_mapping):
        kwargs = {'conn': self.conn, 'table': self.table, 'data': self.json_data,
                  'mo4jo_pac_clients': mo4jo_pac_clients_mapping,'start_date':self.start_date,'end_date':self.end_date,'batch_size':self.batch_size}
        data = self.entity_mapping['load_data_service'](kwargs)
