import json
import pandas as pd
from settings import Settings
from services.commons import get_last_version
from services.commons import get_clients_for_porting
import base64
from datetime import datetime



class PullData(object):

    def __init__(self, conn, table, file_path):
        self.conn = conn
        self.table = table
        self.file_path = file_path
        print(self.file_path)

        self.entity_mapping = Settings.TABLES_MAPPING[self.table]
        self.entity_dict = []
        self.fields = self.entity_mapping['fields']


    def create_json_file(self, row):

        entity_row = {value: row[key] for key,value in self.fields.items() if self.fields.keys()!='WarImage'}
        if self.table == 'maintenance_task' :
            entity_row['barcode'] = '*' + ('T0' + str(("%x" % row['WarId']).upper()).zfill(5)) + '*'
            entity_row['secondliner_barcode'] = '*' + ('S' + str(("%x" % row['WarId']).upper()).zfill(6)) + '*'
            entity_row['secondliner_done_barcode'] = '*' + ('U' + str(("%x" % row['WarId']).upper()).zfill(6)) + '*'
            # print(type(row['WarImage']))
            # entity_row['icon']=row['WarImage'].decode() if row['WarImage'] else ''
            entity_row['icon'] = base64.b64encode(row['WarImage']).decode()  if isinstance(row['WarImage'],bytes) else ''


        elif self.table=='cleaning_task':
            entity_row['barcode'] = '*' + ('T0' + str(("%x" % row['OpeId']).upper()).zfill(5)) + '*'
            entity_row['secondliner_barcode'] = '*' + ('S' + str(("%x" % row['OpeId']).upper()).zfill(6)) + '*'
            entity_row['secondliner_done_barcode'] = '*' + ('U' + str(("%x" % row['OpeId']).upper()).zfill(6)) + '*'
            # print(type(row['WarImage']))
            # entity_row['icon']=row['WarImage'].decode() if row['WarImage'] else ''
            entity_row['icon'] = base64.b64encode(row['OpeImage']).decode() if isinstance(row['OpeImage'],bytes) else ''



            # entity_row['icon']=base64.b64decode(row['WarImage']) if row['WarImage'] else ''
            # if isinstance(row['WarImage'],bytes):
            #     entity_row['icon']=base64.b64encode(row['WarImage']).decode()
            #     print(type(entity_row['icon']))


        entity_row['is_active'] = "YES" if entity_row['is_active'] == '1' else 'NO'
        entity_row['is_active'] = "YES" if entity_row['is_active'] in ['1', 1, 'YES'] else 'NO'
        self.entity_dict.append(entity_row)


    def get_data(self):
        entity_mapping = Settings.TABLES_MAPPING[self.table]
        if self.conn:
            count = entity_mapping['count_query']
            clients = get_clients_for_porting()
            # start_date = kwargs['start_date']
            # end_date = kwargs['end_date']
            count = count.format(clients)
            count_df= pd.read_sql(count,self.conn)
            total_count=count_df['count'][0]


            window_size = Settings.DATABASE_CONFIG['tnt']['window_size']  # or whatever limit you like

            window_idx = 0
            N=(total_count//int(window_size))+1


            for i in range(N):
                start, stop = int(window_size) * window_idx, int(window_size)
                print(start)
                query = entity_mapping['select_query']
                query = query.format(clients,start,stop)
                print(query)
                entity_df = pd.read_sql(query, self.conn)
                entity_df[list(self.fields.keys())].copy()
                entity_df.apply(lambda row: self.create_json_file(row), axis=1)
                window_idx+=1

            with open("{}/data/{}_v{}.json".format(Settings.BASE_DIR, self.table,
                                                   int(get_last_version(self.table)) + 1), 'w') as outfile:
                print("Writing json to file...")
                print(self.entity_dict)

                json.dump(self.entity_dict, outfile)
            return self.entity_dict

        return json.loads(open(self.file_path).read())
