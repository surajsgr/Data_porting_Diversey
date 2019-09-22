import json
from argparse import ArgumentParser
from connection import Connection
from pull_data import PullData
from load_data import LoadData
from settings import Settings
from services.commons import get_last_version
from datetime import datetime




def main(tablename, use_json=0, database='tnt' ,start_date=0,end_date=0,batch_size=30):
    try:
        last_version = get_last_version(tablename)
        file_path = "{}/data/{}_v{}.json".format(Settings.BASE_DIR, tablename, last_version) if use_json == 0 else use_json
        source_conn = Connection('mo4jo').connect_mssql() if use_json is None else None

        target_conn = Connection(database).connect_mysql()

        start_date=start_date if start_date!=None else '2012-01-01'
        batch_size=Settings.DATABASE_CONFIG['tnt']['batch_size']
        now=datetime.utcnow().date()

        mo4jo_pac_clients_mapping = json.loads(open("{}/data/mo4jo_pac_clients_mapping.json".format(Settings.BASE_DIR), "r").read())
        end_date = end_date if end_date != None else now
        # entity_json = PullData(source_conn, tablename, file_path).get_data(kwargs={'start_date':start_date,'end_date':end_date})
        entity_json = PullData(source_conn, tablename, file_path).get_data()
        if type(end_date)=='datetime.date':
            end_date=datetime.strptime(end_date,'%Y-%m-%d').date()


        LoadData(target_conn, tablename, entity_json, start_date, end_date, batch_size).load_data(mo4jo_pac_clients_mapping)
        # LoadData(target_conn, tablename, entity_json).load_data(mo4jo_pac_clients_mapping)
    except Exception as e:
        import traceback
        print(traceback.format_exc())

parser = ArgumentParser()
parser.add_argument("-t", "--table", dest="tablename", help="Provide name of table for which data is to be imported")
parser.add_argument("-db", "--target_db", dest="database", nargs='?', default="tnt", help="Provide name of database in which data is to be ported")
parser.add_argument("-e", "--environment", dest="env", help="Provide type of environment in which to port the data")
parser.add_argument("-use_json", "--use_json", dest="use_json", nargs='?', const=0,
                    help="True/False if you want to use saved json or fetch data from remote server")
parser.add_argument("-std", "--start_date", dest="start_date", nargs='?', const=0,
                    help="True/False if you want to use saved json or fetch data from remote server")
parser.add_argument("-ed", "--end_date", dest="end_date", nargs='?', const=0,
                    help="True/False if you want to use saved json or fetch data from remote server")
args = vars(parser.parse_args())
main(args['tablename'], args['use_json'], args['database'],args['start_date'],args['end_date'])

