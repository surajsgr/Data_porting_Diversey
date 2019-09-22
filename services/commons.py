import glob
import json
import os

def get_last_version(tablename):
    from settings import Settings
    list_of_files = glob.glob('{}/data/{}*.json'.format(Settings.BASE_DIR, tablename))
    return max(list_of_files, key=os.path.getctime).split("_v")[-1].split('.json')[0] if list_of_files else 0

def get_clients_for_porting():
    from settings import Settings
    clients_json = json.loads(open("{}/data/clients_for_data_porting.json".format(Settings.BASE_DIR), "r").read())
    clients = tuple([client['CliId'] for client in clients_json['data']])
    return clients
