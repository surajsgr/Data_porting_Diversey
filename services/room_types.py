from uuid import uuid4
from pymysql.err import IntegrityError
import json
from datetime import datetime

class RoomTypeData(object):

    @staticmethod
    def write_room_type(mapping_json):
        from settings import Settings
        with open("{}/mapping/maintenance_date_chunked.json".format(Settings.BASE_DIR), 'w') as outfile:
            json.dump(mapping_json, outfile)

    @staticmethod
    def load_chunked_employee(kwargs):
        json_data = kwargs['data']
        date_tree = {}
        mo4jo_pac_clients_mapping = kwargs['mo4jo_pac_clients']

        for item in json_data:
            client_id = mo4jo_pac_clients_mapping[str(item['client_id'])]
            created_date = item['created_date']

            if created_date in date_tree.keys():
                date_tree[created_date].append({"id": item["id"], "phone": item["phone"], "client_id": client_id,
                                                "employee_number": item['employee_number'],
                                                "first_name": item['first_name'], "last_name": item['last_name']})
            else:
                # list=[{"client_id":client_id,"employee_number":item['PerNummer'],"first_name":item['PerFirstName'],"last_name":item['PerLastName']]
                date_tree[created_date] = []

                date_tree[created_date].append({"id": item["id"], "number": item["number"], "client_id": client_id,
                                                "denotation": item['denotation'],
                                                "description": item['description'], "last_name": item['last_name'],"addition":item['addition']})

        RoomTypeData.write_employee(date_tree)
        return date_tree

    @staticmethod
    def load_room_types(kwargs):

        from settings import Settings
        json_data = EmployeeData.load_chunked_employee(kwargs)
        conn = kwargs['conn']
        cursor = conn.cursor()
        mo4jo_pac_clients_mapping = kwargs['mo4jo_pac_clients']

        count_su = 0
        count_er = 0
        date = kwargs['start_date']
        start_date = datetime.strptime(date, '%Y-%m-%d').date()
        end_date = kwargs['end_date']
        batch_size = kwargs['batch_size']
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

        diff = (end_date - start_date)
        print(diff)
        duration = int(diff / timedelta(days=1))
        N = (duration // int(batch_size)) + 1
        initialize_logger('{}/logs/'.format(Settings.BASE_DIR))
        logging.info("Mo4Jo DB: {} ({})".format(Settings.DATABASE_CONFIG['mo4jo']['host'],Settings.DATABASE_CONFIG['mo4jo']['database']))
        logging.info("SmartView DB: {} ({})".format(Settings.DATABASE_CONFIG['tnt']['host'],Settings.DATABASE_CONFIG['tnt']['database']))

        logging.info("Migrating Persons from Mo4Jo to SmartView Employee from {} to {}".format(start_date, end_date))
        logging.info("Data needs to be ported for {} days\n".format(duration))
        for item in json_data:
            uuid = str(uuid4())

            client_id = item['client_id']
            try:
                cursor.execute("INSERT INTO room_types (id, client_id, number, denotation, addition, description, "
                                       "is_active, is_deleted, created_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);",
                                       (uuid, client_id, item['RomNummer'], item['RomBezeichnung'], item['RomZusatz'],
                                        item['RomBeschreibung'], item['RomActiv'], 0, item['RomDate']))
            except IntegrityError:
                print({'client_id': client_id, 'denotation': item['RomBezeichnung'], 'addition': item['RomZusatz'],
                       'number': item['RomNummer']})


        conn.commit()
        conn.close()
