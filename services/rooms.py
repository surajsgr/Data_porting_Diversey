import ast
import json
import pickle
import requests
from uuid import uuid4
from pymysql.err import IntegrityError
from services.barcode_counter import BarCodeEntity


class RoomsData(object):

    @staticmethod
    def map_clients_customers_sites(room, mo4jo_pac_clients_mapping, mo4jo_pac_sites_mapping):
        room['client_id'] = mo4jo_pac_clients_mapping[str(room['client_id'])]['client_id']
        mapping = mo4jo_pac_sites_mapping.get(room['site_id'], None)
        if mapping:
            room['site_id'] = mapping['pac_site_id']
            room['customer_id'] = mapping['pac_customer_id']
        else:
            print("no mapping!")
            room['site_id'] = None
            room['customer_id'] = None
        return room

    @staticmethod
    def map_room_types(room, mo4jo_room_types, cursor):
        if room['room_type_id']:
            mapping = mo4jo_room_types.get(int(room['room_type_id']), None)
            if mapping:
                (denotation, addition) = (mapping['denotation'], mapping['addition'])
                cursor.execute("select * from room_types where client_id=%s and denotation=%s and addition=%s;",
                                        (room['client_id'], denotation, addition))
                row = cursor.fetchone()
                room['room_type_id'] = row[0]
        return room

    @staticmethod
    def map_cleaning_types(room, rooms_cleaning_types, unique_cleaning_types_json):
        associated_cleaning_types = []
        cleaning_types = rooms_cleaning_types.get(str(room['Id']), [])
        for cleaning_type in cleaning_types:
            if str(cleaning_type) in unique_cleaning_types_json:
                associated_cleaning_types.append(unique_cleaning_types_json[str(cleaning_type)]['tnt_id'])
        room['cleaning_types'] = associated_cleaning_types
        return room

    @staticmethod
    def map_maintenance_types(room, rooms_maintenance_types, unique_maintenance_types_json):
        room['maintenance_types'] = []
        maintenance_types = rooms_maintenance_types.get(str(room['Id']), [])
        for maintenance_type in maintenance_types:
            if str(maintenance_type) in unique_maintenance_types_json:
                room['maintenance_types'].append(unique_maintenance_types_json[str(maintenance_type)]['tnt_id'])
        return room

    @staticmethod
    def get_pac_customers_sites(token, mo4jo_pac_clients_mapping):
        customers_sites = {}
        for key,value in mo4jo_pac_clients_mapping.items():
            client_id = value['node_id']
            customers_sites[key] = {}
            res = requests.get("https://portaladmin-qa.digital.diversey.com/api/portal-admin/v1/hierarchy/{}".format(client_id),
                               headers={'Content-Type': 'application/json', 'Authorization': token})
            customers = json.loads(res.content)['data'][0].get('Customer', [])
            for customer in customers:
                for site in customer.get('Site', []):
                    customers_sites[key][site['name']] = {'site_id': site['site_id'], "node_id": site['id'],
                                                                        'customer_id': site['parent_id']}
        return customers_sites

    @staticmethod
    def load_rooms(kwargs):
        from data_porting.settings import Settings
        from data_porting.connection import Connection
        token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRhIjp7InV1aWQiOiIwMmVkODg4Yi03ODI1LTQ5ZjMtOWM5ZS1lNGNlMGUzYTdmM2MiLCJpZCI6MTMxLCJ1c2VyX3JvbGVfaWQiOjEsIm9yZ19pZCI6MSwiZW1haWwiOiJxYTVAZGl2ZXJzZXkuY29tIn0sImlhdCI6MTUzMjY4MjQyOCwiZXhwIjoxNTMyNzY4ODI4LCJpc3MiOiJEaXZlcnNleSBBZG1pbiJ9.0Gp8iF8rSrCxXB44EGKzjgFXQWs5_YI2ODjhj8Uinzg"
        table = kwargs['table']
        conn = kwargs['conn']
        cursor = conn.cursor()
        mo4jo_pac_clients_mapping = kwargs['mo4jo_pac_clients']
        temp_rooms = json.loads(json.loads(open("{}/data/temp_rooms.json".format(Settings.BASE_DIR), "r").read()))
        pac_customers_sites = RoomsData.get_pac_customers_sites(token, mo4jo_pac_clients_mapping)
        mo4jo_room_sites = ast.literal_eval(json.loads(open("{}/data/rooms_sites.json".format(Settings.BASE_DIR), "r").read()))
        mo4jo_pac_sites_mapping = {}

        for site in mo4jo_room_sites:
            site_client_id = str(site['ObjClientId'])
            default = list(pac_customers_sites[site_client_id].keys())
            if default:
                default = pac_customers_sites[site_client_id][default[0]]
                pac_site_id = pac_customers_sites[site_client_id].get(site['ObjName1'][:40], default)['node_id']
                pac_customer_id = pac_customers_sites[site_client_id].get(site['ObjName1'][:40], default)['customer_id']
                mo4jo_pac_sites_mapping[site['ObjId']] = {'pac_site_id': pac_site_id, 'pac_customer_id': pac_customer_id}

        temp_rooms = list(map(lambda room: RoomsData.map_clients_customers_sites(room, mo4jo_pac_clients_mapping, mo4jo_pac_sites_mapping), temp_rooms))

        # process room-types
        rooms_room_types = json.loads(json.loads(open("{}/data/rooms_room_types.json".format(Settings.BASE_DIR), "r").read()))
        mo4jo_room_types = {}
        for room_type in rooms_room_types:
            mo4jo_room_types[int(room_type['Id'])] = {'denotation': room_type['denotation'], 'addition': room_type['addition']}
        temp_rooms = list(map(lambda room: RoomsData.map_room_types(room, mo4jo_room_types, cursor), temp_rooms))

        # process cleaning-types
        rooms_cleaning_types = json.loads(open("{}/data/rooms_cleaning_types.json".format(Settings.BASE_DIR), 'r').read())
        unique_cleaning_types = json.loads(json.loads(open("{}/data/unique_cleaning_types.json".format(Settings.BASE_DIR), "r").read()))
        tnt_conn = Connection('tnt').connect_mysql()
        tnt_cursor = tnt_conn.cursor()
        unique_cleaning_types_json = {}
        for cleaning_type in unique_cleaning_types:
            client_id = mo4jo_pac_clients_mapping[str(cleaning_type['ClientId'])]['client_id']
            tnt_cursor.execute("select id from cleaning_type where designation=%s and addition=%s and client_id=%s",
                               (cleaning_type['Designation'], cleaning_type['Addition'], client_id))
            row = tnt_cursor.fetchone()
            if row:
                unique_cleaning_types_json[str(cleaning_type['Id'])] = {'designation': cleaning_type['Designation'],
                                                                              'addition': cleaning_type['Addition'],
                                                                              'client_id': cleaning_type['ClientId'],
                                                                              'tnt_id': row[0]}
        temp_rooms = list(map(lambda room: RoomsData.map_cleaning_types(room, rooms_cleaning_types,
                                                                        unique_cleaning_types_json), temp_rooms))

        # process maintenance-types
        rooms_maintenance_types = json.loads(open("{}/data/rooms_maintenance_types.json".format(Settings.BASE_DIR), 'r').read())
        unique_maintenance_types = json.loads(json.loads(open("{}/data/unique_maintenance_types.json".format(Settings.BASE_DIR), "r").read()))
        unique_maintenance_types_json = {}
        for maintenance_type in unique_maintenance_types:
            client_id = mo4jo_pac_clients_mapping[str(cleaning_type['ClientId'])]['client_id']
            tnt_cursor.execute("select id from maintenance_type where designation=%s and addition=%s and client_id=%s",
                               (cleaning_type['Designation'], cleaning_type['Addition'], client_id))
            row = tnt_cursor.fetchone()
            if row:
                unique_maintenance_types_json[str(maintenance_type['Id'])] = {'designation': maintenance_type['Designation'],
                                                                        'addition': maintenance_type['Addition'],
                                                                        'client_id': maintenance_type['ClientId'],
                                                                        'tnt_id': row[0]}
        temp_rooms = list(map(lambda room: RoomsData.map_maintenance_types(room, rooms_maintenance_types,
                                                                        unique_maintenance_types_json), temp_rooms))

        with open("{}/data/temp_rooms_2.json".format(Settings.BASE_DIR), 'w') as outfile:
            print("WRITING....")
            json.dump(temp_rooms, outfile)

        # insert data into Rooms Table
        print("Now Inserting Rooms Data into Database...")
        errors = []
        cursor.execute("select id from room_types where client_id=0;")
        default_room_type = cursor.fetchone()[0]
        for item in temp_rooms:
            uuid = str(uuid4())
            hex_code = ("%x" % item['Id']).upper()
            barcode = BarCodeEntity.get_barcode(BarCodeEntity.ROOM, hex_code)['barcode']
            item['cleaning_types'] = pickle.dumps(item['cleaning_types'])
            item['maintenance_types'] = pickle.dumps(item['maintenance_types'])
            item['room_type_id'] = default_room_type if item['room_type_id'] is None else item['room_type_id']
            print("item:", item)

            try:
                cursor.execute("INSERT INTO rooms (id, client_id, customer_id, site_id, number, room_number, denotation,"
                               "floor_level, room_type_id, cleaning_types, maintenance_types, height, square_meter, "
                               "floor_covering, window_square_meters, surface_material, wall_material, cleaning_frequency,"
                               "conditions, obstruction, cleaning_level, cost_unit, district, key_list, access, check_cleaning, "
                               "heating_devices, luminous_elements, air_condition, power_outlet, fire_detector, water_available, "
                               "security, plants, barcode, is_active, is_deleted, created_date) "
                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                                (uuid, item['client_id'], item['customer_id'], item['site_id'], item['number'],
                                 item['room_number'], item['denotation'], item['floor_level'], item['room_type_id'],
                                 item['cleaning_types'], item['maintenance_types'], item['height'], item['square_meter'],
                                 item['floor_covering'], item['window_square_meters'], item['surface_material'],
                                 item['wall_material'], item['cleaning_frequency'], item['conditions'], item['obstruction'],
                                 item['cleaning_level'], item['cost_unit'], item['district'], item['key_list'], item['access'],
                                 item['check_cleaning'], item['heating_devices'], item['luminous_elements'], item['air_condition'],
                                 item['power_outlet'], item['fire_detector'], item['water_available'], item['security'],
                                 item['plants'], barcode, 'YES' if item['is_active'] == True else 'NO', 0, item['created_date']))
            except IntegrityError as e:
                # print("error:", e)
                errors.append(item['Id'])
                # print({'client_id': item['client_id'], 'denotation': item['denotation'], 'number': item['number'],
                #        'room_number': item['room_number']})

        conn.commit()
        conn.close()
        print("errors:", errors)
