import json
from uuid import uuid4


class MaintenanceTypeData(object):

    @staticmethod
    def write_maintenance_type(mapping_json):
        from settings import Settings
        with open("{}/mapping/maintenance_date_chunked.json".format(Settings.BASE_DIR), 'w') as outfile:
            json.dump(mapping_json, outfile)

    @staticmethod
    def load_maintenance(kwargs):
        json_data = kwargs['data']
        date_tree = {}
        mo4jo_pac_clients_mapping = kwargs['mo4jo_pac_clients']

        for item in json_data:
            client_id = mo4jo_pac_clients_mapping[str(item['client_id'])]
            created_date = item['created_date']

            if created_date in date_tree.keys():
                date_tree[created_date].append({"id": item["id"], "number": item["number"], "client_id": client_id,"description": item['description'], "designation": item['designation'],"task_order": item['task_order'], "tasks": item["tasks"], "is_active": item["is_active"],"color":item["color"],"addition":item["addition"]})



            else:
                # list=[{"client_id":client_id,"employee_number":item['PerNummer'],"first_name":item['PerFirstName'],"last_name":item['PerLastName']]
                date_tree[created_date] = []

                date_tree[created_date].append({"id": item["id"], "number": item["number"], "client_id": client_id,
                                                "description": item['description'], "designation": item['designation'],
                                                "task_order": item['task_order'], "tasks": item["tasks"],
                                                "is_active": item["is_active"], "color": item["color"],
                                                "addition": item["addition"]})

        MaintenanceTypeData.write_maintenance_type(date_tree)
        return date_tree



    @staticmethod
    def load_maintenance_types(kwargs):
        from data_porting.settings import Settings
        table = kwargs['table']
        print(table)
        json_data = kwargs['data']
        conn = kwargs['conn']
        cursor = conn.cursor()
        mo4jo_pac_clients_mapping = kwargs['mo4jo_pac_clients']
        maintenance_tasks_json = json.loads(open("{}/mapping/maintenance_task_mapping.json".format(Settings.BASE_DIR), "r").read())
        maintenance_types_json = dict()
        mo4jo_pac_clients_mapping = kwargs['mo4jo_pac_clients']

        for item in json_data:
            task = maintenance_tasks_json[str(item['MaintenanceId'])]
            if item['Id'] not in maintenance_types_json:
                maintenance_types_json[item['Id']] = item
                maintenance_types_json[item['Id']]['task'] = [task]
            else:
                maintenance_types_json[item['Id']]['task'].append(task)






        for key, value in maintenance_types_json.items():
            uuid = str(uuid4())
            client_id = mo4jo_pac_clients_mapping[str(value['ClientId'])]['client_id']
            cursor.execute("INSERT INTO maintenance_type (id, client_id, number, designation, addition, description, "
                           "color, is_active, is_deleted, created_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                           (uuid, client_id, value['Number'], value['Designation'], value['Addition'],
                            value['Description'], value['Color'], 'YES' if value['Activ'] in ["1", 1, "YES"] else 'NO',
                            0, value['Date']))

            for task in set(value['task']):
               cursor.execute("INSERT INTO maintenance_task_mapping (maintenance_type_id, maintenance_task_id) VALUES (%s, %s);",
                               (uuid, task))

        conn.commit()
        conn.close()
