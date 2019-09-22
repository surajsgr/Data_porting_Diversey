import json
from uuid import uuid4
from pymysql.err import IntegrityError


class CleaningTypeData(object):

    @staticmethod
    def load_cleaning_types(kwargs):
        from data_porting.settings import Settings
        json_data = kwargs['data']
        conn = kwargs['conn']
        cursor = conn.cursor()
        cleaning_tasks_json = json.loads(open("{}/mapping/cleaning_task_mapping.json".format(Settings.BASE_DIR), "r").read())
        cleaning_types_json = dict()
        mo4jo_pac_clients_mapping = kwargs['mo4jo_pac_clients']
        for item in json_data:
            task = cleaning_tasks_json[str(item['tasks'])]
            if item['id'] not in cleaning_types_json:
                cleaning_types_json[item['id']] = item
                cleaning_types_json[item['id']]['tasks'] = [task]
            else:
                cleaning_types_json[item['id']]['tasks'].append(task)

        for key, value in cleaning_types_json.items():
            uuid = str(uuid4())

            client_id = mo4jo_pac_clients_mapping[str(value['client_id'])]["client_id"]

            try:
                cursor.execute("INSERT INTO cleaning_type (id, client_id, type_number, designation, addition, description, "
                                   "color, is_active, is_deleted, created_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                                   (uuid, client_id, value['type_number'], value['designation'], value['addition'],
                                    value['description'], value['color'],
                                    'YES' if value['is_active'] in ["1", "YES"] else 'NO', 0, value['created_date']))

                for task in set(value['tasks']):
                    cursor.execute("INSERT INTO task (cleaning_type_id, cleaning_task_id) VALUES (%s, %s);", (uuid, task))
            except IntegrityError as e:
                print({"client_id": client_id, "type_number": value['type_number'], 'designation': value['designation'],
                       'addition': value['addition']})

        conn.commit()
        conn.close()
