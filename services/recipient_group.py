import json
from uuid import uuid4


class ReceipientGroupData(object):

    @staticmethod
    def write_receipient_group_mapping(table, mapping_json):
        from settings import Settings
        with open("{}/mapping/{}_mapping.json".format(Settings.BASE_DIR, table), 'w') as outfile:
            json.dump(mapping_json, outfile)



    @staticmethod
    def load_receipient_group(kwargs):
        from settings import Settings
        json_data = kwargs['data']
        conn = kwargs['conn']
        cursor = conn.cursor()
        recipient_group_mapping=dict()
        missing_recipient=dict()
        mo4jo_pac_clients_mapping = kwargs['mo4jo_pac_clients']
        recipient_json=json.loads(open("{}/mapping/recipient_mapping.json".format(Settings.BASE_DIR), "r").read())
        recipient_dict=dict()
        for key,values in recipient_json.items():
            recipient_dict[key]=values


        receipient_group_id_map_json = dict()
        for item in json_data:

            client_id = mo4jo_pac_clients_mapping[str(item['CliId'])]
            uuid = str(uuid4())
            recipient_group_mapping[item['id']]=uuid
            # print(recipient_group_mapping)

            # image = item['WarImage']
            # icon = MaintenanceTaskData.save_image(table, uuid, image) if image else None
            try:

                 cursor.execute(
                "INSERT INTO recipient_group (id, client_id, denotation, "
                " is_active, is_deleted, created_date) "
                "VALUES(%s, %s, %s, %s, %s, %s);",
                (uuid, client_id, item['Bezeichnung'],
                 'YES', 0, item['CDate']))
            except:
                print("{}-{}".format(item['id'],item['Bezeichnung']))
            # receipient_group_id_map_json[item['id']]=uuid

            if ';' not in item['Receiver'] and item['Receiver'] in recipient_dict.keys():
                email=item['Receiver']
                recipient = recipient_dict[email]

                cursor.execute("INSERT INTO recipient_mapping (recipient_group_id, recipient_id) VALUES (%s, %s);",
                   (uuid, recipient))
            elif ';' not in item['Receiver'] and item['Receiver'] not in recipient_dict.keys():
                missing_recipient[item['Receiver']]="Doesnot exist"


            else:
                user_email=item['Receiver'].split(';')

                for user in user_email:

                    if user in recipient_dict.keys():

                        recipient = recipient_dict[user]

                        cursor.execute("INSERT INTO recipient_mapping (recipient_group_id, recipient_id) VALUES (%s, %s);",
                                   (uuid, recipient))
                    else:
                        missing_recipient[user] = "Doesnot exist"




        conn.commit()
        conn.close()
        ReceipientGroupData.write_receipient_group_mapping('recipient_group', recipient_group_mapping)
