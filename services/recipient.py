import json
from uuid import uuid4
from datetime import datetime
from datetime import timedelta
import pymysql
from logger_initialiser import initialize_logger
import logging


class ReceipientData(object):

    @staticmethod
    def write_recipient(mapping_json):
        from settings import Settings
        with open("{}/mapping/recipient_date_chunked.json".format(Settings.BASE_DIR), 'w') as outfile:
            json.dump(mapping_json, outfile)

    @staticmethod
    def load_recipient(kwargs):
        json_data = kwargs['data']
        date_tree = {}
        mo4jo_pac_clients_mapping = kwargs['mo4jo_pac_clients']

        for item in json_data:
            client_id = item['client_id']
            created_date = item['created_date']

            if created_date in date_tree.keys():
                date_tree[created_date].append({"id": item["id"], "email": item["email"], "client_id": client_id,
                     "is_active": item["is_active"]})



            else:
                # list=[{"client_id":client_id,"employee_number":item['PerNummer'],"first_name":item['PerFirstName'],"last_name":item['PerLastName']]
                date_tree[created_date] = []

                date_tree[created_date].append({"id": item["id"], "email": item["email"], "client_id": client_id,"is_active": item["is_active"]})



        ReceipientData.write_recipient(date_tree)
        return date_tree


    @staticmethod
    def write_receipient_group_mapping(table, mapping_json):
        from settings import Settings
        with open("{}/mapping/{}_mapping.json".format(Settings.BASE_DIR, table), 'w') as outfile:
            json.dump(mapping_json, outfile)

    @staticmethod
    def load_receipient(kwargs):
        from data_porting.settings import Settings
        json_data = ReceipientData.load_recipient(kwargs)
        conn = kwargs['conn']
        cursor = conn.cursor()
        receipient_group_id_map_json=dict()
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
        logging.info("Migrating Users from Mo4Jo to SmartView Employee from {} to {}".format(start_date, end_date))
        logging.info("Data needs to be ported for {} days\n".format(duration))
        for i in range(N):
           success_count=0
           error_count=0

           for k,v in json_data.items():

              create_date=datetime.strptime(k,'%Y-%m-%d').date()

              for item in v:
                    if duration>=int(batch_size):

                       end_date_new = start_date+timedelta(days=int(batch_size))


                    else:
                        end_date_new = start_date+timedelta(days=duration)

                    if create_date>=start_date and create_date<=end_date_new:
                        uuid = str(uuid4())
                        client_id=item['client_id']
                        cursor.execute(
                        "INSERT INTO recipient (id, client_id, email, "
                        " user_id, is_active, is_deleted, created_date) "
                        "VALUES(%s, %s, %s, %s, %s, %s, %s);",
                        (uuid, client_id, item['email'], 299,
                         'YES' if item['is_active'] in ["1", 1, "YES"] else 'NO', 0, create_date))
                        receipient_group_id_map_json[item['email']] = uuid
                        logging.info("Migrated ID {} for {} ".format(item['id'], item['email']))
                        success_count+=1
           if end_date_new < end_date:
               # print("\n")

               logging.info("Migrated Recipient from Mo4Jo to SmartView Employee from {} to {} ".format(start_date, end_date_new))

               logging.info("Total {} records has been imported with {} error\n".format(success_count, error_count))
           elif end_date_new > end_date:

               logging.info("Migrated Recipient from Mo4Jo to SmartView Employee from {} to {} ".format(start_date, end_date))

               logging.info("Total {} records has been imported with {} error\n".format(success_count, error_count))
               break

           start_date = end_date_new + timedelta(days=1)
           duration = duration - int(batch_size) if duration > int(batch_size) else int(batch_size) - duration


        conn.commit()
        conn.close()

        ReceipientData.write_receipient_group_mapping('recipient', receipient_group_id_map_json)
