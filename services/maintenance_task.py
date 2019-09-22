import os
import json
from uuid import uuid4
import ast
import base64
from datetime import timedelta
from datetime import datetime
from logger_initialiser import initialize_logger
import logging


from data_porting.services.barcode_counter import BarCodeEntity

class MaintenanceTaskData(object):


    @staticmethod
    def write_maintenance_task( mapping_json):
        from settings import Settings
        with open("{}/mapping/maintenance_date_chunked.json".format(Settings.BASE_DIR), 'w') as outfile:
            json.dump(mapping_json, outfile)


    @staticmethod
    def load_maintenance(kwargs):
        json_data=kwargs['data']
        date_tree={}
        mo4jo_pac_clients_mapping = kwargs['mo4jo_pac_clients']


        for item in json_data:
            client_id = mo4jo_pac_clients_mapping[str(item['client_id'])]
            created_date = item['created_date']

            if created_date  in date_tree.keys():
                date_tree[created_date].append({"id":item["id"],"task_number":item["task_number"],"client_id":client_id,"denotation":item['denotation'],"description":item['description'],"task_order":item['task_order'],"icon":item["icon"],"is_active":item["is_active"]})
            else:
                # list=[{"client_id":client_id,"employee_number":item['PerNummer'],"first_name":item['PerFirstName'],"last_name":item['PerLastName']]
                date_tree[created_date]=[]

                date_tree[created_date].append( {"id": item["id"], "task_number": item["task_number"], "client_id": client_id,"denotation": item['denotation'], "description": item['description'],"task_order": item['task_order'], "icon": item["icon"], "is_active": item["is_active"]})




        MaintenanceTaskData.write_maintenance_task(date_tree)
        return date_tree


    # @staticmethod
    # def save_image(table, uuid, image):
    #     from data_porting.settings import Settings
    #     # full_path = "{}/data/media/images/maintenance_task/{}".format(Settings.BASE_DIR, uuid)
    #     full_path = "/Users/suraj.sagar/PycharmProjects/utilities/data_porting/data/media/images/maintenance_task/".format(uuid)
    #
    #     with open(full_path, 'w') as f:
    #         f.write(image)
    #     file_ext = Settings.IMAGE_EXTENSIONS_MAPPING.get(image[:3], 'jpg')
    #     os.system("cat {} | xxd -r -p > {}.{}".format(full_path, full_path, file_ext))
    #     os.remove(full_path)
    #     icon = "media/images/{}/{}.{}".format(table, uuid, file_ext)
    #     return icon

    @staticmethod
    def write_maintenance_tasks_mapping(table, mapping_json):
        from data_porting.settings import Settings
        with open("{}/mapping/{}_mapping.json".format(Settings.BASE_DIR, table), 'w') as outfile:
            json.dump(mapping_json, outfile)

    @staticmethod
    def load_maintenance_tasks(kwargs):
        from settings import Settings
        table = kwargs['table']
        json_data = MaintenanceTaskData.load_maintenance(kwargs)
        conn = kwargs['conn']
        cursor = conn.cursor()
        mo4jo_pac_clients_mapping = kwargs['mo4jo_pac_clients']

        maintenance_tasks_id_map_json = dict()
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

        logging.info("Migrating Maintenance from Mo4Jo to SmartView Maintenance_task from {} to {}".format(start_date, end_date))

        logging.info("Data needs to be ported for {} days\n".format(duration))
        for i in range(N):
                success_count = 0
                error_count = 0

                for k, v in json_data.items():

                    create_date = datetime.strptime(k, '%Y-%m-%d').date()

                    for item in v:
                        if duration >= int(batch_size):

                            end_date_new = start_date + timedelta(days=int(batch_size))


                        else:
                            end_date_new = start_date + timedelta(days=duration)

                        if create_date >= start_date and create_date <= end_date_new:

                            uuid = str(uuid4())
                            image =base64.b64decode(item['icon'])
                            hex_code = ("%x" % item['id']).upper()
                            client_id  =item['client_id']
                            barcodes = BarCodeEntity.get_barcode(BarCodeEntity.MAINTENANCE, hex_code)
                            (maintenance_barcode, maintenance_done_barcode) = (barcodes['maintenance_barcode'], barcodes['maintenance_done_barcode'])


                            # file_ext = Settings.IMAGE_EXTENSIONS_MAPPING.get(image[:3], 'jpg')
                            with open('/Users/suraj.sagar/PycharmProjects/utilities/data_porting/data/media/images/maintenance_task/{}.jpg'.format(uuid),'wb') as im:
                                  im.write(image)


                        # icon = MaintenanceTaskData.save_image(table, uuid, image) if image else None
                            icon="media/images/{}/{}.jpg".format(table, uuid)
                            description = item.get('description', None)
                            task_order = item.get('task_order', None)
                            task_order = task_order if task_order.isdigit() else None
                            cursor.execute(
                            "INSERT INTO maintenance_task (id, client_id, task_number, denotation, description, "
                            " task_order, icon, is_active, is_deleted, created_date, maintenance_barcode, maintenance_done_barcode) "
                            "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                            (uuid, client_id, item.get('task_number', None), item['denotation'],

                            description, task_order, icon,item.get('is_active'), 0, create_date,maintenance_barcode,maintenance_done_barcode))

                            success_count+=1
                            logging.info("Migrated ID {} for {} ({})".format(item['id'], 'Maintenance', description))


                            maintenance_tasks_id_map_json[item['id']] = uuid
                if end_date_new < end_date:
                    # print("\n")
                    logging.info("Migrated Maintenance from Mo4Jo to SmartView Employee from {} to {} ".format(start_date,                                                                                                          end_date_new))
                    logging.info("Total {} records has been imported with {} error\n".format(success_count, error_count))
                elif end_date_new > end_date:
                    # print("\n")
                    logging.info("Migrated Maintenance from Mo4Jo to SmartView Employee from {} to {} ".format(start_date, end_date))
                    logging.info("Total {} records has been imported with {} error\n".format(success_count, error_count))
                    break

                start_date = end_date_new + timedelta(days=1)
                duration = duration - int(batch_size) if duration > int(batch_size) else int(batch_size) - duration

        conn.commit()
        conn.close()


        MaintenanceTaskData.write_maintenance_tasks_mapping('maintenance_task', maintenance_tasks_id_map_json)
