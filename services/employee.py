from uuid import uuid4

from datetime import datetime


from services.barcode_counter import BarCodeEntity
import json
from datetime import timedelta
import pymysql
from logger_initialiser import initialize_logger
import logging
import logging


class EmployeeData(object):

    @staticmethod
    def write_employee( mapping_json):
        from settings import Settings
        with open("{}/mapping/maintenance_date_chunked.json".format(Settings.BASE_DIR), 'w') as outfile:
            json.dump(mapping_json, outfile)


    @staticmethod
    def load_chunked_employee(kwargs):
        json_data=kwargs['data']
        date_tree={}
        mo4jo_pac_clients_mapping = kwargs['mo4jo_pac_clients']


        for item in json_data:
            client_id = mo4jo_pac_clients_mapping[str(item['client_id'])]
            created_date = item['created_date']

            if created_date  in date_tree.keys():
                date_tree[created_date].append({"id":item["id"],"phone":item["phone"],"client_id":client_id,"employee_number":item['employee_number'],"first_name":item['first_name'],"last_name":item['last_name']})
            else:
                # list=[{"client_id":client_id,"employee_number":item['PerNummer'],"first_name":item['PerFirstName'],"last_name":item['PerLastName']]
                date_tree[created_date]=[]

                date_tree[created_date].append({"id":item["id"],"phone":item["phone"],"client_id":client_id,"employee_number":item['employee_number'],"first_name":item['first_name'],"last_name":item['last_name']})

        EmployeeData.write_employee(date_tree)
        return date_tree



    @staticmethod
    def load_employees(kwargs):
        from settings import Settings
        json_data = EmployeeData.load_chunked_employee(kwargs)
        conn = kwargs['conn']
        cursor = conn.cursor()
        mo4jo_pac_clients_mapping = kwargs['mo4jo_pac_clients']


        count_su=0
        count_er=0
        date = kwargs['start_date']
        start_date=datetime.strptime(date,'%Y-%m-%d').date()
        end_date=kwargs['end_date']
        batch_size = kwargs['batch_size']
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()


        diff = (end_date-start_date)
        print(diff)
        duration=int(diff/timedelta(days=1))
        N=(duration // int(batch_size))+1
        initialize_logger('{}/logs/'.format(Settings.BASE_DIR))
        logging.info("Mo4Jo DB: {} ({})".format(Settings.DATABASE_CONFIG['mo4jo']['host'], Settings.DATABASE_CONFIG['mo4jo']['database']))
        logging.info("SmartView DB: {} ({})".format(Settings.DATABASE_CONFIG['tnt']['host'], Settings.DATABASE_CONFIG['tnt']['database']))
        logging.info("Migrating Persons from Mo4Jo to SmartView Employee from {} to {}".format(start_date, end_date))
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
                        employee_number=item.get('employee_number',None)
                        first_name=item.get('first_name',None)
                        last_name=item.get('last_name',None)
                        client_id = item['client_id']
                        hex_code = ("%x" % item['id']).upper()
                        barcode = BarCodeEntity.get_barcode(BarCodeEntity.PERSONAL_EMPLOYEE, hex_code)['barcode']
                        try:

                           cursor.execute("INSERT INTO employee (id, client_id, employee_number, first_name, last_name, phone, "
                           "language, barcode, is_active, is_deleted, created_date)"
                           " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                           (uuid, client_id, employee_number, first_name, last_name,
                            item['phone'], 'de-DE', barcode, 'YES', 0, create_date))
                           success_count+=1
                           logging.info("Migrated ID {} for {} ({}, {})".format(item['id'], 'Employee', first_name, last_name))

                        except pymysql.err.IntegrityError as err:
                            logging.error((err))
                            logging.error(("Error : ", str(item['id']) + ' ' + first_name + last_name))
                            error_count+=1


           if end_date_new<end_date:
                  print("\n")

                  logging.info("Migrated Person from Mo4Jo to SmartView Employee from {} to {} ".format(start_date,end_date_new))
                  logging.info("Total {} records has been imported with {} error".format(success_count,error_count))
           elif end_date_new>end_date:

                  logging.info("Migrated Person from Mo4Jo to SmartView Employee from {} to {} ".format(start_date,end_date))
                  logging.info("Total {} records has been imported with {} error".format(success_count,error_count))
                  break

           start_date = end_date_new + timedelta(days=1)
           duration = duration - int(batch_size) if duration > int(batch_size) else int(batch_size) - duration


        conn.commit()
        conn.close()

                 # duration = int((end_date_new-start_date)/timedelta(days=1)) if int(batch_size) < duration else end_date-start_date




           # if start_date < end_date:
           #
           #
           #     # print(duration)
           #     # logging.info("Migrated Persons from Mo4Jo to SmartView Employee from {} to {} ".format(start_date ,start_date+timedelta(days=int(batch_size) if int(batch_size) < duration else duration-1) if int(batch_size) < duration else end_date ))
           #     if int(batch_size) < duration:
           #       logging.info("Migrated Persons from Mo4Jo to SmartView Employee from {} to {} ".format(start_date,start_date+timedelta(days=(end_date_new-start_date)/timedelta(days=1))))
           #       logging.info("Total {} Records has been imported, with {} errors\n".format(success_count, error_count))
           #     elif int(batch_size) > duration:
           #
           #       print(duration)
           #       logging.info("Migrated Persons from Mo4Jo to SmartView Employee from {} to {} ".format(start_date,start_date+timedelta(days=(end_date-start_date)/timedelta(days=1))))
           #       logging.info("Total {} Records has been imported, with {} errors\n".format(success_count,error_count))
           #     else:
           #
           #       logging.info("Migrated Persons from Mo4Jo to SmartView Employee from {} to {} ".format(start_date, end_date))
           #
           #       logging.info("Total {} Records has been imported, with {} errors\n".format(success_count, error_count))
           #
           #
           # elif start_date == end_date:
           #       logging.info("Migrated Persons from Mo4Jo to SmartView Employee from {} to {} ".format(start_date,end_date))
           # else:
           #     break
           #
           #
           #
           #
           #
           #
           #
           # # logging.info('Records remain to import for {} days\n'.format(duration))
           # # duration = duration - int(batch_size) if duration > int(batch_size) else duration





