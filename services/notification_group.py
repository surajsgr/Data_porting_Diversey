import json
from uuid import uuid4


class NotificationGroupData(object):



        @staticmethod
        def tree_maker(a=0):
            # {notid: {clintid:iiii,
            #         customerisd: {siteid: sitename}}}}
            tree_abstract = {}
            data= json.loads(open("{}/mapping/Notification_customer.json".format('/Users/suraj.sagar/PycharmProjects/utilities/data_porting'), "r").read())
            customer_json = json.loads(open("{}/data/customer_dev.json".format('/Users/suraj.sagar/PycharmProjects/utilities/data_porting'),"r").read())



            site_json=json.loads(open("{}/data/sites_dev.json".format('/Users/suraj.sagar/PycharmProjects/utilities/data_porting'), "r").read())

            for item in data:
                notification_id = "{}".format(item['Id'])
                client_id = item['ObjClientId']
                for i in customer_json.keys():
                    if item['ObjKd'].split('|')[-1][:10] in i:
                        customers=i
                        # print(item['ObjKd'])

                cust_id = str(customer_json.get(customers.split('|')[-1][:20],None))

                # print(type(cust_id))
                # object_id = item['ObjId']
                for i in site_json.keys():
                    if item['ObjName1'].split('|')[-1][:30] in i:
                        sites=i
                        # print(item['ObjName1'])
                    # else:
                    #     print(item['ObjName1'])
                print(sites)
                object_name = site_json.get(sites,None)
                if notification_id in tree_abstract.keys():
                    if cust_id in tree_abstract[notification_id].keys():

                          tree_abstract[notification_id][cust_id].append(object_name)
                          # (tree_abstract[notification_id][cust_id]))

                    else:
                        tree_abstract[notification_id][cust_id]=[]
                        tree_abstract[notification_id][cust_id].append(object_name)
                        # tree_abstract[notification_id][cust_id]))
                    # else:
                    # # tree_abstract[notification_id][cust_id]=[object_name]
                    #     tree_abstract[notification_id][client_id]={cust_id:[object_name]}
                else:
                    tree_abstract[notification_id]={cust_id:[object_name]}

                tree_abstract[notification_id][cust_id]=list(set(tree_abstract[notification_id][cust_id]))


                # print(tree_abstract[notification_id].keys())
                #         if cust_id in tree_abstract[notification_id][client_id].keys():
                #             if object_id in tree_abstract[notification_id][client_id][cust_id].keys():
                #                 pass
                #             else:
                #                 tree_abstract[notification_id][client_id][cust_id][object_id] = object_name
                #         else:
                #             tree_abstract[notification_id][client_id][cust_id] = {object_id: object_name}
                #     else:
                #         tree_abstract[notification_id][client_id] = {cust_id: {object_id: object_name}}
                # else:
                #     tree_abstract[notification_id] = {client_id: {cust_id: {object_id: object_name}}}

            return tree_abstract
        @staticmethod
        def load_notification_group(kwargs):
            from data_porting.settings import Settings
            json_data=kwargs['data']
            conn=kwargs['conn']
            cursor=conn.cursor()
            recipient_group_mapping = dict()
            mo4jo_pac_clients_mapping = kwargs['mo4jo_pac_clients']
            customer_json = json.loads(open("{}/data/customers.json".format('/Users/suraj.sagar/PycharmProjects/utilities/data_porting'),"r").read())
            # print(customer_json)


            recipient_json = json.loads(open("{}/mapping/recipient_group_mapping.json".format(Settings.BASE_DIR), "r").read())
            # print(recipient_json)


            customer_sites_json=json.loads(open("{}/mapping/notification_mapping.json".format(Settings.BASE_DIR), "r").read())



            for item in json_data:

               client_id = mo4jo_pac_clients_mapping[str(item["CliId"])]
               uuid = str(uuid4())
               denotation=item['Bezeichnung']
               description=item['Beschreibung']
               date=item['CDate']
               # try:
               customers_sites_json_id=customer_sites_json.get(str(item['Id']))
               # print(str(customers_sites_json_id))
               try:

                    cursor.execute(
                    "INSERT INTO notification_group (id, is_active, is_deleted, created_date, "
                    " client_id,customers_sites, denotation, description) "
                    "VALUES(%s, %s, %s, %s, %s, %s, %s, %s);",
                    (uuid, 'YES', 0, date, client_id,str(customers_sites_json_id), denotation, description))
               except Exception as e:
                   print(e)
                   print({item['Id']:denotation})

               event=[item['EvtBez4'],item['EvtBez3'],item['EvtBez2'],item['EvtBez1']]
               recipients_group=[item['ReceiverGrpId4'],item['ReceiverGrpId3'],item['ReceiverGrpId2'],item['ReceiverGrpId1']]



               for i in range(4):



                   if str(recipients_group[i]) in recipient_json.keys():
                        print(recipient_json.keys())

                        id=str(uuid4())

                        recipient=recipient_json[str(recipients_group[i])]
                        # print(recipient)
                        try:

                           cursor.execute(
                           "INSERT INTO notification_group_mapping (id, notification_group_id, event_id, recipient_id,is_used) VALUES ( %s, %s, %s, %s, %s);",
                                (id, uuid, event[i], recipient, 'YES'))
                        except Exception as e:
                            print(e)
                            print(item['Id'])
                   else:
                       print(recipients_group[i])
                       # print(recipient_json.keys())
            conn.commit()
            conn.close()

# print(NotificationGroupData.tree_maker())





