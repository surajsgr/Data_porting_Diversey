import json
import random
import requests
from data_porting.settings import Settings


def add_customers_for_clients():
    auth_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRhIjp7InV1aWQiOiIwMmVkODg4Yi03ODI1LTQ5ZjMtOWM5ZS1lNGNlMGUzYTdmM2MiLCJpZCI6MTMxLCJ1c2VyX3JvbGVfaWQiOjEsIm9yZ19pZCI6MSwiZW1haWwiOiJxYTVAZGl2ZXJzZXkuY29tIn0sImlhdCI6MTUzMjU4Njg2OSwiZXhwIjoxNTMyNjczMjY5LCJpc3MiOiJEaXZlcnNleSBBZG1pbiJ9.61rpjBdRIrK5dFFzlyZX6rzK1RmqrcAaVorjoUc07i8"
    clients = json.loads(open("{}/data/mo4jo_pac_clients_mapping.json".format(Settings.BASE_DIR), "r").read())
    customers_json = json.loads(open("{}/data/Customers.json".format(Settings.BASE_DIR), "r").read())
    failed_requests = []
    for customer_obj in customers_json:
        parent_id = clients[str(customer_obj['CusClientId'])]['node_id']
        json_data = {
            "org_name": customer_obj['CusName1'][:20],
            "erp_number": str(random.random())[:5],
            "city": customer_obj['CusCity'] if customer_obj['CusCity'] else "Dummy",
            "address": customer_obj['CusStreet'] if customer_obj['CusStreet'] else "Dummy",
            "postal_code": customer_obj['CusPostC'] if customer_obj['CusPostC'] else "000",
            "country_id": 1,
            "org_type": "Customer",
            "parent_id": parent_id
        }
        res = requests.post(url="https://portaladmin-qa.digital.diversey.com/api/portal-admin/v1/hierarchy/",
                            json=json_data, headers={"Content-Type": "application/json", "Authorization": auth_token})
        # print("status code:", cus tomer_obj['CusName1'], res.status_code, res.text)
        if res.status_code not in [200, 409]:
            failed_requests.append({'org_name': customer_obj['CusName1'][:20], 'error': res.text})
        elif res.status_code == 200:
            print("Customer successfully created: {}".format(customer_obj['CusName1']))

    print("failed requests:", failed_requests)


if __name__ == "__main__":
    print("Fetching Relevant Clients...")
    add_customers_for_clients()
