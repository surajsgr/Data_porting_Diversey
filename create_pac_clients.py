import json
import requests


def pac_login():
    response = requests.post(url="https://dev-pac.diverseyiot.com/api/v1/auth/login",
                             json={"email": "admin@gmail.net", "password": "smartview"},
                             headers={'Content-Type': 'application/json'})
    print("response:", response.content)
    res = json.loads(response.content)
    return res['token']


if __name__ == "__main__":
    auth_token = pac_login()
    # from data_porting.settings import Settings

    # clients_json = json.loads(open("{}/data/clients_for_data_porting.json".format(Settings.BASE_DIR), "r").read())
    # clients = [client['CliName1'] for client in clients_json['data']]
    # for client in clients:
    response = requests.post(url="https://dev-pac.diverseyiot.com/api/v1/hierarchy",
                                 json={"org_name": "sahoo_hdkdj", "erp_number": "482dskdsd86", "address": "Switzerland",
                                       "org_type": "Client", "postal_code":"5135353", "parent_id": 1, "city": "Switzerland", "country_id": 2},
                                 headers={'Content-Type': 'application/json', 'Authorization': auth_token})
    print("response status:", response.status_code, response.content)
    result=json.loads(response.content)
    print(result)
    ids=result["data"]["id"]

    print(type(result))

    solution=requests.put(url="https://dev-pac.diverseyiot.com/api/v1/organization/solution-assign/{}".format(ids),json={"solutions":["SmartView"]},headers={'Content-Type': 'application/json', 'Authorization': auth_token})
    print(solution.content)