import json
import random
import requests
# from data_porting.settings import Settings

token="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRhIjp7InV1aWQiOiJjZWQ5MDg2NS02MTY0LTQzNTAtOGViYi04NTdlODY5YjE5YzYiLCJpZCI6MzA3LCJ1c2VyX3JvbGVfaWQiOjIsIm9yZ19pZCI6MX0sImlhdCI6MTUzNTQ0ODU1NywiZXhwIjoxNTM1NTM0OTU3LCJpc3MiOiJEaXZlcnNleSBBZG1pbiJ9._PLVAMxXvXd_g35vTbT72msZa0RUJ-oGJc_yQlkja8c"
def get_customers_mapping(token):

	customers = {}
	res = requests.get("https://dev-pac.diverseyiot.com//api/v1/hierarchy/12360",
					   headers={'Content-Type': 'application/json', 'Authorization': token})
	clients = json.loads(res.content)['data'][0]['Client'];return clients



tree=get_customers_mapping(token)
all_customers={}
all_sites={}
for client in tree:
    # print(client['org_id'])
    # print({client['org_id']:client.get('Customer',[])})
    customers=client.get('Customer',[])

    for customer in customers:

        all_customers[customer['name']]=customer['id']
        sites=customer.get('Site',[])
        for site in sites:
            all_sites[site.get('name')]=site['id']
print(all_customers)
print(all_sites)



