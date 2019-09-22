import json
import random
import requests
from data_porting.settings import Settings


def get_customers_mapping(token):
	customers = {}
	res = requests.get("https://portaladmin-qa.digital.diversey.com/api/portal-admin/v1/hierarchy/17",
					   headers={'Content-Type': 'application/json', 'Authorization': token})
	clients = json.loads(res.content)['data'][0]['Client']
	mo4jo_clients = ["Firma Gottlieb AG2", "Oettli's Allroundse", "Reiwag2", "Hägni AG Reinigunsun", "REIWAG Rail Services",
					 "ISS Deutschland2", "Hellrein Reinigungs", "Simacek Facilitysro", "HELIOS Kliniken", "Diversey España2",
					 "Hellrein Wiener", "Interserve", "Interserve Spain", "Hellrein BBG", "Bouygues CH", "Dussmann Hungary",
					 "Haubis GmbH", "GateGournetFR", "Skaraborgs Städ", "Bouygues UK", "APEX Taski Switzer", "Consulting Switzer",
					 "Bouygues ES FS Sch", "City Kent Clean UK", "McDonalds Resturnt", "REIWAG Facility Ser", "Hôpital eHnv",
					 "Servis s.r.o.", "INTEGRAL SERVICE srl", "Spitalregion Rhein", "Dussmann Lithuania"]

	for client in clients:
		client_name = client['name']
		print("client_name:", client_name)
		if client_name in mo4jo_clients:
			client_customers = client.get('Customer', [])
			customers[client_name] = {}
			for customer in client_customers:
				customers[client_name][customer['name']] = customer['id']
	return customers


def add_sites_for_customers(token):
	sites_json = json.loads(open("{}/data/site.json".format(Settings.BASE_DIR), "r").read())
	customers_json = json.loads(open("{}/data/mo4jo_pac_customers_mapping.json".format(Settings.BASE_DIR), "r").read())
	# invalid = []
	failed_requests = []
	for site in sites_json:
		parent_id = customers_json[site['ObjMandant']].get(site['ObjKd'].split('|')[1][:20].strip(), None)

		if parent_id:

			json_data = {
				"erp_number": str(random.random())[:5],
				"site_name": site['ObjName1'][:40],
				"address": site['ObjStreet'] if site['ObjStreet'] else 'Dummy',
				"city": site['ObjCity'] if site['ObjCity'] else 'Dummy',
				"postal_code": 'Dummy',
				"country_id": 1,
				"gps_location": {"latitude": site['ObjLat'], "longitude": site['ObjLon']},
				"parent_id": parent_id,
				"site_type": "Simple"
			}



			res = requests.post(url="https://portaladmin-qa.digital.diversey.com/api/portal-admin/v1/site/",
								json=json_data, headers={"Content-Type": "application/json", "Authorization": token})
			print("status code:", site['ObjName1'], res.text)
			if res.status_code not in [200, 409]:
				failed_requests.append({'site_name': site['ObjName1'], 'response': res.text})
			elif res.status_code == 200:
				print("Site Created Successfully")
		else:
			print("parent id is None for {}".format(site['ObjName1']))
	print("failed:", failed_requests)



if __name__ == "__main__":
	token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRhIjp7InV1aWQiOiI4ODllMDRiNi0yYTk5LTRlZDctYTdjNy0xNjA0ZGYzZGE2NzAiLCJpZCI6MTEsInVzZXJfcm9sZV9pZCI6Miwib3JnX2lkIjoxfSwiaWF0IjoxNTMzNjMzNTQ2LCJleHAiOjE1MzM3MTk5NDYsImlzcyI6IkRpdmVyc2V5IEFkbWluIn0.pzGBcX0gMaabXu9dq4uWesJzG28oAnFmqyLRpmUs26M"
	# Note: Uncomment next 3 lines if want new mapping of customers
	# customers_mapping = get_customers_mapping(token)
	# with open("{}/data/mo4jo_pac_customers_mapping.json".format(Settings.BASE_DIR), "w") as outfile:
	# 	json.dump(customers_mapping, outfile)

	add_sites_for_customers(token)
