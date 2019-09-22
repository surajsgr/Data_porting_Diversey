# Data_porting_Diversey


Data Porting Process
----

#### Port Clients

   - can be done by script and assign to smartview solution
   
#### Port customers and sites for that clients

   - can be done by script and assign to smartview solution
   
####Port Users

#### Port Employee
   
   - Create a json with create_date
   - Add the chunk size in settings.py
   - Specify the date range
   -command-(python3 main.py -t [table_name] -use_json [json_file] -std [start_date] -ed [ end_date ])
   
#### Port Cleaning Task
   
   - Create a json with create_date
   - Add the chunk size in settings.py
   - Specify the date range
   - write script to map cleaning_task with uuid and store it to a file
   - command-(python3 main.py -t [table_name] -use_json [json_file] -std [start_date] -ed [ end_date ])
   
#### Port Cleaning Task
   
   - Create a json with create_date
   - Add the chunk size in settings.py
   - Specify the date range
   - write script to map cleaning_task with uuid and store it to a file
   - command-(python3 main.py -t [table_name] -use_json [json_file] -std [start_date] -ed [ end_date ])

#### Port Cleaning Type
   
   - Create a json with create_date
   - Add the chunk size in settings.py
   - Specify the date range
   - command-(python3 main.py -t [table_name] -use_json [json_file] -std [start_date] -ed [ end_date ])
   
#### Port Maintenance Task
   
   - Create a json with create_date
   - Add the chunk size in settings.py
   - Specify the date range
   - write script to map maintenance_task with uuid and store it to a file
   - command-(python3 main.py -t [table_name] -use_json [json_file] -std [start_date] -ed [ end_date ])
   
#### Port Maintenance Type
   
   - Create a json with create_date
   - Add the chunk size in settings.py
   - Specify the date range
   - command-(python3 main.py -t [table_name] -use_json [json_file] -std [start_date] -ed [ end_date ])
   
#### Port Recipient 
   
   - Create a json with create_date
   - Add the chunk size in settings.py
   - Specify the date range
   - write script to map recipient data with uuid and store it to a file
   - command-(python3 main.py -t [table_name] -use_json [json_file] -std [start_date] -ed [ end_date ])

#### Port Recipient_Group
   
   - Create a json with create_date
   - Add the chunk size in settings.py
   - Specify the date range
   - write script to map recipient_group data with uuid and store it to a file
   - command-(python3 main.py -t [table_name] -use_json [json_file] -std [start_date] -ed [ end_date ])
 
#### Port Notification_Group
   
   - Create a json with create_date
   - Create a json by writing script to map notification_id with customers_sites
   - Add the chunk size in settings.py
   - Specify the date range
   - write script to map recipient_group data with uuid and store it to a file
   - command-(python3 main.py -t [table_name] -use_json [json_file] -std [start_date] -ed [ end_date ])



Issues:
----

- No Users found for following emails: 

To Do:
----

- [ ] Client specific records
- [ ] use batch_size
- [ ] Compress Images <optional>
- [ ] Loggging
- [ ] .gitignore
- [ ] Library to return extension/format of image using hexdump
- [ ] Add data using requests to application (disable pac/token)
