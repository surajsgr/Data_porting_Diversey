import os
from services.cleaning_task import CleaningTaskData
from services.cleaning_type import CleaningTypeData
from services.maintenance_task import MaintenanceTaskData
from services.maintenance_type import MaintenanceTypeData
from services.room_types import RoomTypeData
from services.employee import EmployeeData

from services.recipient import ReceipientData
from services.recipient_group import ReceipientGroupData
from services.notification_group import NotificationGroupData

from services.rooms import RoomsData



class Settings(object):
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    DATABASE_CONFIG = {
        'mo4jo': {
            'host': os.environ['MO4JO_DB_HOST'],
            'user': os.environ['MO4JO_DB_USERNAME'],
            'password': os.environ['MO4JO_DB_PASSWORD'],
            'port': os.environ['MO4JO_DB_PORT'],
            'database': os.environ['MO4JO_DB_NAME']
        },
        'tnt': {
            'host': os.environ['MYSQL_DB_HOST'],
            'user': os.environ['MYSQL_DB_USERNAME'],
            'password': os.environ['MYSQL_DB_PASSWORD'],
            'port': os.environ['MYSQL_DB_PORT'],
            'database': os.environ['MYSQL_DB_NAME'],
            'batch_size': os.environ['BATCH_SIZE'],
            'window_size':os.environ['WINDOW_SIZE']
        },
        'rooms': {
            'host': os.environ['ROOMS_DB_HOST'],
            'user': os.environ['ROOMS_DB_USERNAME'],
            'password': os.environ['ROOMS_DB_PASSWORD'],
            'port': os.environ['ROOMS_DB_PORT'],
            'database': os.environ['ROOMS_DB_NAME']

        }
    }

    TABLES_MAPPING = {
        "cleaning_task": {
            "mo4jo_table_name": "Operations",
            "fields": {
                "OpeId": "id",
                "OpeClientId": "client_id",
                "OpeNummer": "task_number",
                "OpeBezeichnung": "denotation",
                "OpeBeschreibung": "description",
                "OpeOrder": "task_order",
                "OpeImage": "icon",
                "OpeActiv": "is_active",
                "OpeDate": "created_date"
            },
            "select_query": "select * from Operations where OpeClientId IN {};",
            "client_field": "OpeClientId",
            "load_data_service": CleaningTaskData.load_cleaning_tasks,
            "count_query": "select count(*) as count from Operations where OpeClientId in {}"
        },
        "cleaning_type": {
            "mo4jo_table_name": "CleaningTypes",
            "fields": {
                "CleaningTypeId": "id",
                "Date": "created_date",
                "Activ": "is_active",
                "ClientId": "client_id",
                "Number": "type_number",
                "Designation": "designation",
                "Addition": "addition",
                "Description": "description",
                "Color": "color",
                "OperationId": "tasks"
            },
            "select_query": "select ct.Date, ct.Activ, ct.ClientId, ct.Number, ct.Designation, ct.Addition, " \
                    "ct.Description, ct.Color, cto.CleaningTypeId, cto.OperationId from CleaningTypes as ct JOIN " \
                    "CleaningTypeOperations as cto ON ct.Id=cto.CleaningTypeId where Ct.ClientID IN {};",
            "client_field": "ClientId",
            "load_data_service": CleaningTypeData.load_cleaning_types
        },

    "maintenance_task": {
        "mo4jo_table_name": "Maintenance",
        "fields": {
            "WarId": "id",
            "WarClientId": "client_id",
            "WarNummer": "task_number",
            "WarBezeichnung": "denotation",
            "WarBeschreibung": "description",
            "WarOrder": "task_order",
            "WarImage": "icon",
            "WarActiv": "is_active",
            "WarDate": "created_date"
        },
        "select_query": "select * from Maintenance where WarClientId in {} ORDER BY WarDate offset {} rows fetch next {} rows only;",
        "client_field": "WarClientId",
        "load_data_service": MaintenanceTaskData.load_maintenance_tasks,
        "count_query":"select count(*) as count from Maintenance where WarClientId in {}"
    },

    "maintenance_type": {
        "mo4jo_table_name": "MaintenanceTypes",
        "fields": {
            "Id": "id",
            "Date": "created_date",
            "Activ": "is_active",
            "ClientId": "client_id",
            "Number": "number",
            "Designation": "designation",
            "Addition": "addition",
            "Description": "description",
            "Color": "color",
            "MaintenanceId": "tasks"
        },
        "select_query": "select mt.Id,mt.Number,mt.Designation,mt.Addition,mt.Description,mt.Color,mt.Activ,mt.Date,mt.ClientId,mtm.MaintenanceId from MaintenanceTypes as mt left join  MaintenanceTypeMaintenance as mtm on mt.Id=mtm.MaintenanceTypeId where mt.ClientId in {} ;",
        "client_field": "ClientId",
        "load_data_service": MaintenanceTypeData.load_maintenance_types,
        "count_query":"select count(*) from MaintenanceTypes where ClientId in {};"

    },
        "room_types": {
            "mo4jo_table_name": "Rooms",
            "fields": {
                "RomId": "id",
                "RomNummer": "number",
                "RomBezeichnung": "denotation",
                "RomZusatz": "addition",
                "RomBeschreibung": "description",
                "RomActiv": "is_active",
                "RomDate": "created_date",
                "RomClientId": "client_id"
            },
            "select_query": "select * from Rooms where RomClientId IN {};",
            "load_data_service": RoomTypeData.load_room_types,
            "count_query":"select count(*) from Rooms where RomClientId IN {};"
        },
        "employee": {
            "mo4jo_table_name": "Persons",
            "fields": {
                "PerId": "id",
                "PerNummer": "employee_number",
                "PerFirstName": "first_name",
                "PerLastName": "last_name",
                "PerPhone": "phone",
                "PerLangCodesIsoId": "language",
                "PerClientId": "client_id",
                "PerActiv": "is_active",
                "PerDate": "created_date"
            },
            "select_query": "select  PerId, PerNummer, PerFirstName, PerLastName, PerPhone, PerLangCodesIsoId, "
                            "PerClientId, PerActiv, PerDate from Persons  where  PerClientId IN {}  ORDER BY PerDate offset {} rows fetch next {} rows only;",
            "load_data_service": EmployeeData.load_employees,
            "count_query":"select count(*) as count from Persons where PerClientId IN {} ;",
            "select_query_date":"select  PerId, PerNummer, PerFirstName, PerLastName, PerPhone, PerLangCodesIsoId, "
                            "PerClientId, PerActiv, PerDate from Persons  where  PerClientId IN {} and PerDate between '{}' and '{}';"
        },

        "recipient": {
            "mo4jo_table_name": "Users",
            "fields": {
                "UsrId": "id",
                "UsrEmail": "email",
                "UsrActiv": "is_active",
                "UsrClientId": "client_id",
                "UsrDate": "created_date"
            },
            "select_query": "select * from Users where UsrClientId in {} ORDER BY UsrDate offset {} rows fetch next {} rows only;;",
            "load_data_service": ReceipientData.load_receipient,
            "count_query":"select count(*) as count from Users where UsrClientId in {};"
        },
        "recipient_group": {
            "mo4jo_table_name": "ReceiverGroup",
            "fields": {
                "id": "id",
                "Bezeichnung": "denotation",
                "Receiver": "email",
                "CliId": "client_id",
                "CDate": "created_date"
            },
            "select_query": "select rc.id,rc.Bezeichnung,rc.Receiver,rc.CDate,c.CliId from ReceiverGroup as rc join Clients as c on rc.Mandant=c.CliName1 where c.CliId in {};",
            "load_data_service": ReceipientGroupData.load_receipient_group
        },
        "notification_group":{

            "mo4jo_table_name": "MessageGroup",
            "fields": {
                "id": "id",
                "Bezeichnung": "denotation",
                "Beschreibung": "description",
                "CliId": "client_id",
                "CDate": "created_date",
                "EvtBez10":"EvtBez10",
                "ReceiverGrpId10":"ReceiverGrpId10",
                "EvtBez9": "EvtBez9",
                "ReceiverGrpId": "ReceiverGrpId10",
                "EvtBez8": "EvtBez8",
                "ReceiverGrpId8": "ReceiverGrpId8",
                "EvtBez7": "EvtBez7",
                "ReceiverGrpId7": "ReceiverGrpId7",
                "EvtBez6": "EvtBez6",
                "ReceiverGrpId6": "ReceiverGrpId6",
                "EvtBez5": "EvtBez5",
                "ReceiverGrpId5": "ReceiverGrpId5",
                "EvtBez4": "EvtBez4",
                "ReceiverGrpId4": "ReceiverGrpId4",
                "EvtBez3": "EvtBez3",
                "ReceiverGrpId3": "ReceiverGrpId3",
                "EvtBez2": "EvtBez2",
                "ReceiverGrpId2": "ReceiverGrpId2",
                "EvtBez1": "EvtBez1",
                "ReceiverGrpId1": "ReceiverGrpId1"
            },
           "select_query": "select mg.Id, mg.Bezeichnung,mg.Beschreibung, mg.EvtBez4, mg.ReceiverGrpId4, mg.EvtBez3, mg.ReceiverGrpId3, mg.EvtBez2, mg.ReceiverGrpId2, mg.EvtBez1, mg.ReceiverGrpId1, mg.CDate, c.CliId from MessageGroup as mg left join Clients as c on mg.Mandant = c.CliName1 where c.CliId in {};",
            "load_data_service": NotificationGroupData.load_notification_group
        },


        "rooms": {
            "mo4jo_table_name": "Locations",
            "fields": {
                "LocId": "id",
                "LocClientId": "client_id",
                "LocObjId": "site_id",
                "LocNummer": "number",
                "LocKdNummer": "room_number",
                "LocBezeichnung": "denotation",
                "LocGeschoss": "floor_level",
                "LocRomType": "room_type_id",
                "LocHohe": "height",
                "LocBodenflache": "square_meter",
                "LocBodenbelag": "floor_covering",
                "LocFensterflachen": "window_square_meters",
                "LocMatOberflachen": "surface_material",
                "LocMatWand": "wall_material",
                "LocMatDecken": "cleaning_frequency",
                "LocBeanspruchung": "conditions",
                "LocUberstellungsgrad": "obstruction",
                "LocReinigungslevel": "cleaning_level",
                "LocKostenstelle": "cost_unit",
                "LocRevier": "district",
                "LocSchlusselliste": "key_list",
                "LocZutrittBer": "access",
                "LocKontrollReinigung": "check_cleaning",
                "LocHeizkorper": "heating_devices",
                "LocLeuchtkorper": "luminous_elements",
                "LocLuftKlima": "air_condition",
                "LocSteckdosen": "power_outlet",
                "LocFeuermelder": "fire_detector",
                "LocWasserstelle": "water_available",
                "LocSicherheit": "security",
                "LocPflanzen": "plants",
                "LocDate": "created_date",
                "LocActiv": "is_active"
            },
            "select_query": "select LocId as Id, LocClientId as client_id, LocObjId as site_id, LocNummer as number, "
                            "LocKdNummer as room_number, LocRomType as room_type_id, LocBezeichnung as denotation, "
                            "LocGeschoss as floor_level, LocHohe as height, LocBodenflache as square_meter, "
                            "LocBodenbelag as floor_covering, LocFensterflachen as window_square_meters, "
                            "LocMatOberflachen as surface_material, LocMatWand as wall_material, "
                            "LocMatDecken as cleaning_frequency, LocBeanspruchung as conditions,"
                            "LocUberstellungsgrad as obstruction, LocReinigungslevel as cleaning_level, "
                            "LocKostenstelle as cost_unit, LocRevier as district, LocSchlusselliste as key_list, "
                            "LocZutrittBer as access, LocKontrollReinigung as check_cleaning, LocHeizkorper as heating_devices, "
                            "LocLeuchtkorper as luminous_elements, LocLuftKlima as air_condition, LocSteckdosen as power_outlet, "
                            "LocFeuermelder as fire_detector, LocWasserstelle as water_available, LocSicherheit as security,"
                            "LocPflanzen as plants, LocDate as created_date, LocActiv as is_active from "
                            "Locations where LocClientId IN {};",
            "rooms_maintenance_type_query": "select MaintenanceTypeId, LocationId, ClientId, Deleted from LocationMaintenanceType where ClientId IN {};",
            "rooms_cleaning_type_query": "select CleaningTypeId, LocationId, ClientId, Deleted from LocationCleaningType where ClientId IN {};",
            "cleaning_type_query": "select Id, Designation, Addition, ClientId from CleaningTypes where Id IN {};",
            "maintenance_type_query": "select Id, Designation, Addition, ClientId from MaintenanceTypes where Id IN {};",
            "room_types_query": "select RomId as Id, RomBezeichnung as denotation, RomZusatz as addition from Rooms where RomId IN {} and RomClientId IN {};",
            "sites_query": "select ObjId, ObjName1, ObjClientId from Objects where ObjId IN {} and ObjClientId IN {};",
            "load_data_service": RoomsData.load_rooms

        }
    }

    IMAGE_EXTENSIONS_MAPPING = {
        '895': 'png',
        'FFD': 'jpg',
        '425': 'bpg',
        '474': 'gif',
        '494': 'tif',
        '255': 'pdf',
        '424': 'bmp',
        'D0C': 'doc'
    }
