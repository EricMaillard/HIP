import json
import requests
import uuid
from random import randint
import time
import math
from datetime import datetime as dt, timedelta


SAP_Order_Create_Step1 = {
  "IntegrationFlowName": "bMx-IF-SLS-Salesforce_TO_SAP_Order.Create.Step1",
  "MessageGuid": "",
  "CorrelationId": "",
  "CustomStatus": "COMPLETED",
  "TransactionId": "",
  "LocalComponentName": "CPI_l4106",
  "LogStart": "",
  "LogEnd": "",
  "Receiver": None,
  "Sender": None,
  "Status": "COMPLETED",
  "CustomHeaders": {
    "salesforce.trigger": "/event/Order_Submission_Event__e",
    "salesforce.BusinessDocumentNumber": "",
    "salesforce.ReplayId": ""
  }
}

SAP_Order_Create_Step2 = {
  "IntegrationFlowName": "bMx-IF-SLS-Salesforce_TO_SAP_Order.Create.Step2",
  "MessageGuid": "",
  "CorrelationId": "",
  "CustomStatus": "COMPLETED",
  "TransactionId": "",
  "LocalComponentName": "CPI_l4200",
  "LogStart": "",
  "LogEnd": "",
  "Receiver": None,
  "Sender": None,
  "Status": "COMPLETED",
  "CustomHeaders": {
    "salesforce.trigger": "/event/Order_Submission_Event__e",
    "salesforce.BusinessDocumentNumber": "",
    "salesforce.ReplayId": ""
  }
}

SAP_Order_Acknowledge_Step1 = {
  "IntegrationFlowName": "bMx-IF-SLS-SAP_TO_Salesforce_Order.Acknowledge.Step1",
  "MessageGuid": "",
  "CorrelationId": "",
  "CustomStatus": "COMPLETED",
  "TransactionId": "",
  "LocalComponentName": "CPI_l4400",
  "LogStart": "",
  "LogEnd": "",
  "Receiver": None,
  "Sender": None,
  "Status": "COMPLETED",
  "CustomHeaders": {
    "salesforce.BusinessDocumentNumber": "",
    "sap.BusinessDocumentNumber": "",
    "sap.IDocNumber": "",
    "sap.SalesArea": ""
  }
}

SAP_Order_Acknowledge_Step2 = {
  "IntegrationFlowName": "bMx-IF-SLS-SAP_TO_Salesforce_Order.Acknowledge.Step2",
  "MessageGuid": "",
  "CorrelationId": "",
  "CustomStatus": "COMPLETED",
  "TransactionId": "",
  "LocalComponentName": "CPI_l4500",
  "LogStart": "",
  "LogEnd": "",
  "Receiver": None,
  "Sender": None,
  "Status": "COMPLETED",
  "CustomHeaders": {
    "salesforce.BusinessDocumentNumber": "",
    "sap.BusinessDocumentNumber": "",
    "sap.IDocNumber": "",
    "sap.SalesArea": ""
  }
}

head = {
    'Accept': 'application/json',
    'Content-Type': 'application/json; charset=UTF-8',
}

with open("dt-settings.json", encoding='utf-8') as f:
     dt_settings = json.load(f)

url = dt_settings.get("dynatrace_server_url")+ '/api/v2/logs/ingest'
dtheader = {
    'Content-Type': 'application/json; charset=utf-8',
    'Authorization': 'Api-Token '+dt_settings.get("dynatrace_api_token"),
}

def datetime_from_local_to_utc(local_datetime):
    now_timestamp = time.time()
    offset = dt.fromtimestamp(now_timestamp) - dt.utcfromtimestamp(now_timestamp)
    return local_datetime - offset

while True:
	log_json = []
	# create CPI bMx-IF-SLS-Salesforce_TO_SAP_Order.Create.Steo1 log
	print("create CPI bMx-IF-SLS-Salesforce_TO_SAP_Order.Create.Step1 log")
	Status = "COMPLETED"
	MessageGuid = str(uuid.uuid4())
	CorrelationId = str(uuid.uuid4())
	TransactionId = str(uuid.uuid4())
	BusinessDocumentNumber = randint(10000000, 99999999)
	print("BusinessDocumentNumber = "+str(BusinessDocumentNumber))
	ReplayId = randint(1000000, 9999999)
	date_now = dt.now()
	LogStart = date_now.strftime("%Y-%m-%d %H:%M:%S.%f")
	millisec = randint(800,3000)
	date_end = date_now + timedelta(milliseconds=millisec)
	LogEnd = date_end.strftime("%Y-%m-%d %H:%M:%S.%f")
	SAP_Order_Create_Step1['MessageGuid'] = MessageGuid
	SAP_Order_Create_Step1['CorrelationId'] = CorrelationId
	SAP_Order_Create_Step1['TransactionId'] = TransactionId
	SAP_Order_Create_Step1['LogStart'] = LogStart
	SAP_Order_Create_Step1['LogEnd'] = LogEnd
	SAP_Order_Create_Step1['CustomHeaders']['salesforce.BusinessDocumentNumber'] = str(BusinessDocumentNumber)
	SAP_Order_Create_Step1['CustomHeaders']['salesforce.ReplayId'] = str(ReplayId)
	SAP_Order_Create_Step1['Status'] = Status
	SAP_Order_Create_Step1['CustomStatus'] = Status
	if Status == "COMPLETED":
		LogLevel = "INFO"
	else:
		LogLevel = "ERROR"

	date_utc = datetime_from_local_to_utc(date_end)
	log_payload = {
		"content" : json.dumps(SAP_Order_Create_Step1),
		"sap.cpi.IntegrationFlowName" : "bMx-IF-SLS-Salesforce_TO_SAP_Order.Create.Step1",
		"sap.cpi.Status" : Status,
		"sap.cpi.LocalComponentName" : "CPI_l4106",
		"sap.cpi.Receiver" : None,
		"sap.cpi.Sender" : None,
		"sap.cpi.Server" : "l4106-tmn.hci.eu1.hana.ondemand.com",
		"log.source" : "demo.sap.cpi",
		"flow.step_name" :"bMx-IF-SLS-Salesforce_TO_SAP_Order.Create.Step1",
		"timestamp" : date_utc.strftime("%Y-%m-%dT%H:%M:%S.%f"),
		"severity" : LogLevel
	}
	log_json.append(log_payload)

    # wait few milliseconds before creating next log line 
	value = millisec + randint(50, 500)
	time.sleep(value/1000)

	# create CPI bMx-IF-SLS-Salesforce_TO_SAP_Order.Create.Step2 log
	print("create CPI bMx-IF-SLS-Salesforce_TO_SAP_Order.Create.Step2 log")
	Status = "COMPLETED"
	MessageGuid = str(uuid.uuid4())
	CorrelationId = str(uuid.uuid4())
	TransactionId = str(uuid.uuid4())
	date_now = dt.now()
	LogStart = date_now.strftime("%Y-%m-%d %H:%M:%S.%f")
	millisec = randint(800,3000)
	date_end = date_now + timedelta(milliseconds=millisec)
	LogEnd = date_end.strftime("%Y-%m-%d %H:%M:%S.%f")
	SAP_Order_Create_Step2['MessageGuid'] = MessageGuid
	SAP_Order_Create_Step2['CorrelationId'] = CorrelationId
	SAP_Order_Create_Step2['TransactionId'] = TransactionId
	SAP_Order_Create_Step2['LogStart'] = LogStart
	SAP_Order_Create_Step2['LogEnd'] = LogEnd
	SAP_Order_Create_Step2['CustomHeaders']['salesforce.BusinessDocumentNumber'] = str(BusinessDocumentNumber)
	SAP_Order_Create_Step2['CustomHeaders']['salesforce.ReplayId'] = str(ReplayId)
	SAP_Order_Create_Step2['Status'] = Status
	SAP_Order_Create_Step2['CustomStatus'] = Status
	if Status == "COMPLETED":
		LogLevel = "INFO"
	else:
		LogLevel = "ERROR"

	date_utc = datetime_from_local_to_utc(date_end)
	log_payload = {
		"content" : json.dumps(SAP_Order_Create_Step2),
		"sap.cpi.IntegrationFlowName" : "bMx-IF-SLS-Salesforce_TO_SAP_Order.Create.Step2",
		"sap.cpi.Status" : Status,
		"sap.cpi.LocalComponentName" : "CPI_l4200",
		"sap.cpi.Receiver" : None,
		"sap.cpi.Sender" : None,
		"sap.cpi.Server" : "l4106-tmn.hci.eu1.hana.ondemand.com",
		"log.source" : "demo.sap.cpi",
		"flow.step_name" :"bMx-IF-SLS-Salesforce_TO_SAP_Order.Create.Step2",
		"timestamp" : date_utc.strftime("%Y-%m-%dT%H:%M:%S.%f"),
		"severity" : LogLevel
	}
	log_json.append(log_payload)

    # wait few milliseconds before creating next log line 
	value = millisec + randint(50, 500)
	time.sleep(value/1000)

	# create CPI IDoc ORDERS (in > ERP) log
	print("create CPI IDoc ORDERS (in > ERP) log")
	status = "53"
	date_now = dt.now()
	date = date_now.strftime("%Y-%m-%d %H:%M:%S.%f")
	log_content = { 
		"salesforce.BusinessDocumentNumber" : str(BusinessDocumentNumber),
		"MessageGuid" : MessageGuid,
		"salesforce.ReplayId " : str(ReplayId),
		"timestamp" : date,
		"Status" : status
	}
	if status == "53":
		LogLevel = "INFO"
	else:
		LogLevel = "ERROR"

	date_now = dt.utcnow()
	log_payload = {
		"content" : json.dumps(log_content),
		"sap.edidc.DIRECT" : 2,
		"sap.edidc.MESTYP" : "ORDERS",
		"sap.edidc.SNDPRN" : "0009000510",
		"sap.edidc.MESCOD" : "CRM",
		"sap.HANADB_HOST" : "frox4114",
		"log.source" : "demo.sap.edidc",
		"flow.step_name" :"IDoc_Order_In_EDIDC",
		"timestamp" : date_now.strftime("%Y-%m-%dT%H:%M:%S.%f"),
		"severity" : LogLevel
	}
	log_json.append(log_payload)

    # wait few milliseconds before creating next log line 
	value = randint(100, 800)
	time.sleep(value/1000)

	# create Document de vente log
	print("create Document de vente log")
	vbeln = randint(100000,999999)
	date_now = dt.now()
	date = date_now.strftime("%Y-%m-%d %H:%M:%S.%f")
	YYEDI_CM_MSGST_51 = ""
	YYEDI_CM_MSGST_69 = ""
	log_content = { 
		"salesforce.BusinessDocumentNumber" : str(BusinessDocumentNumber),
		"sap.vbak.num_command" : str(vbeln),
		"timestamp" : date,
		"sap.vbak.YYEDI_CM_MSGST_51" : YYEDI_CM_MSGST_51,
		"sap.vbak.YYEDI_CM_MSGST_69" : YYEDI_CM_MSGST_69
	}
	if YYEDI_CM_MSGST_51 != "":
		LogLevel = "WARN"
	elif YYEDI_CM_MSGST_69 != "":
		LogLevel = "WARN"
	else:
		LogLevel = "INFO"
	date_now = dt.utcnow()
	log_payload = {
		"content" : json.dumps(log_content),
		"sap.vbak.BSARK" : "018",
		"sap.HANADB_HOST" : "frox4114",
		"log.source" : "demo.sap.vbak",
		"flow.step_name" :"Sales_Document_Creation_in_VBAK",
		"timestamp" : date_now.strftime("%Y-%m-%dT%H:%M:%S.%f"),
		"severity" : LogLevel
	}
	log_json.append(log_payload)

    # wait few milliseconds before creating next log line 
	value = randint(100, 800)
	time.sleep(value/1000)

	# create IDoc ORDACK (ERP > out) log
	print("create IDoc ORDACK (ERP > out) log")
	status = "03"
	date_now = dt.now()
	date = date_now.strftime("%Y-%m-%d %H:%M:%S.%f")
	log_content = { 
		"salesforce.BusinessDocumentNumber" : str(BusinessDocumentNumber),
		"MessageGuid" : MessageGuid,
		"salesforce.ReplayId " : str(ReplayId),
		"timestamp" : date,
		"Status" : status
	}
	if status == "03" or status == "41":
		LogLevel = "INFO"
	else:
		LogLevel = "ERROR"
	date_now = dt.utcnow()
	log_payload = {
		"content" : json.dumps(log_content),
		"sap.edidc.DIRECT" : 1,
		"sap.edidc.MESTYP" : "ORDACK",
		"sap.edidc.SNDPRN" : "0009000510",
		"sap.edidc.MESCOD" : "CRM",
		"sap.HANADB_HOST" : "frox4114",
		"log.source" : "demo.sap.edidc",
		"flow.step_name" :"IDoc_Order_Out_EDIDC",
		"timestamp" : date_now.strftime("%Y-%m-%dT%H:%M:%S.%f"),
		"severity" : LogLevel
	}
	log_json.append(log_payload)

    # wait few milliseconds before creating next log line 
	value = randint(100, 800)
	time.sleep(value/1000)

	# create CPI bMx-IF-SLS-SAP_TO_Salesforce_Order.Acknowledge.Step1 log
	print("create CPI bMx-IF-SLS-SAP_TO_Salesforce_Order.Acknowledge.Step1 log")
	Status = "COMPLETED"
	MessageGuid = str(uuid.uuid4())
	CorrelationId = str(uuid.uuid4())
	TransactionId = str(uuid.uuid4())
	sap_IDocNumber = randint(100000,999999)
	sap_SalesArea = "sales_area"
	date_now = dt.now()
	LogStart = date_now.strftime("%Y-%m-%d %H:%M:%S.%f")
	millisec = randint(800,3000)
	date_end = date_now + timedelta(milliseconds=millisec)
	LogEnd = date_end.strftime("%Y-%m-%d %H:%M:%S.%f")
	SAP_Order_Acknowledge_Step1['MessageGuid'] = MessageGuid
	SAP_Order_Acknowledge_Step1['CorrelationId'] = CorrelationId
	SAP_Order_Acknowledge_Step1['TransactionId'] = TransactionId
	SAP_Order_Acknowledge_Step1['LogStart'] = LogStart
	SAP_Order_Acknowledge_Step1['LogEnd'] = LogEnd
	SAP_Order_Acknowledge_Step1['CustomHeaders']['salesforce.BusinessDocumentNumber'] = str(BusinessDocumentNumber)
	SAP_Order_Acknowledge_Step1['CustomHeaders']['sap.BusinessDocumentNumber'] = str(vbeln)
	SAP_Order_Acknowledge_Step1['CustomHeaders']['sap.IDocNumber'] = str(sap_IDocNumber)
	SAP_Order_Acknowledge_Step1['CustomHeaders']['sap.SalesArea'] = str(sap_SalesArea)
	SAP_Order_Acknowledge_Step1['Status'] = Status
	SAP_Order_Acknowledge_Step1['CustomStatus'] = Status
	if Status == "COMPLETED":
		LogLevel = "INFO"
	else:
		LogLevel = "ERROR"

	date_utc = datetime_from_local_to_utc(date_end)
	log_payload = {
		"content" : json.dumps(SAP_Order_Acknowledge_Step1),
		"sap.cpi.IntegrationFlowName" : "bMx-IF-SLS-SAP_TO_Salesforce_Order.Acknowledge.Step1",
		"sap.cpi.Status" : Status,
		"sap.cpi.LocalComponentName" : "CPI_l4400",
		"sap.cpi.Receiver" : None,
		"sap.cpi.Sender" : None,
		"sap.cpi.Server" : "l4106-tmn.hci.eu1.hana.ondemand.com",
		"log.source" : "demo.sap.cpi",
		"flow.step_name" :"bMx-IF-SLS-SAP_TO_Salesforce_Order.Acknowledge.Step1",
		"timestamp" : date_utc.strftime("%Y-%m-%dT%H:%M:%S.%f"),
		"severity" : LogLevel
	}
	log_json.append(log_payload)

    # wait few milliseconds before creating next log line 
	value = millisec + randint(50, 500)
	time.sleep(value/1000)

	# create CPI bMx-IF-SLS-SAP_TO_Salesforce_Order.Acknowledge.Step2 log
	print("create CPI bMx-IF-SLS-SAP_TO_Salesforce_Order.Acknowledge.Step2 log")
	Status = "COMPLETED"
	MessageGuid = str(uuid.uuid4())
	CorrelationId = str(uuid.uuid4())
	TransactionId = str(uuid.uuid4())
	date_now = dt.now()
	LogStart = date_now.strftime("%Y-%m-%d %H:%M:%S.%f")
	millisec = randint(800,3000)
	date_end = date_now + timedelta(milliseconds=millisec)
	LogEnd = date_end.strftime("%Y-%m-%d %H:%M:%S.%f")
	SAP_Order_Acknowledge_Step2['MessageGuid'] = MessageGuid
	SAP_Order_Acknowledge_Step2['CorrelationId'] = CorrelationId
	SAP_Order_Acknowledge_Step2['TransactionId'] = TransactionId
	SAP_Order_Acknowledge_Step2['LogStart'] = LogStart
	SAP_Order_Acknowledge_Step2['LogEnd'] = LogEnd
	SAP_Order_Acknowledge_Step2['CustomHeaders']['salesforce.BusinessDocumentNumber'] = str(BusinessDocumentNumber)
	SAP_Order_Acknowledge_Step2['CustomHeaders']['sap.BusinessDocumentNumber'] = str(vbeln)
	SAP_Order_Acknowledge_Step2['CustomHeaders']['sap.IDocNumber'] = str(sap_IDocNumber)
	SAP_Order_Acknowledge_Step2['CustomHeaders']['sap.SalesArea'] = str(sap_SalesArea)
	SAP_Order_Acknowledge_Step2['Status'] = Status
	SAP_Order_Acknowledge_Step2['CustomStatus'] = Status
	if Status == "COMPLETED":
		LogLevel = "INFO"
	else:
		LogLevel = "ERROR"

	date_utc = datetime_from_local_to_utc(date_end)
	log_payload = {
		"content" : json.dumps(SAP_Order_Acknowledge_Step2),
		"sap.cpi.IntegrationFlowName" : "bMx-IF-SLS-SAP_TO_Salesforce_Order.Acknowledge.Step2",
		"sap.cpi.Status" : Status,
		"sap.cpi.LocalComponentName" : "CPI_l4500",
		"sap.cpi.Receiver" : None,
		"sap.cpi.Sender" : None,
		"sap.cpi.Server" : "l4106-tmn.hci.eu1.hana.ondemand.com",
		"log.source" : "demo.sap.cpi",
		"flow.step_name" :"bMx-IF-SLS-SAP_TO_Salesforce_Order.Acknowledge.Step2",
		"timestamp" : date_utc.strftime("%Y-%m-%dT%H:%M:%S.%f"),
		"severity" : LogLevel
	}
	log_json.append(log_payload)
	value = millisec + randint(50, 500)
	time.sleep(value/1000)

	print("send logs to url = "+url)
	print(log_json)
	#print("dtheader = "+str(dtheader))
	dynatrace_response = requests.post(url, json=log_json, headers=dtheader)
	if dynatrace_response.status_code >= 400:
		print(f'Error in Dynatrace log API Response :\n'
					f'{dynatrace_response.text}\n'
					f'Message was :\n'
					f'{json.dumps(log_json)}'
					)

	value = randint(10000, 60000)
	time.sleep(value/1000)

