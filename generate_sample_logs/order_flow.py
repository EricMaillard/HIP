import json
import requests
import uuid
from random import randint
import time
from datetime import datetime as dt, timedelta
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
import threading
import traceback

# set up logging #####################################
import sys,logging,logging.handlers,os.path
#in this particular case, argv[0] is likely pythonservice.exe deep in python's lib\
# so it makes no sense to write log there
log_file="order_flow.log"
logger = logging.getLogger()
logger.setLevel(logging.INFO)
f = logging.Formatter(u"%(asctime)s %(process)d:%(thread)d %(name)s %(levelname)-8s %(message)s")
h=logging.StreamHandler(sys.stdout)
h.setLevel(logging.NOTSET)
h.setFormatter(f)
logger.addHandler(h)
h=logging.handlers.RotatingFileHandler(log_file,maxBytes=1024**2,backupCount=1)
h.setLevel(logging.NOTSET)
h.setFormatter(f)
logger.addHandler(h)
del h,f
#hook to log unhandled exceptions
def excepthook(type,value,traceback):
    logger.error("Unhandled exception occured",exc_info=(type,value,traceback))
    #Don't need another copy of traceback on stderr
    if old_excepthook!=sys.__excepthook__:
        old_excepthook(type,value,traceback)
old_excepthook = sys.excepthook
sys.excepthook = excepthook
del log_file
# ####################################################


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
nb_threads = dt_settings.get("nb_threads")
error_problem_pattern_frequency = dt_settings.get("error_problem_pattern_frequency")
error_problem_pattern_duration = dt_settings.get("error_problem_pattern_duration")
slowdown_problem_pattern_frequency = dt_settings.get("slowdown_problem_pattern_frequency")
slowdown_problem_pattern_duration = dt_settings.get("slowdown_problem_pattern_duration")

exit_requested = False

class ExecutionState(Enum):
    NORMAL = 1
    PROBLEM = 2

class ExecutionInfo:
	state = ExecutionState.NORMAL
	with_errors = False
	with_slowdown = False
	thread_id = -1
	error_type = -1
	startTime = None
	startProblemTime = None

	def __init__(self, with_errors, with_slowdown, thread_id):
		self.with_errors = with_errors
		self.with_slowdown = with_slowdown
		self.thread_id = thread_id
		self.state = ExecutionState.NORMAL

	def getState(self):
		return self.state
   
	def setState(self, state):
		self.state = state

	def getErrorType(self):
		return self.error_type
   
	def setErrorType(self, error_type):
		self.error_type = error_type
	
	def isWithErrors(self):
		return self.with_errors

	def isWithSlowdown(self):
		return self.with_slowdown
	
	def getThreadId(self):
		return self.thread_id

	def setStartTime(self, startTime):
		self.startTime = startTime

	def getStartTime(self):
		return self.startTime

	def setStartProblemTime(self, startProblemTime):
		self.startProblemTime = startProblemTime

	def getStartProblemTime(self):
		return self.startProblemTime
	

lock = threading.Lock()
log_json = []

def send_logs():
	global exit_requested
	#send logs to Dynatrace every minute
	while True:
		try:
			if exit_requested:
				logger.info("End send_logs thread")
				break
			time.sleep(60)
			lock.acquire()
			dynatrace_response = requests.post(url, json=log_json, headers=dtheader)
			if dynatrace_response.status_code >= 400:
				print(f'Error in Dynatrace log API Response :\n'
							f'{dynatrace_response.text}\n'
							f'Message was :\n'
							f'{str(log_json)}'
							)
			log_json.clear()
			lock.release()
		except Exception as e:
			lock.release()
			print(traceback.format_exc())

def datetime_from_local_to_utc(local_datetime):
    now_timestamp = time.time()
    offset = dt.fromtimestamp(now_timestamp) - dt.utcfromtimestamp(now_timestamp)
    return local_datetime - offset

def createFlow(execution_info: ExecutionInfo):
	try:
		global exit_requested
		execution_info.setStartTime(dt.now())
		while True:
			if exit_requested:
				logger.info("End thread "+str(execution_info.getThreadId()))
				break
			logger.info('Thread id = '+str(execution_info.getThreadId()))
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.NORMAL:
				currentTime = dt.now()
				delta = currentTime - execution_info.getStartTime()
				logger.info(str(execution_info.getThreadId())+ " delta1 = "+str(delta.total_seconds()))
				if (delta.total_seconds() > error_problem_pattern_frequency*60):
					# error type 0 : flow stops in step 1 with error message "Unable to send message to Step2"
					# error type 1 : flow stops in step 2 with error message "Unable to connect to SCC"
					# error type 2 : flow stops in step 3 with error message "51 erreur fonctionnelle"
					# error type 3 : flow stops in step 3 with error message "56 erreur de configuration"
					# error type 4 : flow stops in step 4 with error message in YYEDI_CM_MSGST_51 "Il y a eu une intervention sur l'IDOC"
					# error type 5 : flow stops in step 4 with error message in YYEDI_CM_MSGST_69 "IDOC a ete modifie (message source modifie)"
					# error type 6 : flow stops in step 5 with error message "40 : traitement CPI acquitte negativement => erreur during update"
					# error type 7 : flow stops in step 6 with error message "Unable to send the message to Step7"
					# error type 8 : flow stops in step 7 with error message "Unable to send the message to salesforce"
					error_type = randint(0,8)
					execution_info.setErrorType(error_type)
					execution_info.setState(ExecutionState.PROBLEM)
					execution_info.setStartProblemTime(dt.now())

			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM:
				currentTime = dt.now()
				delta = currentTime - execution_info.getStartProblemTime()
				logger.info(str(execution_info.getThreadId())+ " delta2 = "+str(delta.total_seconds()))
				if (delta.total_seconds() > error_problem_pattern_duration*60):
					execution_info.setErrorType(-1)
					execution_info.setState(ExecutionState.NORMAL)
					execution_info.setStartTime(dt.now() - timedelta(minutes=error_problem_pattern_duration))

			if execution_info.isWithSlowdown() and execution_info.getState() == ExecutionState.NORMAL:
				currentTime = dt.now()
				delta = currentTime - execution_info.getStartTime()
				if (delta.total_seconds() > slowdown_problem_pattern_frequency*60):
					execution_info.setState(ExecutionState.PROBLEM)
					execution_info.setStartProblemTime(dt.now())

			if execution_info.isWithSlowdown() and execution_info.getState() == ExecutionState.PROBLEM:
				currentTime = dt.now()
				delta = currentTime - execution_info.getStartProblemTime()
				if (delta.total_seconds() > slowdown_problem_pattern_duration*60):
					execution_info.setState(ExecutionState.NORMAL)
					execution_info.setStartTime(dt.now() - timedelta(minutes=slowdown_problem_pattern_duration))

			# create CPI bMx-IF-SLS-Salesforce_TO_SAP_Order.Create.Steo1 log
			logger.info("create CPI bMx-IF-SLS-Salesforce_TO_SAP_Order.Create.Step1 log")
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 0:
				Status = "FAILED"
			else:
				Status = "COMPLETED"
			MessageGuid = str(uuid.uuid4())
			CorrelationId = str(uuid.uuid4())
			TransactionId = str(uuid.uuid4())
			BusinessDocumentNumber = randint(10000000, 99999999)
			logger.info("BusinessDocumentNumber = "+str(BusinessDocumentNumber))
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
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 0:
				SAP_Order_Create_Step1['ErrorMessage'] = "Unable to send message to Step2"
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
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 0:
				log_payload['flow.error_message'] = SAP_Order_Create_Step1.get('ErrorMessage')
			lock.acquire()
			log_json.append(log_payload)
			lock.release()
			# send flow logs to Dynatrace and stop the flow here if there is an error
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 0:
				waitUntilNextStep()
				continue

			# wait few milliseconds before creating next log line 
			value = millisec + randint(50, 500)
			time.sleep(value/1000)

			# create CPI bMx-IF-SLS-Salesforce_TO_SAP_Order.Create.Step2 log
			logger.info("create CPI bMx-IF-SLS-Salesforce_TO_SAP_Order.Create.Step2 log")
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 1:
				Status = "FAILED"
			else:
				Status = "COMPLETED"
			MessageGuid = str(uuid.uuid4())
			CorrelationId = str(uuid.uuid4())
			TransactionId = str(uuid.uuid4())
			date_now = dt.now()
			LogStart = date_now.strftime("%Y-%m-%d %H:%M:%S.%f")
			if execution_info.isWithSlowdown() and execution_info.getState() == ExecutionState.PROBLEM:
				millisec = randint(15000,20000)
			else:
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
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 1:
				SAP_Order_Create_Step1['ErrorMessage'] = "Unable to connect to SCC"
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
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 1:
				log_payload['flow.error_message'] = SAP_Order_Create_Step2.get('ErrorMessage')
			lock.acquire()
			log_json.append(log_payload)
			lock.release()
			# send flow logs to Dynatrace and stop the flow here if there is an error
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 1:
				waitUntilNextStep()
				continue

			# wait few milliseconds before creating next log line 
			value = millisec + randint(50, 500)
			time.sleep(value/1000)

			# create CPI IDoc ORDERS (in > ERP) log
			logger.info("create CPI IDoc ORDERS (in > ERP) log")
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 2:
				status = "51"
			elif execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 3:
				status = "56"
			else:
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
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 2:
				log_content['ErrorMessage'] = "51 erreur fonctionnelle"
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 3:
				log_content['ErrorMessage'] = "56 erreur de configuration"
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
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 2:
				log_payload['flow.error_message'] = log_content.get('ErrorMessage')
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 3:
				log_payload['flow.error_message'] = log_content.get('ErrorMessage')
			lock.acquire()
			log_json.append(log_payload)
			lock.release()
			# send flow logs to Dynatrace and stop the flow here if there is an error
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 2:
				waitUntilNextStep()
				continue
			# send flow logs to Dynatrace and stop the flow here if there is an error
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 3:
				waitUntilNextStep()
				continue

			# wait few milliseconds before creating next log line 
			value = randint(100, 800)
			time.sleep(value/1000)

			# create Document de vente log
			logger.info("create Document de vente log")
			vbeln = randint(100000,999999)
			date_now = dt.now()
			date = date_now.strftime("%Y-%m-%d %H:%M:%S.%f")
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 4:
				YYEDI_CM_MSGST_51 = "Il y a eu une intervention sur l'IDOC"
				YYEDI_CM_MSGST_69 = ""
			elif execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 5:
				YYEDI_CM_MSGST_51 = ""
				YYEDI_CM_MSGST_69 = "IDOC a ete modifie (message source modifie)"
			else:
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
				LogLevel = "ERROR"
			elif YYEDI_CM_MSGST_69 != "":
				LogLevel = "ERROR"
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
			if YYEDI_CM_MSGST_51 != "":
				log_payload['flow.error_message'] = YYEDI_CM_MSGST_51
			elif YYEDI_CM_MSGST_69 != "":
				log_payload['flow.error_message'] = YYEDI_CM_MSGST_69
			lock.acquire()
			log_json.append(log_payload)
			lock.release()
			# send flow logs to Dynatrace and stop the flow here if there is an error
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 4:
				waitUntilNextStep()
				continue
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 5:
				waitUntilNextStep()
				continue

			# wait few milliseconds before creating next log line 
			value = randint(100, 800)
			time.sleep(value/1000)

			# create IDoc ORDACK (ERP > out) log
			logger.info("create IDoc ORDACK (ERP > out) log")
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 6:
				status = "40"
			else:
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
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 6:
				log_content['ErrorMessage'] = "40 : traitement CPI acquitte negativement => erreur during update"
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
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 6:
				log_payload['flow.error_message'] = log_content.get('ErrorMessage')
			lock.acquire()
			log_json.append(log_payload)
			lock.release()
			# send flow logs to Dynatrace and stop the flow here if there is an error
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 6:
				waitUntilNextStep()
				continue

			# wait few milliseconds before creating next log line 
			value = randint(100, 800)
			time.sleep(value/1000)

			# create CPI bMx-IF-SLS-SAP_TO_Salesforce_Order.Acknowledge.Step1 log
			logger.info("create CPI bMx-IF-SLS-SAP_TO_Salesforce_Order.Acknowledge.Step1 log")
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 7:
				Status = "FAILED"
			else:
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
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 7:
				SAP_Order_Acknowledge_Step1['ErrorMessage'] = "Unable to send the message to Step7"
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
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 7:
				log_payload['flow.error_message'] = SAP_Order_Acknowledge_Step1.get('ErrorMessage')
			lock.acquire()
			log_json.append(log_payload)
			lock.release()
			# send flow logs to Dynatrace and stop the flow here if there is an error
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 7:
				waitUntilNextStep()
				continue

			# wait few milliseconds before creating next log line 
			value = millisec + randint(50, 500)
			time.sleep(value/1000)

			# create CPI bMx-IF-SLS-SAP_TO_Salesforce_Order.Acknowledge.Step2 log
			logger.info("create CPI bMx-IF-SLS-SAP_TO_Salesforce_Order.Acknowledge.Step2 log")
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 8:
				Status = "FAILED"
			else:
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
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 8:
				SAP_Order_Acknowledge_Step2['ErrorMessage'] = "Unable to send the message to salesforce"
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
			if execution_info.isWithErrors() and execution_info.getState() == ExecutionState.PROBLEM and execution_info.getErrorType() == 8:
				log_payload['flow.error_message'] = SAP_Order_Acknowledge_Step2.get('ErrorMessage')
			lock.acquire()
			log_json.append(log_payload)
			lock.release()
			value = millisec + randint(50, 500)
			time.sleep(value/1000)

			# send flow logs to Dynatrace
			waitUntilNextStep()
	except Exception:
		excepthook(*sys.exc_info())

def waitUntilNextStep():
	value = randint(10000, 60000)
	time.sleep(value/1000)	

def run():
	global exit_requested
	executor = ThreadPoolExecutor(max_workers=nb_threads)
	for i in range (0,nb_threads-2):
		if i % 2 == 0:
			execution_info = ExecutionInfo(False, False, i)
		else:
			execution_info = ExecutionInfo(False, True, i)			
		executor.submit(createFlow, execution_info)

	execution_info = ExecutionInfo(True, False, nb_threads-2)
	executor.submit(createFlow, execution_info)

	execution_info = ExecutionInfo(True, False, nb_threads-1)
	executor.submit(createFlow, execution_info)

	send_logs_thread = threading.Thread(target=send_logs)
	send_logs_thread.daemon=True
	send_logs_thread.start()

	loop_forever = True
	while loop_forever:
		try:
			time.sleep(60)
		except KeyboardInterrupt:
			logger.info("exit_requested")
			exit_requested = True
			loop_forever = False
	logger.info("exit program")
	quit()

if __name__ == "__main__":
    run()
