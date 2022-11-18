import json
import logging
from threading import Thread
import time
import requests
import oneagent # SDK initialization functions
from contextlib import suppress
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime as dt, timedelta
from urllib.request import Request

frequency=60

#logger = logging.getLogger(__name__)
# set up logging #####################################
import sys,logging,logging.handlers,os.path
#in this particular case, argv[0] is likely pythonservice.exe deep in python's lib\
# so it makes no sense to write log there
log_file="create_traces.log"
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


getsdk = oneagent.get_sdk # Just to make the code shorter.
IN_DEV_ENVIRONMENT = True # Let's assume we are *not* in production here...
def _diag_callback(text):
    logger.info(text)


# generic function to call API with a given uri, that returns json
def queryDynatraceAPI(uri, token):
    head = {
        'Accept': 'application/json',
        'Content-Type': 'application/json; charset=UTF-8',
        'Authorization': 'Api-Token ' + token
    }    

    logger.debug(uri)

    response = None
    jsonContent = None
    response = requests.get(uri,headers=head)
    # For successful API call, response code will be 200 (OK)
    if(response.ok):
        if(len(response.text) > 0):
            jsonContent = json.loads(response.text)
    else:
        logger.error(response.text)
        jsonContent = json.loads(response.text)
        errorMessage = ""
        if(jsonContent["error"]):
            errorMessage = jsonContent["error"]["message"]
            logger.error("Dynatrace API in URL:"+uri+" returned an error: " + errorMessage)
        jsonContent = None
        raise Exception("Error", "Dynatrace API returned in URI "+uri+" an error: " + errorMessage)

    return jsonContent


class CreateTraces():

    executor = ThreadPoolExecutor(max_workers=50)

    def __init__(self, argv):
        config_file_path = argv[1]
        logger.info('config_file_path = '+config_file_path)
        self.config_file_path = config_file_path
        self.environment = argv[0]
        os.environ['LANGUAGE'] = 'en_US.UTF-8'
        with open(self.config_file_path) as f:
            config_json = json.load(f)

        self.dt_url = config_json.get('dt_url')
        self.dt_token = config_json.get('dt_token')
        self.environment = config_json.get('environment')        
        self.service_name = config_json.get('service_name')        
        self.entry_point_query = config_json.get('entry_point_query')
        self.log_sources_to_query = config_json.get('log_sources_to_query')
        self.correlation_id = config_json.get('correlation_id')        
        self.number_of_steps = config_json.get('number_of_steps')        
        self.timeout = config_json.get('timeout')        
        log_level = config_json.get('log_level')
        if log_level == 'INFO':
            logger.setLevel(logging.INFO)
        elif log_level == 'DEBUG':
            logger.setLevel(logging.DEBUG)
        elif log_level == 'ERROR':
            logger.setLevel(logging.ERROR)
        elif log_level == 'WARNING':
            logger.setLevel(logging.WARNING)
        elif log_level == 'CRITICAL':
            logger.setLevel(logging.CRITICAL)
        else:
            logger.error('Invalid log level in configuration file')
        if not oneagent.initialize():
            logger.error('Error initializing OneAgent SDK.')
        sdk = getsdk()
        self.wappinfo = sdk.create_web_application_info(
            virtual_host="HIP_"+self.environment, # Logical name of the host server.
            application_id=self.environment+" - "+self.service_name, # Unique web application ID.
            context_root='/') # App's prefix of the path part of the URL.

    def getHeaders(self, logevent):
        headers = {}
        # get content from logevent and parse it to json
        log_source = logevent.get('additionalColumns').get('log.source')[0]
        log_content = logevent.get('content')
        log_content_json = json.loads(log_content)
        # depending on log source, add fields as request headers
        if log_source == 'demo.sap.cpi':
            sap_cpi_server = logevent.get('sap.cpi.server')
            if sap_cpi_server:
                headers['sap.cpi.server'] = sap_cpi_server
            MessageGuid = log_content_json.get('MessageGuid')         
            if MessageGuid:
                headers['MessageGuid'] = MessageGuid
            CorrelationId = log_content_json.get('CorrelationId')         
            if CorrelationId:
                headers['CorrelationId'] = CorrelationId
            CustomStatus = log_content_json.get('CustomStatus')         
            if CustomStatus:
                headers['CustomStatus'] = CustomStatus
            TransactionId = log_content_json.get('TransactionId')         
            if TransactionId:
                headers['TransactionId'] = TransactionId
            LocalComponentName = log_content_json.get('LocalComponentName')         
            if LocalComponentName:
                headers['LocalComponentName'] = LocalComponentName
            LogStart = log_content_json.get('LogStart')         
            if LogStart:
                headers['LogStart'] = LogStart
            LogEnd = log_content_json.get('LogEnd')         
            if LogEnd:
                headers['LogEnd'] = LogEnd
            Receiver = log_content_json.get('Receiver')         
            if Receiver:
                headers['Receiver'] = Receiver
            Sender = log_content_json.get('Sender')         
            if Sender:
                headers['Sender'] = Sender
            Status = log_content_json.get('Status')         
            if Status:
                headers['Status'] = Status
            CustomHeaders = log_content_json.get('CustomHeaders')         
            if CustomHeaders:
                headers['CustomHeaders'] = str(CustomHeaders)

        if log_source == 'demo.sap.edidc':
            MessageGuid = log_content_json.get('MessageGuid')         
            if MessageGuid:
                headers['MessageGuid'] = MessageGuid
            salesforce_ReplayId = log_content_json.get('salesforce.ReplayId')         
            if salesforce_ReplayId:
                headers['salesforce.ReplayId'] = salesforce_ReplayId
            timestamp = log_content_json.get('timestamp')         
            if timestamp:
                headers['timestamp'] = timestamp
            Status = log_content_json.get('Status')         
            if Status:
                headers['Status'] = Status
            sap_hanadb_host = logevent.get('sap.hanadb_host')
            if sap_hanadb_host:
                headers['sap.hanadb_host'] = sap_hanadb_host

        if log_source == 'demo.sap.vbak':
            sap_vbak_num_command = log_content_json.get('sap.vbak.num_command')         
            if sap_vbak_num_command:
                headers['sap.vbak.num_command'] = sap_vbak_num_command
            timestamp = log_content_json.get('timestamp')         
            if timestamp:
                headers['timestamp'] = timestamp
            sap_vbak_YYEDI_CM_MSGST_51 = log_content_json.get('sap.vbak.YYEDI_CM_MSGST_51')         
            if sap_vbak_YYEDI_CM_MSGST_51:
                headers['sap.vbak.YYEDI_CM_MSGST_51'] = sap_vbak_YYEDI_CM_MSGST_51
            sap_vbak_YYEDI_CM_MSGST_69 = log_content_json.get('sap.vbak.YYEDI_CM_MSGST_69')         
            if sap_vbak_YYEDI_CM_MSGST_69:
                headers['sap.vbak.YYEDI_CM_MSGST_69'] = sap_vbak_YYEDI_CM_MSGST_69
            sap_hanadb_host = logevent.get('sap.hanadb_host')
            if sap_hanadb_host:
                headers['sap.hanadb_host'] = sap_hanadb_host
        return headers

    def createTrace(self, trace_array):
        logger.info("createTrace")
        try:
            sdk = getsdk()
            correlation_id = trace_array[0].get('additionalColumns').get(self.correlation_id)[0]
            root_tracer = sdk.trace_incoming_web_request(
                self.wappinfo,
                'sales_order?',
                'POST',
                headers={},
                remote_address='')

            with root_tracer:
                # Process web 
                sdk.add_custom_request_attribute(self.correlation_id, correlation_id)
                status = trace_array[0].get('status')
                if status == 'INFO':
                    root_tracer.set_status_code(200) # OK
                else:
                    root_tracer.set_status_code(500) # KO
                    fullmessage = 'test|test'
                    splittedMessage = fullmessage.split('|')
                    root_tracer.mark_failed(splittedMessage[0], splittedMessage[1])
                # get headers
                headers_array = []
                for i in range (0,len(trace_array)):
                    headers_array.append(self.getHeaders(trace_array[i]))
                for i in range (0,len(trace_array)):
                    self.trace_outgoing_web_requests(trace_array[i], headers_array[i])
                    # wait until the start of the next call
                    if i < len(trace_array) - 1:
                        log_source1 = trace_array[i].get('additionalColumns').get('log.source')[0]
                        log_source2 = trace_array[i+1].get('additionalColumns').get('log.source')[0]
                        if log_source1 == 'demo.sap.cpi':
                            LogEnd = headers_array[i].get('LogEnd')         
                            date_end = dt.strptime(LogEnd, "%Y-%m-%d %H:%M:%S.%f")
                        else:
                            ts = headers_array[i].get('timestamp')
                            date_end = dt.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
                        if log_source2 == 'demo.sap.cpi':
                            LogStart = headers_array[i+1].get('LogStart')         
                            date_start = dt.strptime(LogStart, "%Y-%m-%d %H:%M:%S.%f")
                        else:
                            ts = headers_array[i+1].get('timestamp')
                            date_start = dt.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
                        logger.debug("date_end = "+str(date_end))
                        logger.debug("date_start = "+str(date_start))
                        delta = date_start - date_end
                        logger.debug("delta = "+str(delta.total_seconds()))
                        time.sleep(delta.total_seconds())

        except Exception:
            excepthook(*sys.exc_info())

    def trace_outgoing_web_requests(self, logevent, headers):
        sdk = getsdk()

        correlation_id = logevent.get('additionalColumns').get(self.correlation_id)[0]
        method_name = logevent.get('additionalColumns').get('flow.step_name')[0]
        log_source = logevent.get('additionalColumns').get('log.source')[0]
        # Create your web request.
        url = 'http://'+log_source+'/'+method_name+'?'

        duration = 1/1000
        if log_source == 'demo.sap.cpi':
            date_start = dt.strptime(headers.get('LogStart'), "%Y-%m-%d %H:%M:%S.%f")
            date_end = dt.strptime(headers.get('LogEnd'), "%Y-%m-%d %H:%M:%S.%f")
            delta = date_end - date_start
            duration = delta.total_seconds()

        # Create the tracer.
        #request_name = event + ' ' + component + ' ' + flow
        tracer = sdk.trace_outgoing_web_request(url, 'GET', headers)
        with tracer:
            sdk.add_custom_request_attribute(self.correlation_id, correlation_id)
            # Here you process and send the web request.
            time.sleep(duration)
            status = logevent.get('status')

            if status == 'INFO':
                tracer.set_status_code(200)
            else:
                tracer.set_status_code(500)
                fullmessage = 'test|test'
                splittedMessage = fullmessage.split('|')				
                tracer.mark_failed(splittedMessage[0], splittedMessage[1])

    def run(self):
        logs_to_correlate = {}
        previous_results = {}
        while (True):
            try:
                querytime = dt.utcnow()
                StartTime = querytime - timedelta(minutes=5)
                EndTime = querytime
                logger.info('starttime = '+StartTime.strftime("%Y-%m-%dT%H:%M:%S"))
                logger.info('endtime = '+EndTime.strftime("%Y-%m-%dT%H:%M:%S"))
                FomattedStartTime = StartTime.strftime("%Y-%m-%dT%H:%M:%S")
                FomattedEndTime = EndTime.strftime("%Y-%m-%dT%H:%M:%S")
                # query logs to get new entry point logs
                uri = self.dt_url+"/api/v2/logs/export?from="+FomattedStartTime+"&to="+FomattedEndTime+"&query="+urllib.parse.quote_plus(self.entry_point_query)+"&sort=timestamp"
                logger.info(uri)
                data = queryDynatraceAPI(uri, self.dt_token)
                results = data.get('results')

                for result in results:
                    correlation_id = result.get('additionalColumns').get(self.correlation_id)[0]
                    if previous_results.get(correlation_id) == None:
                        logger.info("Add new correlation_id = "+correlation_id)
                        previous_results[correlation_id] = result
                        logs_to_correlate[correlation_id] = result
                
                # clean previous results to remove logs older that 5 minutes
                keys_to_remove = []
                for key, value in previous_results.items():
                    timestamp = value.get('timestamp')
                    utc_dt = dt.utcfromtimestamp(timestamp/1000)
                    if utc_dt < StartTime:
                        keys_to_remove.append(key)
                for key in keys_to_remove:
                    del previous_results[key]

                # do a log search for each element in the dictionnary logs_to_correlate
                keys = logs_to_correlate.keys()
                length = len(keys)
                query = ""
                for log_source_to_query in self.log_sources_to_query:
                    query = query + 'log.source="'+log_source_to_query+'" OR '
                cnt = 0
                for correlation_id in keys:
                    if cnt < length-1:
                        query = query + self.correlation_id+'="'+correlation_id+'" OR '
                    else:
                        query = query + self.correlation_id+'="'+correlation_id+'"'
                    cnt = cnt + 1

                uri = self.dt_url+"/api/v2/logs/search?query="+urllib.parse.quote_plus(query)+"&sort=timestamp"
                data = queryDynatraceAPI(uri, self.dt_token)
                logger.info(data)
                results = data.get('results')
                correlation_id_completed = {}
                correlation_id_in_timeout = {}
                for correlation_id, log_payload in logs_to_correlate.items():
                    log_events_per_correlation_id = []
                    if len(results) > 0:
                        for result in results:
                            correlation_id_in_result = result.get('additionalColumns').get(self.correlation_id)[0]
                            if correlation_id_in_result == correlation_id:
                                log_events_per_correlation_id.append(result)
                        if len(log_events_per_correlation_id) == self.number_of_steps:
                            # all log events found. We can create the trace
                            correlation_id_completed[correlation_id] = log_events_per_correlation_id
                        elif len(log_events_per_correlation_id)>0:
                            # get the timestamp of the first element of the flow
                            timestamp = log_payload.get('timestamp')
                            utc_dt = dt.utcfromtimestamp(timestamp/1000)
                            diff = querytime - utc_dt
                            if (diff.total_seconds() > self.timeout):
                                correlation_id_in_timeout[correlation_id] = log_events_per_correlation_id
                # remove correlation_id_completed and correlation_id_in_timeout from logs_to_correlate dictionnary because they are ended
                for correlation_id in correlation_id_completed.keys():
                    del logs_to_correlate[correlation_id]
                for correlation_id in correlation_id_in_timeout.keys():
                    del logs_to_correlate[correlation_id]

                # generate traces for correlation_id_completed
                for id, value in correlation_id_completed.items():
                    logger.info("createTrace for id "+id)
                    self.executor.submit(self.createTrace, (value))

                # generate traces for correlation_id_in_timeout
                for id, value in correlation_id_in_timeout.items():
                    logger.info("createTrace for id in timeout "+id)
                    self.executor.submit(self.createTrace, (value))

                # wait until next minute
                currenttime = dt.utcnow()
                next_time = querytime + timedelta(seconds=frequency)
                delta = next_time - currenttime
                time.sleep(delta.total_seconds())
            except Exception as e:
                logger.error(e)
                # wait until next minute
                currenttime = dt.utcnow()
                next_time = querytime + timedelta(seconds=frequency)
                delta = next_time - currenttime
                time.sleep(delta.total_seconds())

if __name__ == "__main__":

    create_traces = CreateTraces(sys.argv)
    create_traces.run()


