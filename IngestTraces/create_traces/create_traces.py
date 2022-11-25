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
import traceback

frequency=60

#logger = logging.getLogger(__name__)
# set up logging #####################################
import sys,logging,logging.handlers,os.path
#in this particular case, argv[0] is likely pythonservice.exe deep in python's lib\
# so it makes no sense to write log there
log_file="create_traces_"+sys.argv[1]+".log"
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


def dynatrace_get(uri, query_parameters, token):
    jsonContent = None
    response = None
    token_query_param = '?api-token='+token
    head = {
        'Accept': 'application/json',
        'Content-Type': 'application/json; charset=UTF-8'
    }    

    if query_parameters == None:
        response = requests.get(uri+token_query_param, headers=head, verify=True)
    else:
        response = requests.get(uri+token_query_param+query_parameters, headers=head, verify=True)
    # For successful API call, response code will be 200 (OK)
    if(response.ok):
        if(len(response.text) > 0):
            jsonContent = json.loads(response.text)
    else:
        jsonContent = json.loads(response.text)
        logger.error(response.text)
        errorMessage = ""
        if(jsonContent["error"]):
            errorMessage = jsonContent["error"]["message"]
            logger.error("Dynatrace API returned an error: " + errorMessage)
        jsonContent = None
        raise Exception("Error", "Dynatrace API returned an error: " + errorMessage)

    return jsonContent


def dynatrace_get_with_next_page_key(uri, query_parameters, array_key, token):
    PAGE_SIZE = '&pageSize=1000'
    if query_parameters == None:
        params = PAGE_SIZE
    else:
        params = query_parameters + PAGE_SIZE
    # For successful API call, response code will be 200 (OK)
    metric_list = []
    data = dynatrace_get(uri, params, token)
    metrics = data.get(array_key)

    for metric in metrics:
        metric_list.append(metric)

    nextPageKey = data.get('nextPageKey')
    if nextPageKey != None:
        while (True):
            NEXT_PAGE_KEY = '&nextPageKey='+nextPageKey
            logger.info('Get Next Page')
            data =  dynatrace_get(uri, NEXT_PAGE_KEY, token)
            nextPageKey = data.get('nextPageKey')
            metrics = data.get(array_key)
            for metric in metrics:
                metric_list.append(metric)
            if nextPageKey == None:
                break

    return metric_list

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
        self.timeout_minutes = config_json.get('timeout_minutes')        
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

    def getKey(self, logevent):
        log_source = logevent.get('additionalColumns').get('log.source')[0]
        log_content = logevent.get('content')
        log_content_json = json.loads(log_content)
        if log_source == 'demo.sap.cpi' or log_source == 'sap.cpi':
            return log_content_json.get('MessageGuid')  
        return None       

    def getRequestAttributes(self, logevent):
        requestAttributes = {}
        log_source = logevent.get('additionalColumns').get('log.source')[0]
        if log_source == 'demo.sap.cpi' or log_source == 'sap.cpi':
            if logevent.get('additionalColumns').get('sap.cpi.salesforce.trigger'):
                sap_cpi_salesforce_trigger = logevent.get('additionalColumns').get('sap.cpi.salesforce.trigger')[0]
                if sap_cpi_salesforce_trigger:
                    requestAttributes['sap.cpi.salesforce.trigger'] = sap_cpi_salesforce_trigger
        return requestAttributes

    def getHeaders(self, logevent):
        headers = {}
        # get content from logevent and parse it to json
        log_source = logevent.get('additionalColumns').get('log.source')[0]
        log_content = logevent.get('content')
        log_content_json = json.loads(log_content)
        # depending on log source, add fields as request headers
        if log_source == 'demo.sap.cpi' or log_source == 'sap.cpi':
            sap_cpi_server = logevent.get('additionalColumns').get('sap.cpi.server')[0]
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

        if log_source == 'demo.sap.edidc' or log_source == 'sap.edidc':
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
            sap_hanadb_host = logevent.get('additionalColumns').get('sap.hanadb_host')[0]
            if sap_hanadb_host:
                headers['sap.hanadb_host'] = sap_hanadb_host

        if log_source == 'demo.sap.vbak' or log_source == 'sap.vbak':
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
            sap_hanadb_host = logevent.get('additionalColumns').get('sap.hanadb_host')[0]
            if sap_hanadb_host:
                headers['sap.hanadb_host'] = sap_hanadb_host
        return headers

    def createTrace(self, params):
        trace_array = params[1]
        try:
            sdk = getsdk()
            correlation_id = None
            if trace_array[0].get('additionalColumns').get(self.correlation_id):
                correlation_id = trace_array[0].get('additionalColumns').get(self.correlation_id)[0]
            root_tracer = sdk.trace_incoming_web_request(
                self.wappinfo,
                'sales_order?',
                'POST',
                headers={},
                remote_address='')

            with root_tracer:
                # Process web 
                if correlation_id:
                    sdk.add_custom_request_attribute(self.correlation_id, correlation_id)
                message = None
                for i in range (0,len(trace_array)):
                    if trace_array[i].get('additionalColumns').get('flow.error_message') != None:
                        message = trace_array[i].get('additionalColumns').get('flow.error_message')[0]
                        if message != None:
                            break
                if params[0] == 0:
                    root_tracer.set_status_code(200) # OK
                else:
                    root_tracer.set_status_code(500) # KO
                    if message == None:
                        message = 'No error message found in flow'
                    root_tracer.mark_failed('HIP Error, imcomplete business flow', message)
                # get headers
                headers_array = []
                request_attribute_array = []
                for i in range (0,len(trace_array)):
                    headers_array.append(self.getHeaders(trace_array[i]))
                    request_attribute_array.append(self.getRequestAttributes(trace_array[i]))
                for i in range (0,len(trace_array)):
                    self.trace_outgoing_web_requests(trace_array[i], headers_array[i], request_attribute_array[i])
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

        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())
            excepthook(*sys.exc_info())

    def trace_outgoing_web_requests(self, logevent, headers, request_attributes):
        try:
            sdk = getsdk()
            correlation_id = None
            if logevent.get('additionalColumns').get(self.correlation_id) != None:
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
                if correlation_id:
                    sdk.add_custom_request_attribute(self.correlation_id, correlation_id)
                for key, value in request_attributes.items():
                    sdk.add_custom_request_attribute(key, value)
                # Here you process and send the web request.
                time.sleep(duration)
                status = logevent.get('status')

                if status == 'INFO':
                    tracer.set_status_code(200)
                else:
                    tracer.set_status_code(500)
                    if logevent.get('additionalColumns').get('flow.error_message') != None:
                        message = logevent.get('additionalColumns').get('flow.error_message')[0]
                    else:
                        message = "No error message"
                    tracer.mark_failed("HIP Error",message)
        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())
            excepthook(*sys.exc_info())

    def run(self):
        logs_to_correlate = {}
        previous_results = {}
        while (True):
            try:
                logs_without_correlation_id = {}
                querytime = dt.utcnow()
                StartTime = querytime - timedelta(minutes=5)
                EndTime = querytime
                logger.info('starttime = '+StartTime.strftime("%Y-%m-%dT%H:%M:%S"))
                logger.info('endtime = '+EndTime.strftime("%Y-%m-%dT%H:%M:%S"))
                FomattedStartTime = StartTime.strftime("%Y-%m-%dT%H:%M:%S")
                FomattedEndTime = EndTime.strftime("%Y-%m-%dT%H:%M:%S")
                # query logs to get new entry point logs
                params = "&from="+FomattedStartTime+"&to="+FomattedEndTime+"&query="+urllib.parse.quote_plus(self.entry_point_query)+"&sort=timestamp"
                uri = self.dt_url+"/api/v2/logs/export"
                logger.info(uri)
                results = dynatrace_get_with_next_page_key(uri, params, "results", self.dt_token)
                logger.info("nb results0 = "+str(len(results)))
                for result in results:
                    if result.get('additionalColumns').get(self.correlation_id) != None:
                        correlation_id = result.get('additionalColumns').get(self.correlation_id)[0]
                        if previous_results.get(correlation_id) == None:
                            logger.info("Add new correlation_id = "+correlation_id)
                            previous_results[correlation_id] = result
                            logs_to_correlate[correlation_id] = result
                    else:
                        key = self.getKey(result)
                        if key and previous_results.get(key) == None:
                            logger.info("Add element in logs_without_correlation_id = "+key)
                            previous_results[key] = result
                            logs_without_correlation_id[key] = result
                # clean previous results to remove logs older that 5 minutes
                keys_to_remove = []
                for key, value in previous_results.items():
                    timestamp = value.get('timestamp')
                    utc_dt = dt.utcfromtimestamp(timestamp/1000)
                    if utc_dt < StartTime:
                        keys_to_remove.append(key)
                for key in keys_to_remove:
                    del previous_results[key]

                # do a log search to get elements older tha now - 5minutes - timeout_minutes
                StartTime = querytime - timedelta(minutes=(5+self.timeout_minutes))
                FomattedStartTime = StartTime.strftime("%Y-%m-%dT%H:%M:%S")
                params = "&from="+FomattedStartTime+"&to="+FomattedEndTime+"&query="+urllib.parse.quote_plus(self.log_sources_to_query)+"&sort=timestamp"
                uri = self.dt_url+"/api/v2/logs/export"
                logger.info(uri)
                resultats = dynatrace_get_with_next_page_key(uri, params, "results", self.dt_token)
                logger.info("nb results1 = "+str(len(resultats)))
                correlation_id_completed = {}
                correlation_id_in_timeout = {}
                logger.info(logs_to_correlate.items())
                for correlation_id, log_payload in logs_to_correlate.items():
                    log_events_per_correlation_id = []
                    if len(resultats) > 0:
                        for result in resultats:
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
                            if (diff.total_seconds() > self.timeout_minutes*60):
                                correlation_id_in_timeout[correlation_id] = log_events_per_correlation_id
                # remove correlation_id_completed and correlation_id_in_timeout from logs_to_correlate dictionnary because they are ended
                for correlation_id in correlation_id_completed.keys():
                    del logs_to_correlate[correlation_id]
                for correlation_id in correlation_id_in_timeout.keys():
                    del logs_to_correlate[correlation_id]

                # generate traces for correlation_id_completed
                for id, value in correlation_id_completed.items():
                    logger.info("createTrace for id "+id)
                    params = [0, value]
                    self.executor.submit(self.createTrace, (params))

                # generate traces for correlation_id_in_timeout
                for id, value in correlation_id_in_timeout.items():
                    logger.info("createTrace for id in timeout "+id)
                    params = [1, value]
                    self.executor.submit(self.createTrace, (params))

                # generate traces for logs_without_correlation_id
                for id, value in logs_without_correlation_id.items():
                    logger.info("createTrace for logs_without_correlation_id "+id)
                    value_array = []
                    value_array.append(value)
                    if value.get('status') == "ERROR":
                        logger.info("Log in Error")
                        params = [1, value_array]
                    else:
                        params = [0, value_array]
                    self.executor.submit(self.createTrace, (params))

                # wait until next minute
                currenttime = dt.utcnow()
                next_time = querytime + timedelta(seconds=frequency)
                delta = next_time - currenttime
                time.sleep(delta.total_seconds())
            except Exception as e:
                logger.error(e)
                logger.error(traceback.format_exc())
                # wait until next minute
                currenttime = dt.utcnow()
                next_time = querytime + timedelta(seconds=frequency)
                delta = next_time - currenttime
                time.sleep(delta.total_seconds())

if __name__ == "__main__":

    create_traces = CreateTraces(sys.argv[1:])
    create_traces.run()


