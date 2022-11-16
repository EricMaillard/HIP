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

    executor = ThreadPoolExecutor(max_workers=20)

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
        self.service_name = config_json.get('service_name')        
        self.entry_point_query = config_json.get('entry_point_query')        
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

    def createTrace(self, trace_array):
        logger.info("createTrace")
        try:
            sdk = getsdk()
            correlation_id = trace_array[0].get('additionalColumns').get(self.correlation_id)[0]
            root_tracer = sdk.trace_custom_service("sales_order", self.service_name)
            with root_tracer:
                sdk.add_custom_request_attribute('correlation_id', correlation_id)
                for i in range (0,len(trace_array)):
                    method_name = trace_array[i].get('additionalColumns').get('flow.step_name')[0]
                    tracer = sdk.trace_custom_service(method_name,self.service_name)
                    with tracer:
                        sdk.add_custom_request_attribute('correlation_id', correlation_id)
                        time.sleep(1/1000)
                    if i < len(trace_array) - 1:
                        timestamp1 = trace_array[i].get('timestamp')
                        timestamp2 = trace_array[i+1].get('timestamp')
                        time.sleep((timestamp2-timestamp1)/1000)

        except Exception:
            excepthook(*sys.exc_info())

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
                    logger.info("createTrace for id "+id)
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


