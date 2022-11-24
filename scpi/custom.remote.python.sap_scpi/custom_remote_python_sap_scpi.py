import json
import requests
from ruxit.api.base_plugin import RemoteBasePlugin
from ruxit.api.exceptions import ConfigException
import logging
import base64
import os
import re
import time
from datetime import datetime as dt, timedelta

logger = logging.getLogger(__name__)

def datetime_from_local_to_utc(local_datetime):
    now_timestamp = time.time()
    offset = dt.fromtimestamp(now_timestamp) - dt.utcfromtimestamp(now_timestamp)
    return local_datetime - offset

CURRENT_FILE_PATH = os.path.dirname(os.path.realpath(__file__))

class SapScpi(RemoteBasePlugin):

    def initialize(self, **kwargs):

        if self.config["scpi_server"].strip() == '':
            raise ConfigException("[SCPI Server] field cannot be empty")
        self.scpi_server = self.config["scpi_server"]

        if self.config["oauth_url"].strip() == '':
            raise ConfigException("[OAuth URL to get access token] field cannot be empty")
        self.oauth_url = self.config["oauth_url"]

        if self.config["client_id"].strip() == '':
            raise ConfigException("[Client ID] field cannot be empty")
        self.client_id = self.config["client_id"]

        if self.config["secret"].strip() == '':
            raise ConfigException("[Secret] field cannot be empty")
        self.secret = self.config["secret"]

        if self.config["iflow_name"].strip() == '':
            raise ConfigException("[IFlowName] field cannot be empty")
        self.iflow_name = self.config["iflow_name"]

        if self.config["iflow_name"].strip() == '':
            raise ConfigException("[IFlowName] field cannot be empty")
        self.iflow_name = self.config["iflow_name"]

        if self.config["dt_tenant_url"].strip() == '':
            raise ConfigException("[Dynatrace Tenant URL] field cannot be empty")
        dt_tenant_url = self.config["dt_tenant_url"]
        self.dt_log_ingest_url = dt_tenant_url+ '/api/v2/logs/ingest'

        if self.config["dt_token"].strip() == '':
            raise ConfigException("[Dynatrace API Token] field cannot be empty")
        dt_token = self.config["dt_token"]
        self.dt_header = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Api-Token '+dt_token
        }
        debugLogging = self.config["debug"]
        if debugLogging:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.WARNING)
        self.previous_lines_sent = []
        self.cnt = 0

    def get_token(self):

        authorization_header = self.client_id+':'+self.secret
        authorization_header_bytes = authorization_header.encode('utf-8')
        base64_bytes = base64.b64encode(authorization_header_bytes)
        base64_authorization_header = base64_bytes.decode('utf-8')
        logger.info("base64_authorization_header = "+base64_authorization_header)

        headers = {'Content-Type': 'application/x-www-form-urlencoded',
                    'Accept': 'application/json;charset=utf-8',
                    'Authorization': 'Basic '+base64_authorization_header}
                        
        try:
            result = requests.post(self.oauth_url+'?grant_type=client_credentials', headers=headers, timeout=20)
            logger.debug(f'Token result: {result.json()}')
            if result.status_code == 200:
                content = result.json()
                self.access_token = content.get('access_token')
            else:
                logger.error(f'Failed to authenticate via OAuth to {self.oauth_url}. Error code returned: {result.status_code}. Error: {result.text}')
                # Add custom info that failed to get token
                raise ConfigException(f'Failed to authenticate via OAuth to {self.oauth_url}. Error code returned: {result.status_code}. Error: {result.text}')
            
        except requests.exceptions.RequestException as e:
            logger.exception(f'Failed to authenticate with OAuth. URL: {self.oauth_url}. {e}')
            raise ConfigException(f'Failed to authenticate with OAuth. URL: {self.oauth_url}. {e}')

    def getMessageProcessingLogs(self):
        headers = {'Content-Type': 'application/json',
                    'Accept': 'application/json;charset=utf-8',
                    'Authorization' : 'Bearer '+self.access_token}

        date_now = dt.utcnow()
        StartTime = date_now - timedelta(minutes=2)
        EndTime = date_now - timedelta(minutes=1)
        logger.info('starttime = '+StartTime.strftime("%Y-%m-%dT%H:%M:%S"))
        logger.info('endtime = '+EndTime.strftime("%Y-%m-%dT%H:%M:%S"))
        FomattedStartTime = StartTime.strftime("%Y-%m-%dT%H:%M:%S")
        FomattedEndTime = EndTime.strftime("%Y-%m-%dT%H:%M:%S")

        filter = "$filter=IntegrationFlowName eq '"+self.iflow_name+"'"+" and LogEnd gt datetime'"+FomattedStartTime+"' and LogEnd lt datetime'"+FomattedEndTime+"'"
        
        url = "https://"+self.scpi_server+"/api/v1/MessageProcessingLogs"+"?$inlinecount=allpages&"+filter+"&$select=IntegrationFlowName,LogStart,LogEnd,Status,MessageGuid,CorrelationId,LogLevel,CustomStatus,TransactionId,LocalComponentName,Receiver,Sender&$orderby=LogEnd desc"
        logger.info(url)
        try:
            response = requests.get(url, headers=headers, timeout=15)
            logger.info(f'Response code: {response.status_code}')
        except requests.exceptions.RequestException as e:
            logger.exception(f'Failed polling: {url}. {e}')
            return 'Error' # The request failed
        
        if response and response.status_code == 200:
            return response.json()
        else:
            logger.error(f'Failed to poll sap scpi. An error occurred. Response: {response}')
            return 'Error'

    def getCustomHeaderProperties(self, id):
        headers = {'Content-Type': 'application/json',
                    'Accept': 'application/json;charset=utf-8',
                    'Authorization' : 'Bearer '+self.access_token}

        url = "https://"+self.scpi_server+"/api/v1/MessageProcessingLogs('"+id+"')/CustomHeaderProperties"
        logger.info(url)
        try:
            response = requests.get(url, headers=headers, timeout=15)
            logger.info(f'Response code: {response.status_code}')
        except requests.exceptions.RequestException as e:
            logger.exception(f'Failed polling: {url}. {e}')
            return 'Error' # The request failed
        
        if response and response.status_code == 200:
            return response.json()
        else:
            logger.error(f'Failed to poll sap scpi. An error occurred. Response: {response}')
            return 'Error'

    def getErrorInformation(self, id):
        headers = {'Content-Type': 'application/json',
                    'Accept': 'application/json;charset=utf-8',
                    'Authorization' : 'Bearer '+self.access_token}

        url = "https://"+self.scpi_server+"/api/v1/MessageProcessingLogs('"+id+"')/ErrorInformation"
        logger.info(url)
        try:
            response = requests.get(url, headers=headers, timeout=15)
            logger.info(f'Response code: {response.status_code}')
        except requests.exceptions.RequestException as e:
            logger.exception(f'Failed polling: {url}. {e}')
            return 'Error' # The request failed
        
        if response and response.status_code == 200:
            return response.json()
        elif response and response.status_code == 204:
            return None
        else:
            logger.error(f'Failed to poll sap scpi. An error occurred. Response: {response}')
            return 'Error'

    def getErrorInformationValue(self, id):
        headers = {'Content-Type': 'text/plain',
                    'Accept': 'text/plain;charset=utf-8',
                    'Authorization' : 'Bearer '+self.access_token}

        url = "https://"+self.scpi_server+"/api/v1/MessageProcessingLogs('"+id+"')/ErrorInformation/$value"
        logger.info(url)
        try:
            response = None
            response = requests.get(url, headers=headers, timeout=15)
            logger.info(f'Response code: {response.status_code}')
        except requests.exceptions.RequestException as e:
            logger.exception(f'Failed polling: {url}. {e}')
            return '@Error' # The request failed
        
        if response and response.status_code == 200:
            return response.text
        elif response and response.status_code == 204:
            return None
        else:
            logger.error(f'Failed to poll sap scpi. An error occurred. Response: {response}')
            return '@Error'

    def getFormattedDateFromTimestamp(self, date):
        reg = 'Date\((.*?)\)'
        search_result = re.search(reg, date)
        if search_result:
            date = search_result.group(1)
            dateInt = int(date)
            return dt.fromtimestamp(dateInt/1000)

    def getShortId(self, fullid):
        reg = "MessageProcessingLogs\('(.*?)'\)"
        search_result = re.search(reg, fullid)
        if search_result:
            id = search_result.group(1)
            return id
        return None

    def query(self, **kwargs):
        # get a new access_token every hour because default validity of the token is 3600 seconds
        if self.cnt % 60 == 0:
            self.get_token()

        self.cnt = self.cnt + 1
        
        log_json = []
        response = self.getMessageProcessingLogs()
        if response == 'Error':
            return
        count = response.get('d').get('__count') 
        logger.info(count)
        results = response.get('d').get('results') 


        if len(results) == 0:
            return
        id_array = []
        for result in results:
            Status = result.get('Status')
            if (Status == "DISCARDED"):
                continue
            FullId = result.get('__metadata').get('id')
            Id = self.getShortId(FullId)
            IntegrationFlowName = result.get('IntegrationFlowName')
            MessageGuid = result.get('MessageGuid')
            CorrelationId = result.get('CorrelationId')
            LogLevel = result.get('LogLevel')
            CustomStatus = result.get('CustomStatus')
            TransactionId = result.get('TransactionId')
            LocalComponentName = result.get('LocalComponentName')
            Receiver = result.get('Receiver')
            Sender = result.get('Sender')
            LogStart = self.getFormattedDateFromTimestamp(result.get('LogStart'))
            LogEnd = self.getFormattedDateFromTimestamp(result.get('LogEnd'))
            logger.info("LogStart = "+str(LogStart))
            logger.info("LogEnd = "+str(LogEnd))
            log_content = { 
                "IntegrationFlowName" : IntegrationFlowName,
                "MessageGuid" : MessageGuid,
                "CorrelationId" : CorrelationId,
                "CustomStatus" : CustomStatus,
                "TransactionId" : TransactionId,
                "LocalComponentName" : LocalComponentName,
                "LogStart" : str(LogStart),
                "LogEnd" : str(LogEnd),
                "Receiver" : Receiver,
                "Sender" : Sender,
                "Status" : Status
            }
            # call scpi to get CustomHeaderProperties
            responseCustomHeaderProperties = self.getCustomHeaderProperties(Id)
            customHeaders = {}
            if responseCustomHeaderProperties != 'Error':
                customHeaderProperties = responseCustomHeaderProperties.get('d').get('results')
                for customHeaderProperty in customHeaderProperties:
                    Name = customHeaderProperty.get('Name')
                    Value = customHeaderProperty.get('Value')
                    customHeaders[Name] = Value
            log_content["CustomHeaders"] = customHeaders
            # call scpi to get ErrorInformation
            responseErrorInformation = self.getErrorInformation(Id)
            if responseErrorInformation != None and responseErrorInformation != 'Error':
                data = responseErrorInformation.get('d')
                LastErrorModelStepId = data.get('LastErrorModelStepId')
                log_content['LastErrorModelStepId'] = LastErrorModelStepId
                responseErrorValue =self.getErrorInformationValue(Id)
                if responseErrorValue != None and responseErrorValue != '@Error':
                    log_content['ErrorMessage'] = responseErrorValue

            date_utc = datetime_from_local_to_utc(LogEnd)
            log_payload = {
                "content" : json.dumps(log_content),
                "sap.cpi.IntegrationFlowName" : IntegrationFlowName,
                "sap.cpi.Status" : Status,
                "sap.cpi.LocalComponentName" : LocalComponentName,
                "sap.cpi.Receiver" : Receiver,
                "sap.cpi.Sender" : Sender,
                "sap.cpi.Server" : self.scpi_server,
                "flow.step_name" : IntegrationFlowName,
                "log.source" : "sap.cpi",
                "timestamp" : date_utc.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                "severity" : LogLevel
            }
            if log_content.get('ErrorMessage') != None:
                log_payload['flow.error_message'] = log_content.get('ErrorMessage')
            if Id not in self.previous_lines_sent:
                log_json.append(log_payload)
                id_array.append(Id)
        
        if len(log_json) > 0:
            dynatrace_response = requests.post(self.dt_log_ingest_url, json=log_json, headers=self.dt_header)
            if dynatrace_response.status_code >= 400:
                logger.error(f'Error in Dynatrace log API Response :\n'
                            f'{dynatrace_response.text}\n'
                            f'Message was :\n'
                            f'{str(log_json)}'
                            )
        self.previous_lines_sent.clear()
        for i in range(0, len(id_array)):
            self.previous_lines_sent.append(id_array[i]);     


