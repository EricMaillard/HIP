from ruxit.api.base_plugin import RemoteBasePlugin
from ruxit.api.exceptions import ConfigException, AuthException

import logging
import json
import requests
import importlib
import time
from datetime import datetime as dt, timedelta

logger = logging.getLogger(__name__)

def datetime_from_utc_to_local(utc_datetime):
    now_timestamp = time.time()
    offset = dt.fromtimestamp(now_timestamp) - dt.utcfromtimestamp(now_timestamp)
    return utc_datetime + offset

class SapHanaDB_VBAK(RemoteBasePlugin):

    def initialize(self, **kwargs):
        try:
            self.dbapi = importlib.import_module("hdbcli.dbapi")
        except ImportError as e:
            logger.exception(e)
            raise ConfigException("Unable to load hdbcli Library. See Documentation.")
        if str(self.config["host"]).strip() != '':
            self.host = str(self.config["host"]).strip()
        else:
            raise ConfigException("Host cannot be empty")

        if self.config["port"] != '':
            self.port = self.config["port"]
        else:
            raise ConfigException("Port cannot be empty")

        if str(self.config["auth_user"]).strip() != '':
            self.user = str(self.config["auth_user"]).strip()
        else:
            raise ConfigException("User cannot be empty")

        if str(self.config["auth_password"]).strip() != '':
            self.password = str(self.config["auth_password"]).strip()
        else:
            raise ConfigException("Password cannot be empty")

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

        if self.config['debug']:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

    def query(self, **kwargs):
        try:
            start = time.perf_counter()
            connection = self.dbapi.connect(address=self.host, port=self.port, user=self.user, password=self.password,
                                            encrypt=True, sslValidateCertificate=False, connecttimeout=15000, communicationTimeout=10000)
            self.doRequest(connection)
            connection.close()
            end = time.perf_counter()
            logger.info(f"Execution took : {end-start:0.4f} seconds")
        except self.dbapi.Error as e:
            # See SAP Error Codes
            # https://help.sap.com/viewer/4fe29514fd584807ac9f2a04f6754767/2.0.03/en-US/20a78d3275191014b41bae7c4a46d835.html
            if str(e).startswith("(10,"):
                logger.exception("Authentication Issue: " + str(e))
                raise AuthException("Authentication Issue: " + str(e))
            elif str(e).startswith("(414,") or str(e).startswith("(415,") or str(e).startswith("(416,") or str(e).startswith("(663,"):
                logger.exception("Authorization Issue: " + str(e))
                raise AuthException("Authorization Issue: " + str(e))
            else:
                raise ConfigException("Could not connect to supplied Host and Port")


    def doRequest(self, connection):
        logger.info("doing request")
        date_now = dt.utcnow()
        StartTime = date_now - timedelta(minutes=2)
        EndTime = date_now - timedelta(minutes=1)
        logger.debug('starttime = '+StartTime.strftime("%H%M%S"))
        logger.debug('endtime = '+EndTime.strftime("%H%M%S"))
        logger.debug('currentdate = '+EndTime.strftime("%Y%m%d"))
        FomattedStartTime = StartTime.strftime("%H%M%S")
        FomattedEndTime = EndTime.strftime("%H%M%S")
        current_day = EndTime.strftime("%Y%m%d")
        log_json = []

        with connection.cursor() as cursor:
            if self.config['debug']:
                try:
                    start_day = "20221101"
                    cursor.execute("SELECT VBELN, YYEDI_CM_HUB_PO_NUMBER, YYEDI_CM_MSGST_51, YYEDI_CM_MSGST_69, ERDAT, ERZET FROM SAPDE1.VBAK WHERE BSARK='018' AND ERDAT > '"+start_day+"';")
                    results = cursor.fetchall()
                    logger.debug(results)
                except (self.dbapi.Error, self.dbapi.ProgrammingError) as e:
                    logger.exception("Error with query: " + str(e))
                try:
                    if cursor.rowcount != 0:
                        for row in results:
                            VBELN = row[0]
                            YYEDI_CM_HUB_PO_NUMBER = row[1]
                            YYEDI_CM_MSGST_51 = row[2]
                            YYEDI_CM_MSGST_69 = row[3]
                            ERDAT = row[4]
                            ERZET = row[5]
                            logger.debug("VBELN = "+str(VBELN)+";YYEDI_CM_HUB_PO_NUMBER = "+str(YYEDI_CM_HUB_PO_NUMBER)+";YYEDI_CM_MSGST_51 = "+str(YYEDI_CM_MSGST_51)+";YYEDI_CM_MSGST_69 = "+str(YYEDI_CM_MSGST_69)+";ERZET = "+str(ERZET)+";ERDAT = "+str(ERDAT))
                    else:
                        logger.debug("Nothing to return")
                except (self.dbapi.Error, self.dbapi.ProgrammingError) as e:
                    logger.exception("Metric Above had error: " + str(e))
            try:
                # Sales Documents in VBAK
                cursor.execute("SELECT VBELN, YYEDI_CM_HUB_PO_NUMBER, YYEDI_CM_MSGST_51, YYEDI_CM_MSGST_69, ERDAT, ERZET FROM SAPDE1.VBAK WHERE BSARK='018' AND ERDAT = '"+current_day+"' AND ERZET between '"+FomattedStartTime+"' and '"+FomattedEndTime+"';")
                results = cursor.fetchall()
                logger.debug(results)
            except (self.dbapi.Error, self.dbapi.ProgrammingError) as e:
                logger.exception("Error with query: " + str(e))
            try:
                if cursor.rowcount != 0:
                    for row in results:
                        VBELN = row[0]
                        YYEDI_CM_HUB_PO_NUMBER = row[1]
                        YYEDI_CM_MSGST_51 = row[2]
                        YYEDI_CM_MSGST_69 = row[3]
                        ERDAT = row[4]
                        ERZET = row[5]
                        date = ""+ERDAT[0:4]+"-"+ERDAT[4:6]+"-"+ERDAT[6:8]+"T"+ERZET[0:2]+":"+ERZET[2:4]+":"+ERZET[4:6]
                        dt_date = dt.strptime(date, "%Y-%m-%dT%H:%M:%S")
                        date_local = datetime_from_utc_to_local(dt_date)
                        log_content = { 
                            "salesforce.BusinessDocumentNumber" : YYEDI_CM_HUB_PO_NUMBER,
                            "sap.vbak.num_command" : VBELN,
                            "timestamp" : str(date_local),
                            "sap.vbak.YYEDI_CM_MSGST_51" : YYEDI_CM_MSGST_51,
                            "sap.vbak.YYEDI_CM_MSGST_69" : YYEDI_CM_MSGST_69
                        }
                        if YYEDI_CM_MSGST_51 != "":
                            LogLevel = "WARN"
                        elif YYEDI_CM_MSGST_69 != "":
                            LogLevel = "WARN"
                        else:
                            LogLevel = "INFO"
                        log_payload = {
                            "content" : json.dumps(log_content),
                            "sap.vbak.BSARK" : "018",
                            "sap.HANADB_HOST" : self.host,
                            "flow.step_name" :"Sales_Document_Creation_in_VBAK",
                            "log.source" : "sap.vbak",
                            "timestamp" : date,
                            "severity" : LogLevel
                        }
                        log_json.append(log_payload)
                else:
                    logger.debug("Nothing to return")
            except (self.dbapi.Error, self.dbapi.ProgrammingError) as e:
                logger.exception("Metric Above had error: " + str(e))
        # send log lines to Dynatrace        
        if len(log_json) > 0:
            dynatrace_response = requests.post(self.dt_log_ingest_url, json=log_json, headers=self.dt_header)
            if dynatrace_response.status_code >= 400:
                logger.error(f'Error in Dynatrace log API Response :\n'
                            f'{dynatrace_response.text}\n'
                            f'Message was :\n'
                            f'{str(log_json)}'
                            )

