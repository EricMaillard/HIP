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

class SapHanaDB_EDIDC(RemoteBasePlugin):

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
                    cursor.execute("SELECT ARCKEY, REFGRP, REFMES, STATUS, CRETIM, CREDAT FROM SAPDE1.EDIDC WHERE DIRECT = '2' AND MESTYP = 'ORDERS' AND SNDPRN = '0009000510' AND MESCOD = 'CRM' AND CREDAT > '"+start_day+"';")
                    results = cursor.fetchall()
                    logger.debug(results)
                except (self.dbapi.Error, self.dbapi.ProgrammingError) as e:
                    logger.exception("Error with query: " + str(e))
                try:
                    if cursor.rowcount != 0:
                        for row in results:
                            arckey = row[0]
                            refgrp = row[1]
                            refmes = row[2]
                            status = row[3]
                            cretim = row[4]
                            credat = row[5]
                            logger.debug("arckey = "+str(arckey)+";refgrp = "+str(refgrp)+";refmes = "+str(refmes)+";status = "+str(status)+";cretim = "+str(cretim)+";credat = "+str(credat))
                    else:
                        logger.debug("Nothing to return")
                except (self.dbapi.Error, self.dbapi.ProgrammingError) as e:
                    logger.exception("Metric Above had error: " + str(e))
            try:
                # IDoc ORDERS (in > ERP)
                cursor.execute("SELECT ARCKEY, REFGRP, REFMES, STATUS, CRETIM, CREDAT FROM SAPDE1.EDIDC WHERE DIRECT = '2' AND MESTYP = 'ORDERS' AND SNDPRN = '0009000510' AND MESCOD = 'CRM' AND CREDAT = '"+current_day+"' AND CRETIM between '"+FomattedStartTime+"' and '"+FomattedEndTime+"';")
                results = cursor.fetchall()
                logger.debug(results)
            except (self.dbapi.Error, self.dbapi.ProgrammingError) as e:
                logger.exception("Error with query: " + str(e))
            try:
                if cursor.rowcount != 0:
                    for row in results:
                        arckey = row[0]
                        refgrp = row[1]
                        refmes = row[2]
                        status = row[3]
                        cretim = row[4]
                        credat = row[5]
                        date = ""+credat[0:4]+"-"+credat[4:6]+"-"+credat[6:8]+"T"+cretim[0:2]+":"+cretim[2:4]+":"+cretim[4:6]
                        dt_date = dt.strptime(date, "%Y-%m-%dT%H:%M:%S")
                        date_local = datetime_from_utc_to_local(dt_date)
                        log_content = { 
                            "salesforce.BusinessDocumentNumber" : refgrp,
                            "MessageGuid" : arckey,
                            "salesforce.ReplayId " : refmes,
                            "timestamp" : str(date_local),
                            "Status" : status
                        }
                        if status == "53":
                            LogLevel = "INFO"
                        else:
                            LogLevel = "ERROR"
                        log_payload = {
                            "content" : json.dumps(log_content),
                            "sap.edidc.DIRECT" : 2,
                            "sap.edidc.MESTYP" : "ORDERS",
                            "sap.edidc.SNDPRN" : "0009000510",
                            "sap.edidc.MESCOD" : "CRM",
                            "sap.HANADB_HOST" : self.host,
                            "flow.step_name" :"IDoc_Order_In_EDIDC",
                            "log.source" : "sap.edidc",
                            "timestamp" : dt_date.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                            "severity" : LogLevel
                        }
                        log_json.append(log_payload)
                else:
                    logger.debug("Nothing to return")
            except (self.dbapi.Error, self.dbapi.ProgrammingError) as e:
                logger.exception("Metric Above had error: " + str(e))
            try:
                # IDoc ORDACK (ERP > out)
                cursor.execute("SELECT ARCKEY, REFGRP, REFMES, STATUS, CRETIM, CREDAT FROM SAPDE1.EDIDC WHERE DIRECT = '1' AND MESTYP = 'ORDACK' AND SNDPRN = '0009000510' AND MESCOD = 'CRM' AND CREDAT = '"+current_day+"' AND CRETIM between '"+FomattedStartTime+"' and '"+FomattedEndTime+"';")
                results = cursor.fetchall()
                logger.debug(results)
            except (self.dbapi.Error, self.dbapi.ProgrammingError) as e:
                logger.exception("Error with query: " + str(e))
            try:
                if cursor.rowcount != 0:
                    for row in results:
                        arckey = row[0]
                        refgrp = row[1]
                        refmes = row[2]
                        status = row[3]
                        cretim = row[4]
                        credat = row[5]
                        date = ""+credat[0:4]+"-"+credat[4:6]+"-"+credat[6:8]+"T"+cretim[0:2]+":"+cretim[2:4]+":"+cretim[4:6]
                        dt_date = dt.strptime(date, "%Y-%m-%dT%H:%M:%S")
                        date_local = datetime_from_utc_to_local(dt_date)
                        log_content = { 
                            "salesforce.BusinessDocumentNumber" : refgrp,
                            "MessageGuid" : arckey,
                            "salesforce.ReplayId " : refmes,
                            "timestamp" : str(date_local),
                            "Status" : status
                        }
                        if status == "03" or status == "41":
                            LogLevel = "INFO"
                        else:
                            LogLevel = "ERROR"
                        log_payload = {
                            "content" : json.dumps(log_content),
                            "sap.edidc.DIRECT" : 1,
                            "sap.edidc.MESTYP" : "ORDACK",
                            "sap.edidc.SNDPRN" : "0009000510",
                            "sap.edidc.MESCOD" : "CRM",
                            "sap.HANADB_HOST" : self.host,
                            "flow.step_name" :"IDoc_Order_Out_EDIDC",
                            "log.source" : "sap.edidc",
                            "timestamp" : dt_date.strftime("%Y-%m-%dT%H:%M:%S.%f"),
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

