# *****************************************************************************
# © Copyright IBM Corp. 2018.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import os
import tempfile
import dill
import logging
import requests
import ibm_boto3
from ibm_boto3.s3.transfer import S3Transfer
from ibm_botocore.client import Config

def cosSave(obj,bucket,filename,credentials):
    try:
        fhandle, fname = tempfile.mkstemp("cosfile")
        os.close(fhandle) 
        with open(fname, 'wb') as file_obj:
            dill.dump(obj, file_obj)
        transfer = getCosTransferAgent(credentials)
        transfer.upload_file(fname, bucket, filename)
        os.unlink(fname)
    except Exception as ex:
        logging.exception(ex)
    return filename

def cosLoad(bucket,filename,credentials):
    try:
        fhandle, fname = tempfile.mkstemp("cosfile")
        os.close(fhandle)
        transfer = getCosTransferAgent(credentials)
        transfer.download_file(bucket, filename, fname)
        answer = None
        with open(fname, 'rb') as file_obj:
            answer = dill.load(file_obj)
        os.unlink(fname)
        return answer
    except Exception as ex:
        logging.exception(ex)

def getCosTransferAgent(credentials):
    endpoints = requests.get(credentials.get('endpoints')).json()
    iam_host = (endpoints['identity-endpoints']['iam-token'])
    cos_host = (endpoints['service-endpoints']['cross-region']['us']['public']['us-geo'])
    api_key = credentials.get('apikey')
    service_instance_id = credentials.get('resource_instance_id')
    auth_endpoint = "https://" + iam_host + "/oidc/token"
    service_endpoint = "https://" + cos_host
    cos = ibm_boto3.client('s3',
                           ibm_api_key_id=api_key,
                           ibm_service_instance_id=service_instance_id,
                           ibm_auth_endpoint=auth_endpoint,
                           config=Config(signature_version='oauth'),
                           endpoint_url=service_endpoint)
    return S3Transfer(cos)

