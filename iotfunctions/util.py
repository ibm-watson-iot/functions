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
import dill as pickle
import requests
import datetime
import hashlib
import hmac
import re
import pandas as pd
import random
import string
import logging
import errno
import sys
import json
import threading
import datetime as dt
import time

from urllib.parse import quote, urlparse
from base64 import b64encode
from lxml import etree

logger = logging.getLogger(__name__)

try:
    from confluent_kafka import Producer
except ImportError:
    logger.warning('Warning: confluent_kafka is not installed. Publish to MessageHub not supported.')
    KAFKA_INSTALLED = False
else:
    KAFKA_INSTALLED = True

try:
    import ibm_boto3
    from ibm_boto3.s3.transfer import S3Transfer
    from ibm_botocore.client import Config
except BaseException:
    IBMBOTO_INSTALLED = False
    logger.info('ibm_boto3 is not installed. Use HMAC credentials to communicate with COS.')
else:
    IBMBOTO_INSTALLED = True

FLUSH_PRODUCER_EVERY = 500

MH_USER = os.environ.get('MH_USER')
MH_PASSWORD = os.environ.get('MH_PASSWORD')
MH_BROKERS_SASL = os.environ.get('MH_BROKERS_SASL')
MH_DEFAULT_ALERT_TOPIC = os.environ.get('MH_DEFAULT_ALERT_TOPIC')
MH_CLIENT_ID = 'as-pipeline-alerts-producer'


def adjust_probabilities(p_list):
    '''
    Adjust a list of probabilties to ensure that they sum to 1
    '''

    if p_list is None or len(p_list) == 0:
        out = None
    else:
        total = sum(p_list)
        if total == 1:
            out = p_list
        elif total == 0:
            raise ValueError('Probabilities may not sum to zero')
        else:
            out = [x / total for x in p_list]

    return out


def build_grouper(freq, timestamp, entity_id=None, dimensions=None, custom_calendar_keys=None, ):
    '''
    Build a pandas grouper from columns and frequecy metadata.
    
    Parameters
    -----------
    
    freq : str
    pandas frequency string
    
    timestamp: str
    name of timestamp column to group by
    
    entity_id: str
    column name for the entity_id if entity id is included in group by
    e.g. device_id

    dimensions: list of strs
    column names for the dimensions to be included in group by
    e.g. ['company','city']

    custom_calendar_keys: list of strs
    column names for the custom calendar keys to be included in group by
    e.g. ['shift']
    
    '''

    grouper = []

    if dimensions is None:
        dimensions = []

    if entity_id is not None:
        grouper.append(pd.Grouper(key=entity_id))

    grouper.append(pd.Grouper(key=timestamp, freq=freq))
    for d in dimensions:
        grouper.append(pd.Grouper(key=d))

    return grouper


def categorize_args(categories, catch_all, *args):
    '''
    Separate objects passed as arguments into a dictionary of categories
    
    members of categories are identified by a bool property or by
    being instances of a class
    
    example:
    
    categories = [('constant','is_ui_control',None),
                  ('granularity','is_granularity',None),
                  ('function','is_function',None),
                  ('column',None,Column)]
    '''

    meta = {}
    if catch_all is not None:
        meta[catch_all] = set()
    logger.debug('categorizing arguments')
    uncat = set(args)
    for (group, attr, cls) in categories:
        meta[group] = set()

        for a in args:
            if attr is not None:
                try:
                    is_group = getattr(a, attr)
                except AttributeError:
                    is_group = False
            else:
                is_group = False

            if is_group:
                meta[group].add(a)
                uncat.remove(a)
            elif cls is not None and isinstance(a, cls):
                meta[group].add(a)
                uncat.remove(a)

    for a in uncat:
        meta[catch_all].add(a)

    return meta


def compare_dataframes(dfl, dfr, cols=None):
    '''
    Explain the differences between 2 dataframes
    '''

    if cols is None:
        cols = list(dfl.columns)

    differences = 0
    trace = ''
    if len(dfl.index) != len(dfr.index):
        msg = 'Row count: %s vs %s' % (dfl.index, dfr.index)
        trace = trace + msg
        differences += abs(len(dfl.index) - len(dfr.index))
    missing_l = set(cols) - set(dfl.columns)
    if len(missing_l) != 0:
        msg = 'dfl is missing columns:' % (missing_l)
        trace = trace + msg
        cols = [x for x in cols if x not in missing_l]
        differences += len(missing_l) * len(dfl.index)
    missing_r = set(cols) - set(dfr.columns)
    if len(missing_r) != 0:
        msg = 'dfr is missing columns: %s' % (missing_r)
        trace = trace + msg
        cols = [x for x in cols if x not in missing_r]
        differences += len(missing_r) * len(dfr.index)

    dfl = dfl[cols].reindex()
    dfr = dfr[cols].reindex()

    dfs = {'dfl': dfl, 'dfr': dfr}
    df = pd.concat(dfs)
    total_rows = len(df.index)
    df = df.drop_duplicates(keep=False)
    if total_rows - len(df.index) > 0:
        msg = 'Rows with different contents:%s' % (total_rows - len(df.index))
        trace = trace + msg
        differences = differences + total_rows - len(df.index)

    return (differences, trace, df)


def reset_df_index(df, auto_index_name='_auto_index_'):
    '''
    Reset the data dataframe index. Ignore duplicate columns.
    '''

    # if the dataframe has an auto index, do not place it in the dataframe
    if len([x for x in df.index.names if x is not None]) > 0:
        drop = False
    elif df.index.name is None or df.index.name == auto_index_name:
        drop = True
    else:
        drop = False

    # drop any duplicate columns that exist in index and df
    try:
        df = df.reset_index(inplace=False, drop=drop)  # do not propregate
    except ValueError:
        index_names = get_index_names(df)
        dup_names = set(index_names).intersection(set(df.columns))
        for i in dup_names:
            df = df.drop(columns=[i])
            logger.debug('Dropped duplicate column name %s while resetting index', i)

        try:
            df = df.reset_index(inplace=False, drop=drop)  # do not propregate
        except ValueError:
            msg = ('There is a problem with the dataframe index. '
                   ' Cant reset as reset caused overlap in col names'
                   ' index: %s, cols: %s' % (df.index.names, df.columns))
            raise RuntimeError(msg)

    return df


def resample(df, time_frequency, timestamp, dimensions=None, agg=None, default_aggregate='last'):
    '''
    Resample a dataframe to a new time grain / dimensional grain
    
    Parameters:
    -----------
    df: Pandas dataframe
        Dataframe to resample
    time_frequency: str
        Pandas frequency string
    dimensions: list of strs
        List of columns to group by
    agg : dict
        Pandas aggregate dictionary
    default_aggregate: str
        Default aggregation function to apply for anything not specified in agg
    
    Returns
    -------
    Pandas dataframe
    
    '''
    if dimensions is None:
        dimensions = []
    if agg is None:
        agg = {}

    df = df.reset_index()

    index_cols = [timestamp]
    index_cols.extend(dimensions)
    for r in [x for x in df.columns if x not in index_cols]:
        try:
            agg[r]
        except KeyError:
            agg[r] = default_aggregate

    group_base = [pd.Grouper(key=timestamp, freq=time_frequency)]
    for d in dimensions:
        group_base.append(pd.Grouper(key=d))

    df = df.groupby(group_base).agg(agg)
    df.reset_index(inplace=True)

    return df


def freq_to_timedelta(freq):
    '''
    The pandas to_timedelta does not handle the full set of
    set of pandas frequency abreviations. Convert to supported
    abreviation and the use to_timedelta.
    '''
    try:
        freq = freq.replace('T', 'min')
    except AttributeError:
        pass
    return (pd.to_timedelta(freq))


def randomword(length):
    letters = string.ascii_lowercase + string.digits
    return ''.join(random.choice(letters) for i in range(length))


def cosSave(obj, bucket, filename, credentials):
    '''
    Use IAM credentials to write an object to Cloud Object Storage
    '''
    try:
        fhandle, fname = tempfile.mkstemp("cosfile")
        os.close(fhandle)
        with open(fname, 'wb') as file_obj:
            pickle.dump(obj, file_obj)
        transfer = getCosTransferAgent(credentials)
        transfer.upload_file(fname, bucket, filename)
        os.unlink(fname)

    except Exception as ex:
        logging.exception(ex)
    return filename


def cosLoad(bucket, filename, credentials):
    '''
    Use IAM credentials to read an object from Cloud Object Storage
    '''
    try:
        fhandle, fname = tempfile.mkstemp("cosfile")
        os.close(fhandle)
        transfer = getCosTransferAgent(credentials)
        transfer.download_file(bucket, filename, fname)
        answer = None
        with open(fname, 'rb') as file_obj:
            answer = pickle.load(file_obj)
        os.unlink(fname)
        return answer

    except Exception as ex:
        logging.exception(ex)


def getCosTransferAgent(credentials):
    '''
    Use IAM credentials to obtain a Cloud Object Storage transfer agent object
    '''
    if IBMBOTO_INSTALLED:
        endpoints = requests.get(credentials.get('endpoints')).json()
        iam_host = (endpoints['identity-endpoints']['iam-token'])
        cos_host = (endpoints['service-endpoints']['cross-region']['us']['public']['us-geo'])
        api_key = credentials.get('apikey')
        service_instance_id = credentials.get('resource_instance_id')
        auth_endpoint = "https://" + iam_host + "/oidc/token"
        service_endpoint = "https://" + cos_host
        cos = ibm_boto3.client('s3', ibm_api_key_id=api_key, ibm_service_instance_id=service_instance_id,
                               ibm_auth_endpoint=auth_endpoint, config=Config(signature_version='oauth'),
                               endpoint_url=service_endpoint)
        return S3Transfer(cos)
    else:
        raise ValueError(
            'Attempting to use IAM credentials to communicate with COS. IBMBOTO is not installed. You make use HMAC credentials and the CosClient instead.')


def get_index_names(df):
    '''
    Get names from either single or multi-part index
    '''

    if df.index.name is not None:
        df_index_names = [df.index.name]
    else:
        df_index_names = list(df.index.names)

    df_index_names = [x for x in df_index_names if x is not None]

    return df_index_names


def infer_data_items(expressions):
    '''
    Examine a pandas expression or list of expressions. Identify data items
    in the expressions by looking for df['<data_item>'].

    Returns as set of strings.
    '''
    if not isinstance(expressions, list):
        expressions = [expressions]
    regex1 = "df\[\'(.+?)\'\]"
    regex2 = 'df\[\"(.+?)\"\]'
    data_items = set()
    for e in expressions:
        data_items |= set(re.findall(regex1, e))
        data_items |= set(re.findall(regex2, e))
    return (data_items)


def get_fn_expression_args(function_metadata, kpi_metadata):
    '''
    Examine a functions metadata dictionary. Identify data items used
    in any expressions that the function has.

    '''

    expressions = []
    args = kpi_metadata.get('input', {})

    for (arg, value) in list(args.items()):
        if arg == 'expression':
            expressions.append(value)
            logger.debug('Found expression %s', value)

    return infer_data_items(expressions)


def log_df_info(df, msg, include_data=False):
    '''
    Log a debugging entry showing first row and index structure
    '''
    try:
        msg = msg + ' df count: %s ' % (len(df.index))
        if df.index.names != [None]:
            msg = msg + ' ; index: %s ' % (','.join(df.index.names))
        else:
            msg = msg + ' ; index is unnamed'
        if include_data:
            msg = msg + ' ; 1st row: '
            try:
                cols = df.head(1).squeeze().to_dict()
                for key, value in list(cols.items()):
                    msg = msg + '%s : %s, ' % (key, value)
            except AttributeError:
                msg = msg + str(df.head(1))
        else:
            msg = msg + ' ; columns: %s' % (','.join(list(df.columns)))
        logger.debug(msg)
        return msg
    except Exception:
        logger.warning('dataframe contents not logged due to an unknown logging error')
        return ''


def asList(x):
    if not isinstance(x, list):
        x = [x]
    return x


class CosClient:
    '''
    Cloud Object Storage client
    '''

    def __init__(self, credentials):
        self._cod_hmac_access_key_id = credentials['objectStorage']['username']
        self._cod_hmac_secret_access_key = credentials['objectStorage']['password']
        self._cos_region = credentials['objectStorage']['region']
        self._cos_endpoint = credentials['config']['objectStorageEndpoint']

        if self._cos_region is None or len(self._cos_region.strip()) == 0:
            self._cos_region = 'any-region'

    # hashing and signing methods
    def _hash(self, key, msg):
        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

    # region is a wildcard value that takes the place of the AWS region value
    # as COS doen't use the same conventions for regions, this parameter can accept any string
    def _create_signature_key(self, key, datestamp, region, service):
        keyDate = self._hash(('AWS4' + key).encode('utf-8'), datestamp)
        keyString = self._hash(keyDate, region)
        keyService = self._hash(keyString, service)
        keySigning = self._hash(keyService, 'aws4_request')
        return keySigning

    def _cos_api_request(self, http_method, bucket, key, request_parameters=None, payload='', extra_headers=None,
                         binary=False):
        if extra_headers is None:
            extra_headers = {}
        # it seems region is not used by IBM COS and can be any string (but cannot be None below still)
        if any([(var is None or len(var.strip()) == 0) for var in
                [self._cod_hmac_access_key_id, self._cod_hmac_secret_access_key, self._cos_endpoint, bucket]]):
            logger.warning('write COS is disabled because not all COS config environment variables are set')
            return None
        # assemble the standardized request

        time = datetime.datetime.utcnow()
        timestamp = time.strftime('%Y%m%dT%H%M%SZ')
        datestamp = time.strftime('%Y%m%d')

        url = urlparse(self._cos_endpoint)
        host = url.netloc

        payload_hash = hashlib.sha256(str.encode(payload) if isinstance(payload, str) else payload).hexdigest()
        standardized_resource = '/'
        if bucket is not None:
            standardized_resource += bucket
        if key is not None:
            standardized_resource += '/' + key
        if request_parameters is None:
            standardized_querystring = ''
        else:
            standardized_querystring = '&'.join(
                ['%s=%s' % (quote(k, safe=''), quote(v, safe='')) for k, v in request_parameters.items()])

        all_headers = {'host': host, 'x-amz-content-sha256': payload_hash, 'x-amz-date': timestamp}
        all_headers.update({k.lower(): v for k, v in extra_headers.items()})
        standardized_headers = ''
        for header in sorted(all_headers.keys()):
            standardized_headers += '%s:%s\n' % (header, all_headers[header])
        signed_headers = ';'.join(sorted(all_headers.keys()))
        # standardized_headers = 'host:' + host + '\n' + 'x-amz-content-sha256:' + payload_hash + '\n' + 'x-amz-date:' + timestamp + '\n'
        # signed_headers = 'host;x-amz-content-sha256;x-amz-date'

        standardized_request = (
                http_method + '\n' + standardized_resource + '\n' + standardized_querystring + '\n' + standardized_headers + '\n' + signed_headers + '\n' + payload_hash)

        # logging.debug('standardized_request=\n%s' % standardized_request)

        # assemble string-to-sign
        hashing_algorithm = 'AWS4-HMAC-SHA256'
        credential_scope = datestamp + '/' + self._cos_region + '/' + 's3' + '/' + 'aws4_request'
        sts = (hashing_algorithm + '\n' + timestamp + '\n' + credential_scope + '\n' + hashlib.sha256(
            str.encode(standardized_request)).hexdigest())

        # logging.debug('string-to-sign=\n%s' % sts)

        # generate the signature
        signature_key = self._create_signature_key(self._cod_hmac_secret_access_key, datestamp, self._cos_region, 's3')
        signature = hmac.new(signature_key, (sts).encode('utf-8'), hashlib.sha256).hexdigest()

        # logging.debug('signature=\n%s' % signature)

        # assemble all elements into the 'authorization' header
        v4auth_header = (
                hashing_algorithm + ' ' + 'Credential=' + self._cod_hmac_access_key_id + '/' + credential_scope + ', ' + 'SignedHeaders=' + signed_headers + ', ' + 'Signature=' + signature)

        # logging.debug('v4auth_header=\n%s' % v4auth_header)

        # the 'requests' package autmatically adds the required 'host' header
        headers = all_headers.copy()
        headers.pop('host')
        headers['Authorization'] = v4auth_header
        # headers = {'x-amz-content-sha256': payload_hash, 'x-amz-date': timestamp, 'Authorization': v4auth_header}

        if standardized_querystring == '':
            request_url = self._cos_endpoint + standardized_resource
        else:
            request_url = self._cos_endpoint + standardized_resource + '?' + standardized_querystring

        # logging.debug('request_url=%s' % request_url)

        if http_method == 'GET':
            resp = requests.get(request_url, headers=headers, timeout=30, verify=False)
        elif http_method == 'DELETE':
            resp = requests.delete(request_url, headers=headers, timeout=30, verify=False)
        elif http_method == 'POST':
            resp = requests.post(request_url, headers=headers, data=payload, timeout=30, verify=False)
        elif http_method == 'PUT':
            resp = requests.put(request_url, headers=headers, data=payload, timeout=30, verify=False)
        else:
            raise RuntimeError('unsupported_http_method=%s' % http_method)

        if resp.status_code != requests.codes.ok and not (
                resp.status_code == requests.codes.no_content and http_method == 'DELETE'):
            logger.warning('error cos_api_request: request_url=%s, http_method=%s, status_code=%s, response_text=%s' % (
                request_url, http_method, str(resp.status_code), str(resp.text)))
            return None

        return resp.content if binary else resp.text

    def cos_get(self, key, bucket, request_parameters=None, binary=False):

        response = self._cos_api_request('GET', bucket=bucket, key=key, request_parameters=request_parameters,
                                         binary=binary)
        if response is not None:
            response = pickle.loads(response)

        return response

    def cos_find(self, prefix, bucket):
        result = self.cos_get(key=None, bucket=bucket, request_parameters={'list-type': '2', 'prefix': prefix})
        if result is None:
            return []

        root = etree.fromstring(str.encode(result))
        return [elem.text for elem in root.findall('Contents/Key', root.nsmap)]

    def cos_put(self, key, payload, bucket, binary=False, serialize=True):
        if payload is not None:
            if serialize:
                payload = pickle.dumps(payload)
        else:
            payload = ''

        return self._cos_api_request('PUT', bucket=bucket, key=key, payload=payload, binary=binary)

    def cos_delete(self, key, bucket):
        return self._cos_api_request('DELETE', bucket=bucket, key=key)

    def cos_delete_multiple(self, keys, bucket):
        if keys is None or len(keys) == 0:
            return ''

        payload = '<?xml version="1.0" encoding="UTF-8"?><Delete>'
        for key in keys:
            payload += '<Object><Key>%s</Key></Object>' % key
        payload += '</Delete>'

        md5 = hashlib.md5(str.encode(payload)).digest()
        base64 = b64encode(md5).decode()
        logger.debug('content-md5: %s' % base64)

        extra_headers = {'Content-Type': 'text/plain; charset=utf-8', 'Content-MD5': base64}

        request_parameters = {'delete': ''}
        return self._cos_api_request('POST', bucket=bucket, key=None, payload=payload,
                                     request_parameters=request_parameters, extra_headers=extra_headers)


class MemoryOptimizer:
    '''
    Util class used to optimize the pipeline memory consumption using native Pandas downcasting
    '''

    def printCurrentMemoryConsumption(self, df):
        logger.info('Memory consumed by the data frame: \n %s' % df.memory_usage(deep=True))

    def printUsagePerType(self, df):
        for dtype in ['float', 'int', 'object']:
            selected_dtype = df.select_dtypes(include=[dtype])
            mean_usage_b = selected_dtype.memory_usage(deep=True).mean()
            mean_usage_mb = mean_usage_b / 1024 ** 2
            logger.info("Average memory usage for {} columns: {:03.2f} MB".format(dtype, mean_usage_mb))

    def downcastInteger(self, df):
        df_new = df.copy()

        logger.info('Applying downcast to Integer columns.')

        try:
            df_int = df_new.select_dtypes(include=['int'])

            if not df_int.empty:
                df_converted = df_int.apply(pd.to_numeric, downcast='unsigned')
                for col in df_converted.columns:
                    df_new[col] = df_converted[col]
        except:
            logger.warning('Not able to downcast Integer')
            return df_new

        return df_new

    def downcastFloat(self, df, precison='float'):
        df_new = df.copy()

        logger.info('Applying downcast to Float columns.')

        try:
            df_float = df_new.select_dtypes(include=['float'])

            if not df_float.empty:
                df_converted = df_float.apply(pd.to_numeric, downcast=precison)
                for col in df_converted.columns:
                    df_new[col] = df_converted[col]
        except:
            logger.warning('Not able to downcast Float types')
            return df_new

        return df_new

    def getColumnsForCategorization(self, df, threshold=0.5):
        '''
        It generates a list of columns that are elegible to be categorized.
        The column name is printed if the number of unique values is proportionally greater than 50% of the total number of rows.
        Threshold is customized.
        '''

        df_new = df.select_dtypes(include=['object']).copy()

        lst_columns = []
        for col in df_new.columns:
            num_unique_values = len(df_new[col].unique())
            num_total_values = len(df_new[col])
            if num_unique_values / num_total_values < threshold:
                logger.info('Column elegible for categorization: %s' % col)
                lst_columns.append(col)

        return lst_columns

    def downcastString(self, df, lst_columns):
        '''
        It converts a data frame column type object into a categorical type
        '''

        logger.info('Applying downcast to String columns. %s' % str(lst_columns))

        df_new = df.select_dtypes(include=['object']).copy()

        try:
            for col in lst_columns:
                df_new.loc[:, col] = df_new[col].astype('category')
        except:
            logger.warning('Not able to downcast String to category')
            return df

        return df_new

    def downcastNumeric(self, df):

        logger.info('Optimizing memory. Before applying downcast.')
        self.printUsagePerType(df)
        self.printCurrentMemoryConsumption(df)

        df_new = self.downcastInteger(df)
        df_new = self.downcastFloat(df_new)

        logger.info('Optimizing memory. After applying downcast.')
        self.printUsagePerType(df_new)
        self.printCurrentMemoryConsumption(df_new)

        return df_new


class MessageHub:
    MH_CA_CERT_PATH = '/etc/ssl/certs/eventstreams.pem'
    MH_CA_CERT = os.environ.get('MH_CA_CERT')

    def _delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            logger.warning('Message delivery failed: {}'.format(err))

    # else:
    #     logger.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    def produce_batch_alert_to_default_topic(self, key_and_msg):
        self.produce_batch(topic=MH_DEFAULT_ALERT_TOPIC, key_and_msg=key_and_msg)

    def produce_batch(self, topic, key_and_msg):
        start_time = dt.datetime.now()
        if topic is None or len(topic) == 0 or key_and_msg is None:
            return

        counter = 0
        producer = None
        for key, msg in key_and_msg:
            producer = self.produce(topic, msg=msg, key=key, producer=producer)
            counter += 1
            if counter % FLUSH_PRODUCER_EVERY == 0:
                # Wait for any outstanding messages to be delivered and delivery report
                # callbacks to be triggered.
                producer.flush()
                logger.info('Number of alert produced so far : %d (%s)' % (counter, topic))
        if producer is not None:
            producer.flush()

        end_time = dt.datetime.now()
        logger.info("Total alerts produced to message hub = %d " % len(key_and_msg))
        logger.info("Total time taken to produce the alert to message hub = %s seconds." % (
                    end_time - start_time).total_seconds())

    def produce(self, topic, msg, key=None, producer=None, callback=_delivery_report):
        if topic is None or len(topic) == 0 or msg is None:
            return

        options = {'sasl.username': MH_USER, 'sasl.password': MH_PASSWORD, 'bootstrap.servers': MH_BROKERS_SASL,
                   'security.protocol': 'SASL_SSL',  # 'ssl.ca.location': '/etc/ssl/certs/', # ubuntu
                   'ssl.ca.location': self.MH_CA_CERT_PATH, 'sasl.mechanisms': 'PLAIN', 'api.version.request': True,
                   'broker.version.fallback': '0.10.2.1', 'log.connection.close': False,
                   'client.id': MH_CLIENT_ID + '-' + randomword(4)}

        if KAFKA_INSTALLED:
            if producer is None:
                producer = Producer(options)

            # Asynchronously produce a message, the delivery report callback
            # will be triggered from poll() above, or flush() below, when the message has
            # been successfully delivered or failed permanently.
            producer.produce(topic, value=msg, key=key, callback=callback)

            # Trigger any available delivery report callbacks from previous produce() calls
            producer.poll(0)

        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
        # producer.flush()

        else:
            logger.info('Topic %s : %s' % (topic, msg))

        return producer

    def mkdir_p(self, path):
        try:
            os.makedirs(path)
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    def safe_open_w(self):
        '''
        Open "MH_CA_CERT_PATH" for writing, creating the parent directories as needed.
        '''
        self.mkdir_p(os.path.dirname(self.MH_CA_CERT_PATH))
        return open(self.MH_CA_CERT_PATH, 'w')

    def __init__(self):
        try:
            exists = os.path.isfile(self.MH_CA_CERT_PATH)
            if exists:
                logger.info('EventStreams certificate exists')
            else:
                if self.MH_CA_CERT is not None and len(self.MH_CA_CERT) > 0 and self.MH_CA_CERT.lower() != 'null':
                    logger.info('EventStreams create ca certificate file in pem format.')
                    with self.safe_open_w() as f:
                        f.write(self.MH_CA_CERT)
                else:
                    logger.info('EventStreams ca certificate is empty.')
                    if sys.platform == "linux":  # Red Hat linux
                        self.MH_CA_CERT_PATH = '/etc/pki/tls/cert.pem'
                    elif sys.platform == "darwin":  # MAC OS
                        self.MH_CA_CERT_PATH = '/etc/ssl/cert.pem'
                    else:  # IBM Cloud/Ubuntu
                        self.MH_CA_CERT_PATH = '/etc/ssl/certs'
        except Exception:
            logger.error('Initialization of EventStreams failed.')
            raise


class Trace(object):
    '''
    Gather status and diagnostic information to report back in the UI
    '''

    save_trace_to_file = False

    def __init__(self, object_name=None, parent=None, db=None):
        if parent is None:
            parent = self
        self.parent = parent
        self.db = db
        self.auto_save = None
        self.auto_save_thread = None
        self.stop_event = None
        (self.name, self.cos_path) = self.build_trace_name(object_name=object_name, execution_date=None)
        self.data = []
        self.df_cols = set()
        self.df_index = set()
        self.df_count = 0
        self.usage = 0
        self.prev_ts = dt.datetime.utcnow()
        logger.debug('Starting trace')
        logger.debug('Trace name: %s', self.name)
        logger.debug('auto_save %s', self.auto_save)
        self.write(created_by=self.parent, text='Trace started. ')

    def as_json(self):

        return json.dumps(self.data, indent=4)

    def build_trace_name(self, object_name, execution_date):

        try:
            (trace_name, cos_path) = self.parent.build_trace_name(object_name=object_name,
                                                                  execution_date=execution_date)
        except AttributeError:
            if object_name is None:

                try:
                    object_name = self.parent.logical_name
                except AttributeError:
                    object_name = self.parent.name

            if execution_date is None:
                execution_date = dt.datetime.utcnow()
            trace_name = 'auto_trace_%s_%s' % (object_name, execution_date.strftime('%Y%m%d%H%M%S'))
            cos_path = ('%s/%s/%s/%s_trace_%s' % (
                self.parent.tenant_id, object_name, execution_date.strftime('%Y%m%d'), object_name,
                execution_date.strftime('%H%M%S')))

        return (trace_name, cos_path)

    def get_stack_trace(self):
        '''
        Extract stack trace entries. Return string.
        '''

        stack_trace = ''

        for t in self.data:
            entry = t.get('exception', None)
            if entry is not None:
                stack_trace = stack_trace + entry + '\n'
            entry = t.get('stack_trace', None)
            if entry is not None:
                stack_trace = stack_trace + entry + '\n'

        return stack_trace

    def reset(self, object_name=None, execution_date=None, auto_save=None):
        '''
        Clear trace information and rename trace
        '''
        self.df_cols = set()
        self.df_index = set()
        self.df_count = 0
        self.usage = 0
        self.prev_ts = dt.datetime.utcnow()
        self.auto_save = auto_save
        if self.auto_save_thread is not None:
            logger.debug('Reseting trace %s', self.name)
            self.stop()
        self.data = []
        (self.name, self.cos_path) = self.build_trace_name(object_name=object_name, execution_date=execution_date)

        logger.debug('Started a new trace %s ', self.name)
        if self.auto_save is not None and self.auto_save > 0:
            logger.debug('Initiating auto save for trace')
            self.stop_event = threading.Event()
            self.auto_save_thread = threading.Thread(target=self.run_auto_save, args=[self.stop_event])
            self.auto_save_thread.start()

    def run_auto_save(self, stop_event):
        '''
        Run auto save. Auto save is intended to be run in a separate thread.
        '''
        last_trace = None
        next_autosave = dt.datetime.utcnow()
        while not stop_event.is_set():
            if next_autosave >= dt.datetime.utcnow():
                if self.data != last_trace:
                    logger.debug('Auto save trace %s' % self.name)
                    self.save()
                    last_trace = self.data
                next_autosave = dt.datetime.utcnow() + dt.timedelta(seconds=self.auto_save)
            time.sleep(0.1)
        logger.debug('%s autosave thread has stopped', self.name)

    def save(self):
        '''
        Write trace to COS
        '''

        if len(self.data) == 0:
            trace = None
            logger.debug('Trace is empty. Nothing to save.')
        else:
            if self.db is None:
                logger.warning('Cannot save trace. No db object supplied')
                trace = None
            else:
                trace = str(self.as_json())
                self.db.cos_save(persisted_object=trace, filename=self.cos_path, binary=False, serialize=False)
                logger.debug('Saved trace to cos %s', self.cos_path)
        try:
            save_to_file = self.parent.save_trace_to_file
        except AttributeError:
            save_to_file = self.save_trace_to_file
        if trace is not None and save_to_file:
            with open('%s.json' % self.name, 'w') as fp:
                fp.write(trace)
            logger.debug('wrote trace to file %s.json' % self.name)

        return trace

    def stop(self):
        '''
        Stop autosave thead
        '''
        self.auto_save = None
        if not self.stop_event is None:
            self.stop_event.set()
        if self.auto_save_thread is not None:
            self.auto_save_thread.join()
            self.auto_save_thread = None
            logger.debug('Stopping autosave on trace %s', self.name)

    def update_last_entry(self, msg=None, log_method=None, df=None, **kw):
        '''
        Update the last trace entry. Include the contents of **kw.
        '''
        kw['updated'] = dt.datetime.utcnow()

        self.usage = self.usage + kw.get('usage', 0)
        kw['cumulative_usage'] = self.usage

        try:
            last = self.data.pop()
        except IndexError:
            last = {}
            logger.debug(('Tried to update the last entry of an empty trace.'
                          ' Nothing to update. New entry will be inserted.'))

        for key, value in list(kw.items()):
            if isinstance(value, pd.DataFrame):
                last[key] = 'Ignored dataframe object that was included in trace'
            elif not isinstance(value, str):
                last[key] = str(value)

        if df is not None:
            df_info = self._df_as_dict(df=df)
            last = {**last, **df_info}

        if msg is not None:
            last['text'] = last['text'] + msg
        self.data.append(last)

        # write trace update to the log
        if log_method is not None:
            if msg is not None:
                log_method('Trace message: %s', msg)
            if len(kw) > 0:
                log_method('Trace payload: %s', kw)

        return last

    def write(self, created_by, text, log_method=None, df=None, **kwargs):
        ts = dt.datetime.utcnow()
        text = str(text)
        elapsed = (ts - self.prev_ts).total_seconds()
        self.prev_ts = ts
        kwargs['elapsed_time'] = elapsed

        self.usage = self.usage + kwargs.get('usage', 0)
        kwargs['cumulative_usage'] = self.usage

        try:
            created_by_name = created_by.name
        except AttributeError:
            created_by_name = str(created_by)
        entry = {'timestamp': str(ts), 'created_by': created_by_name, 'text': text, 'elapsed_time': elapsed}
        for key, value in list(kwargs.items()):
            if not isinstance(value, str):
                kwargs[key] = str(value)
        entry = {**entry, **kwargs}

        # The trace can track changes in a dataframe between writes

        if df is not None:
            df_info = self._df_as_dict(df=df)
            entry = {**entry, **df_info}

        self.data.append(entry)

        exception_type = entry.get('exception_type', None)
        exception = entry.get('exception', None)
        stack_trace = entry.get('stack_trace', None)

        try:
            if log_method is not None:
                log_method(text)
                if exception_type is not None:
                    log_method(exception_type)
                if exception is not None:
                    log_method(exception)
                if stack_trace is not None:
                    log_method(stack_trace)
        except TypeError:
            msg = 'A write to the trace called an invalid logging method. Logging as warning: %s' % text
            logger.warning(text)
            if exception_type is not None:
                logger.warning(exception_type)
            if exception is not None:
                logger.warning(exception)
            if stack_trace is not None:
                logger.warning(stack_trace)

    def write_usage(self, db, start_ts=None, end_ts=None):
        '''
        Write usage stats to the usage log
        '''

        usage_logged = False
        msg = 'No db object provided. Did not write usage'

        usage = []
        for i in self.data:
            result = int(i.get('usage', 0))
            if end_ts is None:
                end_ts = dt.datetime.utcnow()

            if start_ts is None:
                elapsed = float(i.get('elapsed_time', '0'))
                start_ts = end_ts - dt.timedelta(seconds=elapsed)

            if result > 0:
                entry = {"entityTypeName": self.parent.name, "kpiFunctionName": i.get('created_by', 'unknown'),
                         "startTimestamp": str(start_ts), "endTimestamp": str(end_ts),
                         "numberOfResultsProcessed": result}
                usage.append(entry)

        if len(usage) > 0:

            if db is not None:
                try:
                    db.http_request(object_type='usage', object_name='', request='POST', payload=usage)
                except BaseException as e:
                    msg = 'Unable to write usage. %s' % str(e)
                else:
                    usage_logged = True

        else:
            msg = 'No usage recorded for this execution'

        if not usage_logged:
            logger.info(msg)
            if len(usage) > 0:
                logger.info(usage)

    def _df_as_dict(self, df):

        '''
        Gather stats about changes to the dataframe between trace entries
        '''

        data = {}
        if df is None:
            data['df'] = 'Ignored null dataframe'
        elif not isinstance(df, pd.DataFrame):
            data['df'] = 'Ignored non dataframe of type %s' % df.__class__.__name__
        else:
            if len(df.index) > 0:
                prev_count = self.df_count
                prev_cols = self.df_cols
                self.df_count = len(df.index)
                if df.index.names is None:
                    self.df_index = {}
                else:
                    self.df_index = set(df.index.names)
                self.df_cols = set(df.columns)
                # stats
                data['df_count'] = self.df_count
                data['df_index'] = list(self.df_index)
                # look at changes
                if self.df_count != prev_count:
                    data['df_rowcount_change'] = self.df_count - prev_count
                if len(self.df_cols - prev_cols) > 0:
                    data['df_added_columns'] = list(self.df_cols - prev_cols)
                if len(prev_cols - self.df_cols) > 0:
                    data['df_added_columns'] = list(prev_cols - self.df_cols)
            else:
                data['df'] = 'Empty dataframe'

        return data

    def __str__(self):

        out = ''
        for entry in self.data:
            out = out + entry['text']

        return out
