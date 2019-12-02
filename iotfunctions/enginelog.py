import logging
import sys
import os
import gzip
import datetime as dt

logger = logging.getLogger(__name__)


class EngineLogging:
    FORMATTER = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)s %(name)s.%(funcName)s %(message)s',
                                  datefmt='%Y-%m-%dT%H:%M:%S', style='%')
    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(FORMATTER)

    SETUP_LOG_NAME = 'setup.log'
    setupLogHandler = None
    setupLogCosPath = None

    RUN_LOG_NAME = 'run.log'
    runLogHandler = None
    runLogCosPath = None

    cosClient = None

    @classmethod
    def configure_console_logging(cls, level):
        root_logger = logging.getLogger()
        root_logger.setLevel(level)
        root_logger.addHandler(cls.consoleHandler)
        logger.debug('Console logging has been configured. Level = %d' % level)

    @classmethod
    def start_setup_log(cls, tenant_id, entity_type_name):
        today = dt.datetime.utcnow()
        cls.setupLogCosPath = ('%s/%s/%s/%s_setup.gz' % (
            tenant_id, entity_type_name, today.strftime('%Y%m%d'), today.strftime('%H%M%S')))
        root_logger = logging.getLogger()
        handler = logging.FileHandler(cls.SETUP_LOG_NAME, mode='w')
        handler.setFormatter(cls.FORMATTER)
        root_logger.addHandler(handler)
        cls.setupLogHandler = handler

        logger.info(
            'Started logging into file %s. Object Store path will be %s' % (cls.SETUP_LOG_NAME, cls.setupLogCosPath))

    @classmethod
    def finish_setup_log(cls):
        if cls.setupLogHandler is not None:
            logger.info('Stopping logging into file %s. File will be pushed to Object Store under %s' % (
                cls.SETUP_LOG_NAME, cls.setupLogCosPath))

            root_logger = logging.getLogger()
            root_logger.removeHandler(cls.setupLogHandler)
            cls.setupLogHandler.flush()
            cls.setupLogHandler.close()
            cls.setupLogHandler = None

            # Push setupLog to Object Store
            cls._push_file_to_store(cls.SETUP_LOG_NAME, cls.setupLogCosPath)

    @classmethod
    def start_run_log(cls, tenant_id, entity_type_name):
        today = dt.datetime.utcnow()
        cls.runLogCosPath = ('%s/%s/%s/%s_run.gz' % (
            tenant_id, entity_type_name, today.strftime('%Y%m%d'), today.strftime('%H%M%S')))
        root_logger = logging.getLogger()
        handler = logging.FileHandler(cls.RUN_LOG_NAME, mode='w')
        handler.setFormatter(cls.FORMATTER)
        root_logger.addHandler(handler)
        cls.runLogHandler = handler

        logger.info(
            'Started logging into file %s. Object Store path will be %s' % (cls.RUN_LOG_NAME, cls.runLogCosPath))

    @classmethod
    def finish_run_log(cls):
        if cls.runLogHandler is not None:
            logger.info('Stopping logging into file %s. File will be pushed to Object Store under %s' % (
                cls.RUN_LOG_NAME, cls.runLogCosPath))

            root_logger = logging.getLogger()
            root_logger.removeHandler(cls.runLogHandler)
            cls.runLogHandler.flush()
            cls.runLogHandler.close()
            cls.runLogHandler = None

            # Push runLog to Object Store
            cls._push_file_to_store(cls.RUN_LOG_NAME, cls.runLogCosPath)

    @classmethod
    def _push_file_to_store(cls, file_name, cos_path):

        if cos_path is None or len(cos_path) == 0:
            raise Exception(('The log file path for Object Store is none or empty. '
                             'Log file %s cannot be sent to Object Store.') % file_name)

        if cls.cosClient is not None:
            bucket = os.environ.get('COS_BUCKET_LOGGING')
            bucket = bucket.strip() if bucket is not None else None
            if bucket is not None and len(bucket) > 0:
                try:
                    with open(file_name, 'r') as file:
                        cls.cosClient.cos_put(cos_path, gzip.compress(file.read().encode()), bucket=bucket,
                                              serialize=False)
                except Exception as ex:
                    raise Exception(('The log file %s could not be transferred to Object Store '
                                     'in bucket %s under %s: %s') % (file_name, bucket, cos_path, str(ex))) from ex
                else:
                    logger.info('File %s was successfully stored in Object Store in bucket %s under %s' % (
                        file_name, bucket, cos_path))
            else:
                logger.warning(('The log file %s could not be transferred to Object Store '
                                'because environment variable COS_BUCKET_LOGGING has not been set.') % file_name)
        else:
            logger.warning(('The log file %s could not be transferred to Object Store '
                            'because access to Object Store has not been configured yet.') % file_name)

    @classmethod
    def set_cos_client(cls, cos_client):
        cls.cosClient = cos_client

    @classmethod
    def get_setup_log_cos_path(cls):
        return cls.setupLogCosPath

    @classmethod
    def get_current_run_log_cos_path(cls):
        return cls.runLogCosPath
