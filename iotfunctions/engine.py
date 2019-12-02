import logging

from .enginelog import EngineLogging
from . import db

logger = logging.getLogger(__name__)


def run(args):
    try:
        EngineLogging.configure_console_logging(logging.DEBUG)

        if args.get('tenant_id', None) is None or len(args['tenant_id']) == 0:
            raise Exception('Mandatory tenant-id is missing')

        if args.get('entity_type', None) is None or len(args['entity_type']) == 0:
            raise Exception('Mandatory entity_type is missing')

        EngineLogging.start_setup_log(args['tenant_id'], args['entity_type'])

    except Exception as ex:
        logger.error('The startup of engine failed: %s' % str(ex), exc_info=True)
        return -1

    try:
        logger.info('tenant id = %s, entity type = %s' % (args['tenant_id'], args['entity_type']))

        database = db.Database(start_session=True, echo=True, tenant_id=args['tenant_id'])
        # If _production_mode flag is set to True then only insert data into DB, Publish alerts to message hub and record the usage.
        database.execute_job(entity_type=args['entity_type'], _production_mode=True)

    except Exception as ex:
        logger.error('The engine stopped execution with the following exception: %s' % str(ex), exc_info=True)
        return -1
    finally:
        EngineLogging.finish_setup_log()
        EngineLogging.finish_run_log()

    return 0
