# *****************************************************************************
# © Copyright IBM Corp. 2018, 2022  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import logging
import traceback

logger = logging.getLogger(__name__)


class MergeException(Exception):

    def __init__(self, msg):
        super().__init__(msg)


class StageException(Exception):

    def __init__(self, error_message, stage_name=None, stage_info=None, exception=None):
        super().__init__(error_message)
        setattr(self, 'exception_details',
                {'stage_name': stage_name, 'stage_info': stage_info, "exception_type": exception.__class__.__name__,
                 "stack_trace": traceback.format_exc()})


class DataWriterException(Exception):

    def __init__(self, msg):
        logger.error(msg)
        super().__init__(msg)


class ApplicationException(Exception):

    def __init__(self, msg):
        logger.error(msg)
        super().__init__(msg)
