# *****************************************************************************
# Â© Copyright IBM Corp. 2018.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import logging

logger = logging.getLogger(__name__)


class MergeException(Exception):

    def __init__(self, msg):
        super().__init__(msg)


class StageException(Exception):
    EXTENSION_DICT = 'extensionDict'
    STAGENAME = 'stageName'
    STAGEINFO = 'stageInfo'

    def __init__(self, msg, stageName=None, stageInfo=None):
        super().__init__(msg)
        setattr(self, StageException.EXTENSION_DICT,
                {StageException.STAGENAME: stageName, StageException.STAGEINFO: stageInfo})


class DataWriterException(Exception):

    def __init__(self, msg):
        logger.error(msg)
        super().__init__(msg)
