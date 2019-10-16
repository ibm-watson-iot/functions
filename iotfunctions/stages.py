# *****************************************************************************
# Â© Copyright IBM Corp. 2018.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import datetime as dt
import logging
import pandas as pd
import json

from . import messagehub, util

DATA_ITEM_TAG_ALERT = 'ALERT'
DATA_ITEM_TAGS_KEY = 'tags'


class ProduceAlerts:

    def __init__(self, dms, alerts=None):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if dms is None:
            raise RuntimeError("argument dms must be provided")
        if alerts is None:
            raise RuntimeError("argument alerts must be provided")
        self.dms = dms
        self.alerts = []
        self.messagehub = messagehub.MessageHub()

        for alert in util.asList(alerts):
            # self.alerts.append(alert)
            metadata = self.dms.data_items.get(alert)
            if metadata is not None:
                if DATA_ITEM_TAG_ALERT in metadata.get(DATA_ITEM_TAGS_KEY, []):
                    self.alerts.append(alert)

    def execute(self, df):
        if self.dms.production_mode:
            self.logger.debug('alerts_to_produce=%s, df_columns=%s' % (str(self.alerts), str(df.dtypes.to_dict())))

            # TODO it may be worth pre-filtering the data frame to be just those rows with True alert column values,
            # because iterating through the whole data frame is a slow process

            t1 = dt.datetime.now()

            msg_and_keys = []
            if len(self.alerts) > 0:  # no alert, do iterate which is slow
                for ix, row in df.iterrows():
                    payload = row.to_dict()

                    # TODO there is an issue for the json.dumps below which cannot deal all types

                    for alert, value in payload.items():
                        # Skip missing values and non-True values
                        if alert in self.alerts and pd.notna(value) and value:
                            # publish alert format
                            # key: <tenant-id>|<entity-type>|<entity-id>|<alert-name>|<timestamp>
                            # value: json document containing all metrics at the same time / same device / same grain
                            msg_and_keys.append((json.dumps(payload, default=self._serialize_converter),
                                                 '%s|%s|%s|%s|%s' % (
                                                     self.dms.tenant_id, self.dms.entity_type, ix[0], alert, ix[1])))

            self.logger.debug("total_alerts_produced=%d" % len(msg_and_keys))

            if len(msg_and_keys) > 0:
                self.messagehub.produce_batch(msg_and_keys=msg_and_keys)

            t2 = dt.datetime.now()
            self.logger.info("produce_alert_time_seconds=%s" % (t2 - t1).total_seconds())

        return df

    '''
    Timestamp is not serialized by default by json.dumps()
    It is necessary to convert the object to string
    '''

    def _serialize_converter(self, o):
        if isinstance(o, dt.datetime):
            return o.__str__()
