#  Licensed Materials - Property of IBM 
#  5737-M66, 5900-AAA, 5900-A0N, 5725-S86, 5737-I75
#  (C) Copyright IBM Corp. 2020 All Rights Reserved.
#  US Government Users Restricted Rights - Use, duplication, or disclosure
#  restricted by GSA ADP Schedule Contract with IBM Corp.
from abc import ABC

import logging
from iotfunctions.base import (BaseComplexAggregator)
from iotfunctions.ui import (UISingleItem,
                             UIMulti)
import pandas as pd

logger = logging.getLogger(__name__)

PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'
_IS_PREINSTALLED = True


class DataQualityChecks(BaseComplexAggregator):
    """
    Brief description of class
    Brief description of the metrics

    """
    # define check name in QUALITY_CHECK same as corresponding staticmethod that executes the function
    QUALITY_CHECKS = ['constant_value',
                      'signal_to_noise_ratio',
                      'stationary',
                      'stuck_at_zero',
                      'white_noise'
                      ]

    def __init__(self, source=None, quality_checks=None, name=None):
        super().__init__()

        self.input_items = source
        self.quality_checks = quality_checks
        self.output_items = name
        logger.debug(f'Data Quality Checks will be performed for : {source}')
        logger.debug(f'quality checks selected: {quality_checks}  corresponding output: {name}')

    @classmethod
    def build_ui(cls):
        inputs = [UISingleItem(name='source', datatype=None,
                               description='Choose data item to run data quality checks on'),
                  UIMulti(name='quality_checks', datatype=str, description='Choose quality checks to run',
                          values=cls.QUALITY_CHECKS, output_item='name',
                          is_output_datatype_derived=True, output_datatype=float)]

        return inputs, []

    def execute(self, group):
        """
        Called on df.groupby
        """
        ret_dict = {}
        for check, output in zip(self.quality_checks, self.output_items):
            agg_func = getattr(self, check)
            ret_dict[output] = group[self.input_items].agg(agg_func)

        return pd.Series(ret_dict, index=self.output_items)

    @staticmethod
    def auto_correlation(series):
        pass

    @staticmethod
    def constant_value(series):
        pass

    @staticmethod
    def signal_to_noise_ratio(series):
        pass

    @staticmethod
    def stationary(series):
        pass

    @staticmethod
    def stuck_at_zero(series):
        pass

    @staticmethod
    def white_noise(series):
        pass
