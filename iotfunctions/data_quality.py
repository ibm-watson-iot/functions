# *****************************************************************************
# © Copyright IBM Corp. 2020, 2022  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import logging
from iotfunctions.base import (BaseComplexAggregator)
from iotfunctions.ui import (UISingleItem,
                             UIMulti,
                             UIFunctionOutMulti)
import math
import pandas as pd
import numpy as np
from statsmodels.tsa.stattools import (kpss, adfuller)
from statsmodels.stats.diagnostic import acorr_ljungbox

logger = logging.getLogger(__name__)

PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'
_IS_PREINSTALLED = True


class DataQualityChecks(BaseComplexAggregator):
    """
    Perform data quality analysis on input data.Select the time series that you want to check in input_item and
    select the data quality checks to run. The checks are grouped by their output data type. The options are
    stationarity (string), sample entropy (numeric), constant value (boolean), struck-at-zero (boolean),
    and white noise (boolean).
    """
    # define check name in QUALITY_CHECK same as corresponding staticmethod that executes the function
    STR_QUALITY_CHECKS = ['stationarity']
    NUMERICAL_QUALITY_CHECKS = ['sample_entropy']
    BOOLEAN_QUALITY_CHECKS = ['constant_value', 'stuck_at_zero', 'white_noise']

    SERIES_LEN_ERROR = {str: 'Series len < 1', float: -1, bool: False}

    def __init__(self, source=None, checks_with_string_output=None, checks_with_numerical_output=None,
                 checks_with_boolean_output=None, name=None):
        super().__init__()

        self.input_items = source
        self.quality_checks = []
        if checks_with_string_output:
            self.quality_checks.extend(checks_with_string_output)
        if checks_with_numerical_output:
            self.quality_checks.extend(checks_with_numerical_output)
        if checks_with_boolean_output:
            self.quality_checks.extend(checks_with_boolean_output)

        self.output_items = name
        logger.debug(f'Data Quality Checks will be performed for : {source}')
        logger.debug(f'quality checks selected: {self.quality_checks}  corresponding output: {name}')

    @classmethod
    def build_ui(cls):
        inputs = [UISingleItem(name='source', datatype=None,
                               description='Choose data item to run data quality checks on'),
                  UIMulti(name='checks_with_string_output', datatype=str, description='Select quality checks '
                          'to run. These checks return string output ', values=cls.STR_QUALITY_CHECKS, required=False),
                  UIMulti(name='checks_with_numerical_output', datatype=str, description='Select quality '
                          'checks to run. These checks return numerical output ', values=cls.NUMERICAL_QUALITY_CHECKS,
                          required=False),
                  UIMulti(name='checks_with_boolean_output', datatype=str, description='Select quality checks '
                          'to run. These checks return boolean output', values=cls.BOOLEAN_QUALITY_CHECKS,
                          required=False)
                  ]
        outputs = [UIFunctionOutMulti('name', cardinality_from='checks_with_string_output', datatype=str,
                                      description='quality check returns string output'),
                   UIFunctionOutMulti('name', cardinality_from='checks_with_numerical_output', datatype=float,
                                      description='quality check returns numerical output'),
                   UIFunctionOutMulti('name', cardinality_from='checks_with_boolean_output', datatype=bool,
                                      description='quality check returns boolean output')
                   ]

        return inputs, outputs

    def execute(self, group):
        """
        Called on df.groupby
        """
        ret_dict = {}
        group_no_nan = group.dropna()
        for check, output in zip(self.quality_checks, self.output_items):
            agg_func = getattr(self, check)
            if len(group_no_nan[self.input_items]) > 1:
                ret_dict[output] = group_no_nan[self.input_items].agg(agg_func)
            #length of incoming data is too short for meaningful computation
            else:
                logger.warning('Not enough data to perform data quality checks')
                ret_dict[output] = self.SERIES_LEN_ERROR[agg_func.__annotations__['return']]

        return pd.Series(ret_dict, index=self.output_items)

    @staticmethod
    def constant_value(series) -> bool:
        """
        A time series signal stuck at a constant value contains no information, and is highly likely to be due to an
        error in data collection

        :returns bool True when series has constant_value
                      False when series has varying values
        """
        return bool(series.nunique() <= 1)

    @staticmethod
    def sample_entropy(series) -> float:
        """
        Measure of signal complexity/randomness in signal
        A value closer to 0 indicates repeated patterns in data/ease of prediction

        References
        Entropy 2019, 21(6), 541; https://doi.org/10.3390/e21060541
        https://en.wikipedia.org/wiki/Sample_entropy

        recommended values
        m (2, 3)
        r (0.1, 2.5) * standard deviation

        :returns float
        """

        def sampen(L, m, r):
            N = len(L)

            # Split time series and save all templates of length m
            xmi = np.array([L[i: i + m] for i in range(N - m)])
            xmj = np.array([L[i: i + m] for i in range(N - m + 1)])

            # Save all matches minus the self-match, compute B
            B = np.sum([np.sum(np.abs(xmii - xmj).max(axis=1) <= r) - 1 for xmii in xmi])

            # Similar for computing A
            m += 1
            xm = np.array([L[i: i + m] for i in range(N - m + 1)])

            A = np.sum([np.sum(np.abs(xmi - xm).max(axis=1) <= r) - 1 for xmi in xm])

            # Return SampEn
            return -np.log(A / B)

        return sampen(series.to_list(), m=2, r=0.2 * series.std())

    @staticmethod
    def stationarity(series) -> str:
        """
        A time series is Stationary when it's mean, variance, co-variance do not change over time.
        Time-invariant process are requirements of statistical models for forecasting problems
        Can indicate spurious causation between variable dependent on time

        Reference:
        https://www.statsmodels.org/stable/examples/notebooks/generated/stationarity_detrending_adf_kpss.html
        performs adf and kpss stationarity tests
        :returns str (Not Stationary, Stationary, Trend Stationary, Difference Stationary, Constant Data)
        """
        stationary_type = {
            # adf stationary, kpss stationary
            (False, False): 'Not Stationary',
            (False, True): 'Trend Stationary',
            (True, False): 'Difference Stationary',
            (True, True): 'Stationary',
            (np.nan, np.nan): 'Constant Data',
            'NoCompute': 'Not Enough Data for stationarity test'
        }
        if len(series) < 4:
            return stationary_type['NoCompute']

        significance_level = 0.05  # p > 5% fail to reject the null hypothesis
        # adf test; H0: series has unit root (non-stationary)
        adf_statistic, adf_p_value, _, _, _, _ = adfuller(series)
        adf_stationary = np.nan
        if not math.isnan(adf_p_value):
            adf_stationary = bool(adf_p_value < significance_level)  # reject null

        # kpss test; H0: process is trend stationary
        kpss_statistic, kpss_p_value, _, _ = kpss(series)
        kpss_stationary = np.nan
        if not math.isnan(kpss_p_value):
            kpss_stationary = bool(kpss_p_value >= significance_level)  # fail to reject null

        return stationary_type[adf_stationary, kpss_stationary]

    @staticmethod
    def stuck_at_zero(series) -> bool:
        """
        A time series signal stuck at zero contains no information

        :returns bool
        """
        tolerance = 10e-8
        is_close_to_zero = np.all((series.to_numpy() <= tolerance))
        return bool(is_close_to_zero)

    @staticmethod
    def white_noise(series) -> bool:
        """
        A white noise time series signal is random signal that cannot be reasonably predicted
        (Additional use) Forecasting error should be white nose

        :returns bool
        """
        # ljung box test; H0: data is iid/random/white noise
        white_noise = True  # accept Null Hypothesis
        significance_level = 0.05  # p < 0.05 rejects null hypothesis
        ljung_box_q_statitic, ljung_box_p_value = acorr_ljungbox(series, lags=len(series) - 1, return_df=False)

        if all([p_value < significance_level for p_value in ljung_box_p_value]):
            white_noise = False  # reject Null Hypothesis

        return white_noise
