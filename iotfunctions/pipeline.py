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
import numpy as np
logger = logging.getLogger(__name__)

class CalcPipeline:
    '''
    A CalcPipeline executes a series of dataframe transformation stages.
    '''
    def __init__(self,stages = None,source =None):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))
        self.set_stages(stages)
        self.source = source
    
    def add_stage(self,stage):
        '''
        Add a new stage to a pipeline. A stage is Transformer or Aggregator.
        '''
        self.stages.append(stage)
        
    def set_stages(self,stages):
        '''
        Replace existing stages with a new list of stages
        '''
        self.stages = []
        if not stages is None:
            if not isinstance(stages,list):
                stages = [stages]
            self.stages.extend(stages)
                
    def execute(self, df=None, to_csv=False, dropna=False, start_ts = None, end_ts = None, entities = None):
        '''
        Execute the pipeline using an input dataframe as source.
        '''
        
        #if no dataframe provided, querying the source entity to get one
        if df is None:
            df = self.source.get_data(start_ts=start_ts, end_ts = end_ts, entities = entities)
            if to_csv:
                filename = 'debugPipelineSource_%s.csv' %self.source.name
                df.to_csv(filename)
        
        #get entity metadata - will be inserted later into each stage
        if self.source is None:
            params = {}
        else:
            params = self.source.get_params()
        
        self.logger.debug("pipeline_input_df_columns=%s, pipeline_input_df_indexes=%s, pipeline_input_df=\n%s" % (df.dtypes.to_dict(), df.index.to_frame().dtypes.to_dict(), df.head()))

        if dropna:
            df = df.replace([np.inf, -np.inf], np.nan)
            df = df.dropna()
        '''
        divide the pipeline into data retrieval stages and transformation stages        
        some retrieval stages behave like alternative data sources. They replace the incoming entity data
        process these first
        '''
        replace_count = 0
        retrieval_stages = []
        transform_stages = []
        for s in self.stages:
            s = s.set_params(**params)
            try:
                is_data_source =  s.is_data_source
            except AttributeError:
                is_data_source = False
            if is_data_source:
                if s.merge_method == 'replace':
                    df = s.execute(df=df)
                    self.logger.debug("stage=%s is a custom data source. It replaced incoming entity data. " %s.__class__.__name__)
                    self.logger.debug("stage=%s, pipeline_intermediate_df=\n%s" % (s.__class__.__name__, df.head()))
                    replace_count += 1
                    if to_csv:
                        df.to_csv('debugPipelineOut_%s.csv' %s.__class__.__name__)  
                else:
                    retrieval_stages.append(s)
            else:
                transform_stages.append(s)
        if replace_count > 1:
            self.logger.warning("The pipeline has more than one custom source with a merge strategy of replace. The pipeline will only contain data from the last replacement")        
        #process remaining data sources
        df =  self._execute_stages(stages = retrieval_stages,df=df,dropna = dropna,to_csv=to_csv)                      
        if df.empty:
            self.logger.info('The data retrieval stages found no data to transform. Skipping transformation stages')        
        else:    
            # process transform stages
            df =  self._execute_stages(stages = transform_stages,df=df,dropna = dropna, to_csv=to_csv) 
        return df
    
    def _execute_stages(self,stages,df,dropna,to_csv):
        '''
        Execute a subset of stages
        '''        
        for s in stages:
            df = s.conform_index(df)
            msg = 'Executing pipeline stage %s. Input dataframe.' %s.__class__.__name__
            s.log_df_info(df,msg)
            original_columns = set(df.columns)
            newdf = s.execute(df)
            try:
                s.validate_df(df,newdf)
            except AttributeError:
                pass
            df = newdf
            if dropna:
                df = df.replace([np.inf, -np.inf], np.nan)
                df = df.dropna()            
            new_columns = set(df.columns)                
            dropped_columns = original_columns - new_columns
            if len(dropped_columns) > 0:
                self.logger.warning("Pipeline stage %s dropped columns %s from the pipeline." %(s.__class__.__name__,dropped_columns))
            if to_csv:
                df.to_csv('debugPipelineOut_%s.csv' %s.__class__.__name__)    
            msg = 'Completed stage %s. Output dataframe.' %s.__class__.__name__
            s.log_df_info(df,msg)
        
        
        return df
