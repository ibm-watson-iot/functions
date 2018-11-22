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
import json
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
        
    def _extract_preload_stages(self):
        '''
        pre-load stages are special stages that are processed outside of the pipeline
        they execute before loading data into the pipeline
        return tuple containing list of preload stages and list of other stages to be processed
        '''
        stages = []
        extracted_stages = []
        for s in self.stages:
            try:
                is_preload = s.is_preload
            except AttributeError:
                is_preload = False
            if is_preload:
                msg = 'Extracted preload stage %s from pipeline' %self.__class__.__name__
                logger.debug(msg)
                extracted_stages.append(s)
            else:
                stages.append(s)
        return (extracted_stages,stages)
                        
    
    def _execute_preload_stages(self, start_ts = None, end_ts = None, entities = None):
        '''
        Extract and run preload stages
        Return remaining stages to process
        '''
        (preload_stages,stages) = self._extract_preload_stages()
        #if no dataframe provided, querying the source entity to get one
        for p in preload_stages:
            status = p.execute(start_ts=start_ts,end_ts=end_ts,entities=entities)
            if status:
                msg = 'Successfully executed preload stage %s' %p.__class__.__name__
                logger.debug(msg)
            else:
                msg = 'Preload stage %s returned continue pipeline value of False. Aborting execution.' %p.__class__.__name__
                logger.debug(msg)
                stages = []
                break
            
        return(stages)
    
    
    def _execute_primary_source(self,stages,df,start_ts=None,end_ts=None,entities=None,to_csv=False):
        '''
        Extract and execute data source stages with a merge_method of replace.
        Identify other data source stages that add rows of data to the pipeline
        '''
        remaining_stages = []
        secondary_sources = []
        replace_count = 0
        for s in stages:
            try:
                is_data_source =  s.is_data_source
                merge_method = s.merge_method
            except AttributeError:
                is_data_source = False
                merge_method = None
            if is_data_source and merge_method == 'replace':
                df = s.execute(df=df)
                self.logger.debug("stage=%s is a custom data source. It replaced incoming entity data. " %s.__class__.__name__)
                try:
                    s.log_df_info(df,'Incoming data replaced with function output from primary data source')
                except AttributeError:
                    pass
                replace_count += 1
                if to_csv:
                    df.to_csv('debugPrimaryDataSource_%s.csv' %s.__class__.__name__)  
            elif is_data_source and merge_method == 'outer':
                '''
                A data source with a merge method of outer is considered a secondary source
                A secondary source can add rows of data to the pipeline.
                '''
                secondary_sources.append(s)
                remaining_stages.append(s)
            else:
                remaining_stages.append(s)
        if replace_count > 1:
            self.logger.warning("The pipeline has more than one custom source with a merge strategy of replace. The pipeline will only contain data from the last replacement")        
            
        return(remaining_stages,secondary_sources)    
    
                
    def execute(self, df=None, to_csv=False, dropna=False, start_ts = None, end_ts = None, entities = None):
        '''
        Execute the pipeline using an input dataframe as source.
        '''    
        # set parameters for stages based on pipeline parameters
        if not self.source is None:
            params = self.source.get_params()
            for s in self.stages:
                try:
                    s = s.set_params(**params)
                except AttributeError:
                    pass
        #process preload stages first if there are any
        stages = self._execute_preload_stages(start_ts = start_ts, end_ts = end_ts, entities = entities)
        if df is None:
            msg = 'No dataframe supplied for pipeline execution. Getting entity source data'
            logger.debug(msg)
            df = self.source.get_data(start_ts=start_ts, end_ts = end_ts, entities = entities)
        if df is None:
            msg = 'Pipeline has no primary source. Provide a dataframe or source entity'
            raise ValueError (msg)
        if to_csv:
            filename = 'debugPipelineSourceData.csv'
            df.to_csv(filename)
        #get entity metadata - will be inserted later into each stage
        if dropna:
            df = df.replace([np.inf, -np.inf], np.nan)
            df = df.dropna()
        '''
        Divide the pipeline into data retrieval stages and transformation stages. First look for
        a primary data source. A primary data source will have a merge_method of 'replace'. This
        implies that it replaces whatever data was fed into the pipeline as default entity data.
        '''
        (stages,secondary_sources) = self._execute_primary_source (
                                            df = df,
                                            stages = stages,
                                            start_ts = start_ts,
                                            end_ts = end_ts,
                                            entities = entities,
                                            to_csv = False)
        # process remaining stages
        for s in stages:
            if df.empty and len(secondary_sources) == 0:
                self.logger.info('No data retrieved and no remaining secondary sources to process. Exiting pipeline execution')        
                break
            #check to see if incoming data has a conformed index, conform if needed
            try:
                df = s.conform_index(df)
            except AttributeError:
                pass
            try:
                msg = 'Executing pipeline stage %s. Input dataframe.' %s.__class__.__name__
                s.log_df_info(df,msg)
            except AttributeError:
                pass
            #validate that stage has not violated any pipeline processing rules
            newdf = s.execute(df)
            try:
                s.validate_df(df,newdf)
            except AttributeError:
                pass
            df = newdf
            if dropna:
                df = df.replace([np.inf, -np.inf], np.nan)
                df = df.dropna()
            if to_csv:
                df.to_csv('debugPipelineOut_%s.csv' %s.__class__.__name__)
            try:
                msg = 'Completed stage %s. Output dataframe.' %s.__class__.__name__
                s.log_df_info(df,msg)
            except AttributeError:
                pass
            secondary_sources = [x for x in secondary_sources if x != s]  
        return df

    def export(self):
        
        export = []
        for s in self.stages:
            if self.source is None:
                source_name = None
            else:
                source_name = self.source.name
            metadata  = { 
                    'name' : s.__class__.__name__ ,
                    'entity_type' : source_name,
                    'args' : s._get_arg_metadata()
                    }
            export.append(metadata)
        
        return json.dumps(export)
    
    def set_stages(self,stages):
        '''
        Replace existing stages with a new list of stages
        '''
        self.stages = []
        if not stages is None:
            if not isinstance(stages,list):
                stages = [stages]
            self.stages.extend(stages)