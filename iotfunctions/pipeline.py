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
        try:
            self.entity_type_name = source.name
        except:
            self.entity_type_name = None
        
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
        preload_item_names = []
        #if no dataframe provided, querying the source entity to get one
        for p in preload_stages:
            status = p.execute(df=None,start_ts=start_ts,end_ts=end_ts,entities=entities)
            try:
                preload_item_names.append(p.output_item)
            except AttributeError:
                msg = 'Preload functions are expected to have an argument and property called output_item. This preload function is not defined correctly'
                raise AttributeError (msg)
            if status:
                msg = 'Successfully executed preload stage %s' %p.__class__.__name__
                logger.debug(msg)
            else:
                msg = 'Preload stage %s returned continue pipeline value of False. Aborting execution.' %p.__class__.__name__
                logger.debug(msg)
                stages = []
                break
            
        return(stages,preload_item_names)
    
    
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
    
                
    def execute(self, df=None, to_csv=False, dropna=False, start_ts = None, end_ts = None, entities = None, preloaded_item_names=None,
                register = False):
        '''
        Execute the pipeline using an input dataframe as source.
        '''    
        #preload may  have already taken place. if so pass the names of the stages that were executed prior to loading.
        if preloaded_item_names is None:
            preloaded_item_names = []
        # set parameters for stages based on pipeline parameters
        if not self.source is None:
            params = self.source.get_params()
            for s in self.stages:
                try:
                    s = s.set_params(**params)
                except AttributeError:
                    pass
        #process preload stages first if there are any
        (stages,preload_item_names) = self._execute_preload_stages(start_ts = start_ts, end_ts = end_ts, entities = entities)
        preloaded_item_names.extend(preload_item_names)
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
        
        #add a dummy item to the dataframe for each preload stage
        #added as the ui expects each stage to contribute one or more output items
        for pl in preloaded_item_names:
            df[pl] = True
        # process remaining stages
        trace_history = ''
        for s in stages:
            if df.empty:
                #only continue empty stages while there are unprocessed secondary sources
                if len(secondary_sources) == 0:
                    self.logger.info('No data retrieved and no remaining secondary sources to process. Exiting pipeline execution')        
                    break
                #skip this stage of it is not a secondary source
                if not s in secondary_sources:
                    continue
            #check to see if incoming data has a conformed index, conform if needed
            trace = trace_history + ' pipeline failed during execution of stage %s. ' %s.__class__.__name__
            trace = trace +' Dataframe had columns: %s.' %(list(df.columns))
            trace = trace +' Dataframe had index: %s.' %(df.index.names)            
            try:
                df = s.conform_index(df)
            except AttributeError:
                pass
            except KeyError:
                logger.exception(trace)
                print(trace)
                raise               
            try:
                msg = 'Executing pipeline stage %s. Input dataframe.' %s.__class__.__name__
                s.log_df_info(df,msg)
            except AttributeError:
                pass             
            # There are two different signatures for the execute method
            try:
                try:
                    newdf = s.execute(df=df,start_ts=start_ts,end_ts=end_ts,entities=entities)
                except TypeError:
                        newdf = s.execute(df=df)
            except Exception as e:
                trace = '%s | %s' %(str(e),trace)
                #logger.exception(trace)
                raise e.__class__(trace)
            #validate that stage has not violated any pipeline processing rules
            if register:
                try:
                    s.register(df=df,credentials=None, new_df= newdf)
                except AttributeError:
                    raise
                    msg = 'Could not export %s as it has no register() method' %s.__class__.__name__
                    logger.warning(msg)
            else:
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
            trace_history = trace_history + ' Completed stage %s ->' %s.__class__.__name__
        return df

    def export(self):
        
        export = []
        for s in self.stages:
            if self.source is None:
                source_name = None
            else:
                source_name = self.source.name
            metadata  = { 
                    'name' : s.name ,
                    'args' : s._get_arg_metadata()
                    }
            export.append(metadata)
            
        response = self.db.http_request(object_type = 'kpiFunctions',
                                        object_name = source_name,
                                        request = 'POST',
                                        payload = metadata)
        
        
        return response
    
    def get_input_items(self):
        '''
        Get the set of input items explicitly requested by each function
        '''
        inputs = set()
        for s in self.stages:
            try:
                inputs = inputs | s.get_input_items()
            except AttributeError:
                pass
            
        return inputs
    
    def set_stages(self,stages):
        '''
        Replace existing stages with a new list of stages
        '''
        self.stages = []
        if not stages is None:
            if not isinstance(stages,list):
                stages = [stages]
            self.stages.extend(stages)