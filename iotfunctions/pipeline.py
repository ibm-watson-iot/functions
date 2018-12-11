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
import json
import numpy as np
import sys
logger = logging.getLogger(__name__)


class CalcPipeline:
    '''
    A CalcPipeline executes a series of dataframe transformation stages.
    '''
    def __init__(self,stages = None,entity_type =None):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))
        self.entity_type = entity_type
        self.set_stages(stages)
        
    def add_stage(self,stage):
        '''
        Add a new stage to a pipeline. A stage is Transformer or Aggregator.
        '''
        stage._entity_type = self.entity_type
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
                try: 
                    df = s.execute(df=df,start_ts=start_ts,end_ts=end_ts,entities=entities) 
                except TypeError: 
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
            
        return(df,remaining_stages,secondary_sources)    
    
                
    def execute(self, df=None, to_csv=False, dropna=False, start_ts = None, end_ts = None, entities = None, preloaded_item_names=None,
                register = False):
        '''
        Execute the pipeline using an input dataframe as source.
        '''
        last_msg = ''
        #preload may  have already taken place. if so pass the names of the stages that were executed prior to loading.
        if preloaded_item_names is None:
            preloaded_item_names = []
        msg = 'Running pipeline with start timestamp %s' %start_ts
        logger.debug(msg)
        #process preload stages first if there are any
        (stages,preload_item_names) = self._execute_preload_stages(start_ts = start_ts, end_ts = end_ts, entities = entities)
        preloaded_item_names.extend(preload_item_names)
        if df is None:
            msg = 'No dataframe supplied for pipeline execution. Getting entity source data'
            logger.debug(msg)
            df = self.entity_type.get_data(start_ts=start_ts, end_ts = end_ts, entities = entities)
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
        # remove rows that contain all nulls ignore deviceid and timestamp
        subset = [x for x in df.columns if x not in [self.entity_type._entity_id,self.entity_type._timestamp_col]]
        df = df.dropna(how='all', subset = subset )
        '''
        Divide the pipeline into data retrieval stages and transformation stages. First look for
        a primary data source. A primary data source will have a merge_method of 'replace'. This
        implies that it replaces whatever data was fed into the pipeline as default entity data.
        '''
        (df,stages,secondary_sources) = self._execute_primary_source (
                                            df = df,
                                            stages = stages,
                                            start_ts = start_ts,
                                            end_ts = end_ts,
                                            entities = entities,
                                            to_csv = False)
        msg = 'Secondary data sources identified:  %s. Other stages are: %s' %(secondary_sources, stages)
        logger.debug(msg)       
        
        
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
            try:
                df = s.conform_index(df)
            except AttributeError:
                pass
            except KeyError:
                logger.exception(trace)
                print(trace)
                raise               
            try:
                msg = ' Dataframe at start of %s: ' %s.__class__.__name__
                last_msg = s.log_df_info(df,msg)
            except Exception:
                last_msg = self.log_df_info(df,msg)
            try:
                abort_on_fail = self._abort_on_fail
            except AttributeError:
                abort_on_fail = True
            # There are two different signatures for the execute method
            try:
                try:
                    newdf = s.execute(df=df,start_ts=start_ts,end_ts=end_ts,entities=entities)
                except TypeError:
                        newdf = s.execute(df=df)
            except AttributeError as e:
                trace = trace + self.get_stage_trace(stage=s,last_msg=last_msg)
                trace = trace + ' The function makes a reference to an object property that does not exist. Available object properties are %s' %s.__dict__
                self._raise_error(exception = e,msg = trace, abort_on_fail = abort_on_fail)
            except SyntaxError as e:
                trace = trace + self.get_stage_trace(stage=s,last_msg=last_msg)
                trace = trace + ' The function contains a syntax error. If the function configuration includes a type-in expression, make sure that this expression is correct' 
                self._raise_error(exception = e,msg = trace, abort_on_fail = abort_on_fail)
            except (ValueError,TypeError) as e:
                trace = trace + self.get_stage_trace(stage=s,last_msg=last_msg)
                trace = trace + ' The function is operating on data that has an unexpected value or data type. '
                self._raise_error(exception = e,msg = trace, abort_on_fail = abort_on_fail)                
            except (NameError) as e:
                trace = trace + self.get_stage_trace(stage=s,last_msg=last_msg)
                trace = trace + ' The function refered to an object that does not exist. You may be refering to data items in pandas expressions, ensure that you refer to them by name, ie: as a quoted string. '
                self._raise_error(exception = e,msg = trace, abort_on_fail = abort_on_fail)
            except Exception as e:
                trace = trace + self.get_stage_trace(stage=s,last_msg=last_msg)
                trace = trace + ' The function failed to execute '
                self._raise_error(exception = e,msg = trace, abort_on_fail = abort_on_fail)
            #validate that stage has not violated any pipeline processing rules
            try:
                s.validate_df(df,newdf)
            except AttributeError:
                pass                
            if register:
                try:
                    s.register(df=df,new_df= newdf)
                except AttributeError:
                    msg = 'Could not export %s as it has no register() method' %s.__class__.__name__
                    logger.warning(msg)
            df = newdf
            if dropna:
                df = df.replace([np.inf, -np.inf], np.nan)
                df = df.dropna()
            if to_csv:
                df.to_csv('debugPipelineOut_%s.csv' %s.__class__.__name__)
            try:
                msg = 'Completed stage %s. Output dataframe.' %s.__class__.__name__
                last_msg = s.log_df_info(df,msg)
            except Exception:
                last_msg = self.log_df_info(df,msg)
            secondary_sources = [x for x in secondary_sources if x != s]
            trace_history = trace_history + ' Completed stage %s ->' %s.__class__.__name__
        return df

    def publish(self):
        
        export = []
        for s in self.stages:
            if self.entity_type is None:
                source_name = None
            else:
                source_name = self.entity_type.name
            metadata  = { 
                    'name' : s.name ,
                    'args' : s._get_arg_metadata()
                    }
            export.append(metadata)
            
        response = self.entity_type.db.http_request(object_type = 'kpiFunctions',
                                        object_name = source_name,
                                        request = 'POST',
                                        payload = export)    
        return response
    
    def get_input_items(self):
        '''
        Get the set of input items explicitly requested by each function
        Not all input items have to be specified as arguments to the function
        Some can be requested through this method
        '''
        inputs = set()
        for s in self.stages:
            try:
                inputs = inputs | s.get_input_items()
            except AttributeError:
                pass
            
        return inputs
    
    def get_stage_trace(self, stage, last_msg = ''):
        '''
        Get a trace message from the stage
        '''
        try:
            msg = stage.trace_get()
        except AttributeError:
            try: 
                msg = stage._trace
            except AttributeError:
                msg = ''
            msg = ''
        if msg is None:
            msg = ''            
        msg = msg + str(last_msg)
        return msg
    
    def log_df_info(self,df,msg):
        '''
        Log a debugging entry showing first row and index structure
        '''
        msg = msg + ' | df count: %s ' %(len(df.index))
        msg = msg + ' | df index: %s \n' %(','.join(df.index.names))

        '''
        try:
            cols = df.head(1).squeeze().to_dict()
        except AttributeError:
            cols = df.head(1).to_dict()

        for key,value in list(cols.items()):
            msg = msg + '%s : %s \n' %(key, value)
            
        '''

        logger.debug(msg)
        return msg
        
    
    def _raise_error(self,exception,msg, abort_on_fail = False):
        '''
        Raise an exception. Append a message to the stacktrace.
        '''
        msg = '%s | %s' %(str(exception),msg)
        if abort_on_fail:
            raise type(exception)(msg).with_traceback(sys.exc_info()[2])
        else:
            logger.warn(msg)
            msg = 'An exception occured during execution of a pipeline stage. The stage is configured to continue after an execution failure'
            logger.warn(msg)
            
    def set_stages(self,stages):
        '''
        Replace existing stages with a new list of stages
        '''
        self.stages = []
        if not stages is None:
            if not isinstance(stages,list):
                stages = [stages]
            self.stages.extend(stages)
        for s in self.stages:
            s._entity_type = self.entity_type        
            
          