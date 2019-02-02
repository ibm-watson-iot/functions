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
import re
import numpy as np
import sys
from .util import log_df_info
#from .bif import IoTExpression
logger = logging.getLogger(__name__)


class CalcPipeline:
    '''
    A CalcPipeline executes a series of dataframe transformation stages.
    '''
    def __init__(self,stages = None,entity_type =None):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))
        self.entity_type = entity_type
        self.set_stages(stages)
        self.log_pipeline_stages()
        
    def add_expression(self,name,expression):
        '''
        Add a new stage using an expression
        '''
        stage = PipelineExpression(name=name,expression=expression)
        self.add_stage(stage)
        
    def add_stage(self,stage):
        '''
        Add a new stage to a pipeline. A stage is Transformer or Aggregator.
        '''
        stage.set_entity_type(self.entity_type)
        self.stages.append(stage)
          
        
    def _extract_preload_stages(self):
        '''
        pre-load stages are special stages that are processed outside of the pipeline
        they execute before loading data into the pipeline
        return tuple containing list of preload stages and list of other stages to be processed
        
        also extract scd lookups. Place them on the entity.
        '''
        stages = []
        extracted_stages = []
        for s in self.stages:
            try:
                is_preload = s.is_preload
            except AttributeError:
                is_preload = False
            #extract preload stages
            if is_preload:
                msg = 'Extracted preload stage %s from pipeline' %s.__class__.__name__
                logger.debug(msg)
                extracted_stages.append(s)
            else:
                stages.append(s)
                
        return (extracted_stages,stages)
                        
    
    def _execute_preload_stages(self, start_ts = None, end_ts = None, entities = None, register= False):
        '''
        Extract and run preload stages
        Return remaining stages to process
        '''
        (preload_stages,stages) = self._extract_preload_stages()
        preload_item_names = []
        #if no dataframe provided, querying the source entity to get one
        for p in preload_stages:
            if not self.entity_type._is_preload_complete:
                status = p.execute(df=None,start_ts=start_ts,end_ts=end_ts,entities=entities)
                if register:
                    p.register(df=None)
                self.trace_append(' preloaded %s ->' %p.__class__.__name__)
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
        self.entity_type._is_preload_complete = True
        return(stages,preload_item_names)
    
    
    def _execute_data_sources(self,stages,
                                df,
                                start_ts=None,
                                end_ts=None,
                                entities=None,
                                to_csv=False,
                                register=False,
                                dropna = False):
        '''
        Extract and execute data source stages with a merge_method of replace.
        Identify other data source stages that add rows of data to the pipeline
        '''
        remaining_stages = []
        secondary_sources = []
        special_lookup_stages = []
        replace_count = 0
        for s in stages:
            try:
                is_data_source =  s.is_data_source
                merge_method = s.merge_method
            except AttributeError:
                is_data_source = False
                merge_method = None        
                
            try:
                is_scd_lookup = s.is_scd_lookup
            except AttributeError:
                is_scd_lookup = False
            else:
                self.entity_type._add_scd_pipeline_stage(s)

            try:
                is_custom_calendar = s.is_custom_calendar
            except AttributeError:
                is_custom_calendar = False
            else:
                self.entity_type.set_custom_calendar(s)
                  
            if is_data_source and merge_method == 'replace':
                self.trace_append(' replace data ')
                df = self._execute_stage(stage=s,
                    df = df,
                    start_ts = start_ts,
                    end_ts = end_ts,
                    entities = entities,
                    register = register,
                    to_csv = to_csv,
                    dropna = dropna,
                    abort_on_fail = True)
                msg = 'replaced incoming dataframe with custom data source %s' %s.__class__.__name__
                logger.debug(msg)
                
            elif is_data_source and merge_method == 'outer':
                '''
                A data source with a merge method of outer is considered a secondary source
                A secondary source can add rows of data to the pipeline.
                '''
                secondary_sources.append(s)
            elif is_scd_lookup or is_custom_calendar:
                special_lookup_stages.append(s)
            else:
                remaining_stages.append(s)
        if replace_count > 1:
            self.logger.warning("The pipeline has more than one custom source with a merge strategy of replace. The pipeline will only contain data from the last replacement")        
        
        #execute secondary data sources
        if len(secondary_sources) > 0:
            self.trace_append('>secondary data sources:')
            for s in secondary_sources:
                msg = 'processing secondary data source %s' %s.__class__.__name__
                logger.debug(msg)
                df = self._execute_stage(stage=s,
                    df = df,
                    start_ts = start_ts,
                    end_ts = end_ts,
                    entities = entities,
                    register = register,
                    to_csv = to_csv,
                    dropna = dropna,
                    abort_on_fail = True)
            
        #execute custom calendar
        if len(special_lookup_stages) > 0:
            self.trace_append('>special lookup stages:')
            for s in special_lookup_stages:
                msg = 'processing special lookup stage %s' %s.__class__.__name__
                logger.debug(msg)
                df = self._execute_stage(stage=s,
                    df = df,
                    start_ts = start_ts,
                    end_ts = end_ts,
                    entities = entities,
                    register = register,
                    to_csv = to_csv,
                    dropna = dropna,
                    abort_on_fail = True)    
            
        return(df,remaining_stages)    
            
                
    def execute(self, df=None, to_csv=False, dropna=False, start_ts = None, end_ts = None, entities = None, preloaded_item_names=None,
                register = False):
        '''
        Execute the pipeline using an input dataframe as source.
        '''
        #preload may  have already taken place. if so pass the names of the items produced by stages that were executed prior to loading.
        if preloaded_item_names is None:
            preloaded_item_names = []
        msg = 'Running pipeline with start timestamp %s' %start_ts
        logger.debug(msg)
        is_initial_transform = self.get_initial_transform_status()
        # A single execution can contain multiple CalcPipeline executions
        # An initial transform and one or more aggregation executions and post aggregation transforms
        # Behavior is different during initial transform
        if is_initial_transform:
            if not start_ts is None:
                msg = 'start ts %s :' %start_ts
                self.trace_append(msg)
            #process preload stages first if there are any
            (stages,preload_item_names) = self._execute_preload_stages(start_ts = start_ts, end_ts = end_ts, entities = entities,register=register)
            preloaded_item_names.extend(preload_item_names)
            if df is None:
                msg = 'No dataframe supplied for pipeline execution. Getting entity source data'
                logger.debug(msg)
                df = self.entity_type.get_data(start_ts=start_ts, end_ts = end_ts, entities = entities)            
            #Divide the pipeline into data retrieval stages and transformation stages. First look for
            #a primary data source. A primary data source will have a merge_method of 'replace'. This
            #implies that it replaces whatever data was fed into the pipeline as default entity data.
            (df,stages) = self._execute_data_sources (
                                                df = df,
                                                stages = stages,
                                                start_ts = start_ts,
                                                end_ts = end_ts,
                                                entities = entities,
                                                to_csv = to_csv,
                                                register = register,
                                                dropna =  dropna
                                                )
                          
        else:
            stages = []
            stages.extend(self.stages)
        if df is None:
            msg = 'Pipeline has no source dataframe'
            raise ValueError (msg)
        if to_csv:
            filename = 'debugPipelineSourceData.csv'
            df.to_csv(filename)
        if dropna:
            df = df.replace([np.inf, -np.inf], np.nan)
            df = df.dropna()
        # remove rows that contain all nulls ignore deviceid and timestamp
        if self.entity_type.get_param('_drop_all_null_rows'):
            exclude_cols = self.get_system_columns()
            exclude_cols.extend(self.entity_type.get_param('_custom_exclude_col_from_auto_drop_nulls'))
            msg = 'columns excluded when dropping null rows %s' %exclude_cols
            logger.debug(msg)
            subset = [x for x in df.columns if x not in exclude_cols]
            msg = 'columns considered when dropping null rows %s' %subset
            logger.debug(msg)
            for col in subset:
                count = df[col].count()
                msg = '%s count not null: %s' %(col,count)
                logger.debug(msg)
            df = df.dropna(how='all', subset = subset )
            self.log_df_info(df,'post drop all null rows')
        else:
            logger.debug('drop all null rows disabled')
        #add a dummy item to the dataframe for each preload stage
        #added as the ui expects each stage to contribute one or more output items
        for pl in preloaded_item_names:
            df[pl] = True
        for s in stages:
            if df.empty:
                self.logger.info('No data retrieved from all sources. Exiting pipeline execution')        
                break
                #skip this stage of it is not a secondary source             
            df = self._execute_stage(stage=s,
                                df = df,
                                start_ts = start_ts,
                                end_ts = end_ts,
                                entities = entities,
                                register = register,
                                to_csv = to_csv,
                                dropna = dropna,
                                abort_on_fail = True)
        self.mark_initial_transform_complete()
        return df
    
    
    def _execute_stage(self,stage,df,start_ts,end_ts,entities,register,to_csv,dropna, abort_on_fail): 
        #check to see if incoming data has a conformed index, conform if needed
        try:
            abort_on_fail = stage._abort_on_fail
        except AttributeError:
            abort_on_fail = abort_on_fail
        try:
            df = stage.conform_index(df=df)
        except AttributeError:
            pass
        except KeyError as e:
            msg = self.log_df_info(df,'conform_index')
            msg = 'KeyError while conforming index (%s) ' %msg
            self.trace_append(msg)
            self._entity_type.raise_error(exception = e,abort_on_fail = abort_on_fail)
        msg = ' Dataframe at start of %s: ' %stage.__class__.__name__
        self.log_df_info(df,msg)
        try:
            last_msg = stage.log_df_info(df,msg)
        except Exception:
            last_msg = self.log_df_info(df,msg)
        #there are two signatures for the execute method
        try:
            try:
                newdf = stage.execute(df=df,start_ts=start_ts,end_ts=end_ts,entities=entities)
            except TypeError:
                newdf = stage.execute(df=df)
        except AttributeError as e:
            self.trace_append(' The function makes a reference to an object property that does not exist')
            self.entity_type.raise_error(exception = e,abort_on_fail = abort_on_fail)
        except SyntaxError as e:
            self.trace_append(' The function contains a syntax error. If the function configuration includes a type-in expression, make sure that this expression is correct')
            self.entity_type.raise_error(exception = e,abort_on_fail = abort_on_fail)
        except (ValueError,TypeError) as e:
            self.trace_append('The function is operating on data that has an unexpected value or data type')
            self.entity_type.raise_error(exception = e,abort_on_fail = abort_on_fail)
        except NameError as e:
            self.trace_append(' The function referred to an object that does not exist. You may be referring to data items in pandas expressions, ensure that you refer to them by name, ie: as a quoted string. ')
            self.entity_type.raise_error(exception = e,abort_on_fail = abort_on_fail)
        except Exception as e:
            self.trace_append(' The function failed to execute ')
            self.entity_type.raise_error(exception = e,abort_on_fail = abort_on_fail)
        #validate that stage has not violated any pipeline processing rules
        try:
            stage.validate_df(df,newdf)
        except AttributeError:
            msg = 'Function has no validate_df method. Skipping validation of the dataframe'
            logger.debug(msg)
        if register:
            try:
                stage.register(df=df,new_df= newdf)
            except AttributeError as e:
                msg = 'Could not export %s as it has no register() method or because an AttributeError was raised during execution' %stage.__class__.__name__
                logger.warning(msg)
                logger.warning(str(e))
        if dropna:
            newdf = newdf.replace([np.inf, -np.inf], np.nan)
            newdf = newdf.dropna()
        if to_csv:
            newdf.to_csv('debugPipelineOut_%s.csv' %stage.__class__.__name__)
        try:
            msg = 'Completed stage %s. Output dataframe.' %stage.__class__.__name__
            last_msg = stage.log_df_info(newdf,msg)
        except Exception:
            last_msg = self.log_df_info(newdf,msg)
        self.trace_append(' completed %s ->' %stage.__class__.__name__)            
        return newdf
    
    def get_custom_calendar(self):
        '''
        Get the optional custom calendar for the entity type
        '''
        return self.entity_type._custom_calendar
    
    def get_initial_transform_status(self):
        '''
        Determine whether initial transform stage is complete
        '''
        return self.entity_type._is_initial_transform    
    
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
    
    def get_scd_lookup_stages(self):
        '''
        Get the scd lookup stages for the entity type
        '''
        return self.entity_type._scd_stages
    
    def get_system_columns(self):
        '''
        Get a list of system columns for the entity type
        '''
        return self.entity_type._system_columns

    
    def log_df_info(self,df,msg,include_data=False):
        '''
        Log a debugging entry showing first row and index structure
        '''
        msg = log_df_info(df=df,msg=msg,include_data = include_data)
        return msg
    
    def log_pipeline_stages(self):
        '''
        log pipeline stage metadata
        '''
        msg = 'pipeline stages (initial_transform=%s) ' %self.entity_type._is_initial_transform
        for s in self.stages:
            msg = msg + s.__class__.__name__
            msg = msg + ' > '
        return msg
    
    def mark_initial_transform_complete(self):
        self.entity_type._is_initial_transform = False
        
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
    
    
    def _raise_error(self,exception,msg, abort_on_fail = False):
        #kept this method to preserve compatibility when
        #moving raise_error to the EntityType
        self.get_entity_type().raise_error(
                exception = exception,
                msg = msg,
                abort_on_fail = abort_on_fail
                )

            
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
            try:
                s.set_entity_type(self.entity_type)
            except AttributeError:
                s._entity_type = self.entity_type
                
    def __str__(self):
        
        return self.__class__.__name__
            
    def trace_append(self,msg,created_by = None, title = None, log_method = None, **kwargs):
        '''
        Append to the trace information collected the entity type
        '''
        if created_by is None:
            created_by = self
            
        if title is None:
            title = 'CalcPipeline execution'
        
        self.entity_type.trace_append(created_by=created_by,
                                      title = title,
                                      msg = msg,
                                      log_method=log_method,
                                      **kwargs)     


class PipelineExpression(object):
    '''
    Create a new item from an expression involving other items
    '''
    def __init__(self, expression , name):
        self.expression = expression
        self.name = name
        super().__init__()
        self.input_items = []
                
    def execute(self, df):
        df = df.copy()
        self.infer_inputs(df)
        if '${' in self.expression:
            expr = re.sub(r"\$\{(\w+)\}", r"df['\1']", self.expression)
        else:
            expr = self.expression
        try:
            df[self.name] = eval(expr)
        except SyntaxError:
            msg = 'Syntax error while evaluating expression %s' %expr
            raise SyntaxError (msg)
        else:
            msg = 'Evaluated expression %s' %expr
            self.get_entity_type().trace_append(msg)
        return df

    def get_input_items(self):
        return self.input_items
    
    def infer_inputs(self,df):
        #get all quoted strings in expression
        possible_items = re.findall('"([^"]*)"', self.expression)
        possible_items.extend(re.findall("'([^']*)'", self.expression))
        self.input_items = [x for x in possible_items if x in list(df.columns)]       
            

            
          