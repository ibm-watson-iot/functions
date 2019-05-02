from collections import OrderedDict
import datetime as dt
import logging
import re

import numpy as np
import pandas as pd
import pandas.core.index

CATALOG_CATEGORY = 'TRANSFORMER'

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

    def set_stage_params(self):
        '''
        Set parameters from pipeline in each stage
        This will overwrite existing class or instance variables with the same name
        Check get_params to see which parameters are set in this way
        '''
        
        if self.source is None:
            source = self
        else:
            source = self.source
        
        for s in self.stages:
            try:
                s.set_params(**source.get_params())
            except AttributeError:
                msg = "Counldn't set parameters on stage %s because it doesn't have a set_params method" %s.__class__.__name__
            finally:
                msg = "Set parameters on stage %s" %s.__class__.__name__
            logger.debug(msg)
        
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
                logger.warning(trace, exc_info=True)
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
                #logger.warning(trace, exc_info=True)
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
    
    def get_params(self):
        '''
        Get metadata parameters
        '''
        params = {
                '_entity_type_logical_name' : self.entity_type,
                'entity_type_name' : self.eventTable,
                '_timestamp' : self.eventTimestampColumn,
                'db' : self.db,
                '_dimension_table_name' : self.dimensionTable,
                '_db_connection_dbi' : self.db_connection_dbi,
                '_db_schema' : self.schema,
                '_data_items' : self.data_items
                }
        return params
    
    
    def set_stages(self,stages):
        '''
        Replace existing stages with a new list of stages
        '''
        self.stages = []
        if not stages is None:
            if not isinstance(stages,list):
                stages = [stages]
            self.stages.extend(stages)
            
    def set_params(self, **params):
        '''
        Set parameters based using supplied dictionary
        '''
        for key,value in list(params.items()):
            setattr(self, key, value)
        return self            


class BaseCalculation:

    def _set_dms(self, dms):
        self.dms = dms

    def _get_dms(self):
        return self.dms


class Alert:

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Create alerts that are triggered when data values reach a particular range.', 
            'input': [
                {
                    'name': 'sources',
                    'description': 'Select one or more data items to build your alert.',
                    'type': 'DATA_ITEM',
                    'required': True,
                    'dataType': 'ARRAY',
                    'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#","title": "sources","type": "array","minItems": 1,"items": {"type": "string"}}
                },
                {
                    'name': 'expression',
                    'description': 'Build the expression for your alert by using Python script. To reference a data item, use the format ${DATA_ITEM}.',
                    'type': 'CONSTANT',
                    'required': True,
                    'dataType': 'LITERAL'
                }
            ], 
            'output': [
                {
                    'name': 'name',
                    'description': 'The name of the new alert.',
                    'dataType': 'BOOLEAN',
                    'tags': [
                        'ALERT',
                        'EVENT'
                    ]
                }
            ],
            'tags': [
                'EVENT'
            ]
        })

    def __init__(self, name=None, sources=None, expression=None):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if name is None or not isinstance(name, str):
            raise RuntimeError("argument name must be provided and must be a string")
        if expression is None or not isinstance(expression, str):
            raise RuntimeError("argument expression must be provided and must be a string")

        self.name = name
        self.expression = expression
        self.sources = sources
        
    def execute(self, df):
        sources_not_in_column = df.index.names
        df = df.reset_index()

        expr = re.sub(r"\$\{(\w+)\}", r"df['\1']", self.expression)
        self.logger.debug('alert_expression=%s' % str(expr))

        df[self.name] = np.where(eval(expr), True, np.nan)

        df = df.set_index(keys=sources_not_in_column)

        return df


class NewColFromCalculation:

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Create a new data item by expression.', 
            'input': [
                {
                    'name': 'sources',
                    'description': 'Select one or more data items to be used in the expression.',
                    'type': 'DATA_ITEM',
                    'required': True,
                    'dataType': 'ARRAY',
                    'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#","title": "sources","type": "array","minItems": 1,"items": {"type": "string"}}
                },
                {
                    'name': 'expression',
                    'description': 'Build the expression by using Python script. To reference a data item, use the format ${DATA_ITEM}.',
                    'type': 'CONSTANT',
                    'required': True,
                    'dataType': 'LITERAL'
                }
            ], 
            'output': [
                {
                    'name': 'name',
                    'description': 'The name of the new data item.'
                }
            ],
            'tags': [
                'EVENT',
                'JUPYTER'
            ] 
        })

    def __init__(self, name=None, sources=None, expression=None):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if name is None or not isinstance(name, str):
            raise RuntimeError("argument name must be provided and must be a string")
        if expression is None or not isinstance(expression, str):
            raise RuntimeError("argument expression must be provided and must be a string")

        self.name = name
        self.expression = expression
        self.sources = sources
        
    def execute(self, df):
        sources_not_in_column = df.index.names
        df = df.reset_index()

        expr = re.sub(r"\$\{(\w+)\}", r"df['\1']", self.expression)
        self.logger.debug('new_column_expression=%s' % str(expr))

        df[self.name] = eval(expr)

        df = df.set_index(keys=sources_not_in_column)

        return df


class Filter:

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Filter data by expression.', 
            'input': [
                {
                    'name': 'sources',
                    'description': 'Select one or more data items to be used in the expression.',
                    'type': 'DATA_ITEM',
                    'required': True,
                    'dataType': 'ARRAY',
                    'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#","title": "sources","type": "array","minItems": 1,"items": {"type": "string"}}
                },
                {
                    'name': 'expression',
                    'description': 'Build the filtering expression by using Python script. To reference a data item, use the format ${DATA_ITEM}.',
                    'type': 'CONSTANT',
                    'required': True,
                    'dataType': 'LITERAL'
                },
                {
                    'name': 'filtered_sources',
                    'description': 'Data items to be kept when expression is evaluated to be true.',
                    'type': 'DATA_ITEM',
                    'required': True,
                    'dataType': 'ARRAY',
                    'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#","title": "filtered_sources","type": "array","minItems": 1,"items": {"type": "string"}}
                }
            ], 
            'output': [
                {
                    'name': 'names',
                    'description': 'The names of the new data items.',
                    'dataTypeFrom': 'filtered_sources',
                    'cardinalityFrom': 'filtered_sources'
                }
            ],
            'tags': [
                'EVENT',
                'JUPYTER'
            ] 
        })

    def __init__(self, names=None, filtered_sources=None, sources=None, expression=None):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if names is not None and isinstance(names, str):
            names = [n.strip() for n in names.split(',') if len(n.strip()) > 0]

        if names is None or not isinstance(names, list):
            raise RuntimeError("argument names must be provided and must be a list")
        if filtered_sources is None or not isinstance(filtered_sources, list) or len(filtered_sources) != len(names):
            raise RuntimeError("argument filtered_sources must be provided and must be a list and of the same length of names")
        if filtered_sources is not None and not set(names).isdisjoint(set(filtered_sources)):
            raise RuntimeError("argument filtered_sources must not have overlapped items with names")
        if expression is None or not isinstance(expression, str):
            raise RuntimeError("argument expression must be provided and must be a string")

        self.names = {}
        for name, source in list(zip(names, filtered_sources)):
            self.names[source] = name
        self.expression = expression
        self.sources = sources
        self.filtered_sources = filtered_sources
        
    def execute(self, df):
        sources_not_in_column = []
        for idx in df.index.names:
            sources_not_in_column.append(idx)
            df = df.reset_index(level=idx, drop=False)

        expr = re.sub(r"\$\{(\w+)\}", r"df['\1']", self.expression)
        self.logger.debug('filter_expression=%s' % str(expr))

        # use copy() here to vaoid setting with copy later
        df_filtered = df[eval(expr)].copy()

        # remove conflicted names
        self.names = {name: new_name for name, new_name in self.names.items() if name in df.columns and new_name not in df.columns}

        # rename to the target 'filtered' names
        new_names = [new_name for name, new_name in self.names.items()]
        for name, new_name in self.names.items():
            df_filtered[new_name] = df_filtered[name]

        # self.logger.debug('\n%s' % str(df_filtered))
        # self.logger.debug('\n%s' % str(df))

        # move back index
        df_filtered = df_filtered.set_index(keys=sources_not_in_column, drop=True, append=True)
        df = df.set_index(keys=sources_not_in_column, drop=True, append=True)

        df_filtered = df_filtered[list(set(new_names) - set(sources_not_in_column))]
        # self.logger.debug('\n%s' % str(df_filtered))
        df = df.join(df_filtered, how='left')
        # self.logger.debug('\n%s' % str(df))

        df = df.reset_index(level=0, drop=True)

        return df


class NewColFromSql(BaseCalculation):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Create new data items by joining SQL query result.', 
            'input': [
                {
                    'name': 'sql',
                    'description': 'The SQL query.',
                    'type': 'CONSTANT',
                    'required': True,
                    'dataType': 'LITERAL'
                },
                {
                    'name': 'index_col',
                    'description': 'Columns in the SQL query result to be joined (multiple items are comma separated).',
                    'type': 'CONSTANT',
                    'required': True,
                    'dataType': 'ARRAY',
                    'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#","title": "index_col","type": "array","minItems": 1,"items": {"type": "string"}}
                },
                {
                    'name': 'parse_dates',
                    'description': 'Columns in the SQL query result to be parsed as dates (multiple items are comma separated).',
                    'type': 'CONSTANT',
                    'required': True,
                    'dataType': 'ARRAY',
                    'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#","title": "parse_dates","type": "array","minItems": 1,"items": {"type": "string"}}
                },
                {
                    'name': 'join_on',
                    'description': 'Data items to join the query result to.',
                    'type': 'DATA_ITEM',
                    'required': True,
                    'dataType': 'ARRAY',
                    'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#","title": "join_on","type": "array","minItems": 1,"items": {"type": "string"}}
                }
            ], 
            'output': [
                {
                    'name': 'names',
                    'description': 'The names of the new data items.'
                }
            ],
            'tags': [
                'JUPYTER'
            ] 
        })

    def __init__(self, names=None, sql=None, index_col=None, parse_dates=None, join_on=None):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if names is not None and isinstance(names, str):
            names = [n.strip() for n in names.split(',') if len(n.strip()) > 0]
        if index_col is not None and isinstance(index_col, str):
            index_col = [n.strip() for n in index_col.split(',') if len(n.strip()) > 0]
        if parse_dates is not None and isinstance(parse_dates, str):
            parse_dates = [n.strip() for n in parse_dates.split(',') if len(n.strip()) > 0]

        if names is None or not isinstance(names, list):
            raise RuntimeError("argument names must be provided and must be a list")
        if sql is None or not isinstance(sql, str) or len(sql) == 0:
            raise RuntimeError('argument sql must be given as a non-empty string')
        if index_col is None or not isinstance(index_col, list):
            raise RuntimeError('argument index_col must be provided and must be a list')
        if join_on is None:
            raise RuntimeError('argument join_on must be given')
        if parse_dates is not None and not isinstance(parse_dates, list):
            raise RuntimeError('argument index_col must be a list')

        self.names = names
        self.sql = sql
        self.index_col = index_col
        self.parse_dates = parse_dates
        self.join_on = asList(join_on)
    
    def execute(self, df):
        df_sql = pd.read_sql(self.sql, self._get_dms().db_connection_dbi, index_col=self.index_col, parse_dates=self.parse_dates)
        if len(self.names) > len(df_sql.columns):
            raise RuntimeError('length of names (%d) is larger than the length of query result (%d)' % (len(self.names), len(df_sql)))

        # in case the join_on is in index, reset first then set back after join
        sources_not_in_column = df.index.names
        df = df.reset_index()
        df = df.join(df_sql, on=self.join_on, how='left')
        df = df.set_index(keys=sources_not_in_column)

        renamed_cols = {df_sql.columns[idx]: name for idx, name in enumerate(self.names)}
        df = df.rename(columns=renamed_cols)

        return df


class NewColFromScalarSql(BaseCalculation):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Create a new data item from a scalar SQL query returning a single value.', 
            'input': [
                {
                    'name': 'sql',
                    'description': 'The SQL query.',
                    'type': 'CONSTANT',
                    'required': True,
                    'dataType': 'LITERAL'
                }
            ], 
            'output': [
                {
                    'name': 'name',
                    'description': 'The name of the new data item.'
                }
            ],
            'tags': [
                'JUPYTER'
            ] 
        })

    def __init__(self, name=None, sql=None):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if name is None or not isinstance(name, str):
            raise RuntimeError('argument name must be given')
        if sql is None or not isinstance(sql, str) or len(sql) == 0:
            raise RuntimeError('argument sql must be given as a non-empty string')

        self.name = name
        self.sql = sql
    
    def execute(self, df):
        df_sql = pd.read_sql(self.sql, self._get_dms().db_connection_dbi)
        if df_sql.shape != (1, 1):
            raise RuntimeError('the scalar sql=%s does not return single value, but the shape=%s' % (len(self.sql), len(df_sql.shape)))
        df[self.name] = df_sql.iloc[0, 0]
        return df


def pairwise(iterable):
    "s -> (s0, s1), (s2, s3), (s4, s5), ..."
    a = iter(iterable)
    return zip(a, a)

class Shift:
    def __init__(self, name, start, end, cross_day_to_next=True):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        self.name = name
        self.ranges = [start, end]
        self.cross_day_to_next = cross_day_to_next
        self.cross_day = (start > end)
        if self.cross_day:
            self.ranges.insert(1, dt.time(0, 0, 0))
            self.ranges.insert(1, dt.time(23, 59, 59, 999999))
        self.ranges = list(pairwise(self.ranges))
    
    def within(self, datetime):
        if isinstance(datetime, dt.datetime):
            date = dt.date(datetime.year, datetime.month, datetime.day)
            time = dt.time(datetime.hour, datetime.minute, datetime.second, datetime.microsecond)
        elif isinstance(datetime, dt.time):
            date = None
            time = datetime
        else:
            logger.debug('unknown datetime value type::%s' % datetime)
            raise ValueError('unknown datetime value type')

        for idx, range in enumerate(self.ranges):
            if range[0] <= time and time < range[1]:
                if self.cross_day and date is not None:
                    if self.cross_day_to_next and idx == 0:
                        date += dt.timedelta(days=1)
                    elif not self.cross_day_to_next and idx == 1:
                        date -= dt.timedelta(days=1)
                return (date, True)

        return False

    def start_time(self, shift_day=None):
        if shift_day is None:
            return self.ranges[0][0]
        else:
            if self.cross_day and self.cross_day_to_next:
                shift_day -= dt.timedelta(days=1)
            return dt.datetime.combine(shift_day, self.ranges[0][0])

    def end_time(self, shift_day=None):
        if shift_day is None:
            return self.ranges[-1][-1]
        else:
            if self.cross_day and not self.cross_day_to_next:
                shift_day += dt.timedelta(days=1)
            return dt.datetime.combine(shift_day, self.ranges[-1][-1])

    def __eq__(self, other):
        return self.name == other.name and self.ranges == other.ranges

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "%s: (%s, %s)" % (self.name, self.ranges[0][0], self.ranges[-1][1])


class ShiftPlan:
    def __init__(self, shifts, cross_day_to_next=True):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        shifts = {shift: [dt.time(*tuple(time)) for time in list(pairwise(time_range))] for shift, time_range in shifts.items()}

        # validation: shifts cannot overlap, gaps are allowed though

        self.shifts = []
        for shift, time_range in shifts.items():
            self.shifts.append(Shift(shift, time_range[0], time_range[1], cross_day_to_next=cross_day_to_next))

        self.shifts.sort(key=lambda x: x.ranges[0][0])

        if cross_day_to_next and self.shifts[-1].cross_day:
            self.shifts.insert(0, self.shifts[-1])
            del self.shifts[-1]
        self.cross_day_to_next = cross_day_to_next

        self.logger.debug("ShiftPlan: shifts=%s, cross_day_to_next=%s" % (self.shifts, self.cross_day_to_next))

    def get_shift(self, datetime):
        for shift in self.shifts:
            ret = shift.within(datetime)
            if ret:
                return (ret[0], shift)

        return None

    def next_shift(self, shift_day, shift):
        shift_idx = None

        for idx, shft in enumerate(self.shifts):
            if shift == shft:
                shift_idx = idx
                break

        if shift_idx is None:
            logger.debug("unknown shift: %s" % str(shift))
            raise ValueError("unknown shift: %s" % str(shift))
        
        shift_idx = shift_idx + 1
        if shift_idx >= len(self.shifts):
            shift_idx %= len(self.shifts)
            shift_day += dt.timedelta(days=1)

        return (shift_day, self.shifts[shift_idx])

    def get_real_datetime(self, shift_day, shift, time):
        if shift.cross_day == False:
            return dt.datetime.combine(shift_day, time)

        if self.cross_day_to_next and time > shift.ranges[-1][-1]:
            # cross day shift the part before midnight
            return dt.datetime.combine(shift_day - dt.timedelta(days=1), time)
        elif self.cross_day_to_next == False and time < shift.ranges[0][0]:
            # cross day shift the part after midnight
            return dt.datetime.combine(shift_day + dt.timedelta(days=1), time)
        else:
            return dt.datetime.combine(shift_day, time)

    def split(self, start, end):
        start_shift = self.get_shift(start)
        end_shift = self.get_shift(end)

        if start_shift is None:
            raise ValueError("starting time not fit in any shift: start_shift is None")
        if end_shift is None:
            raise ValueError("ending time not fit in any shift: end_shift is None")

        if start > end:
            logger.warning('starting time must not be after ending time %s %s. Ignoring end date.' % (start, end))
            return [(start_shift, start, start)]

        if start_shift == end_shift:
            return [(start_shift, start, end)]

        splits = []
        shift_day, shift = start_shift
        splits.append((start_shift, start, self.get_real_datetime(shift_day, shift, shift.ranges[-1][-1])))
        start_shift = self.next_shift(shift_day, shift)
        while start_shift != end_shift:
            shift_day, shift = start_shift
            splits.append((
                start_shift, 
                self.get_real_datetime(shift_day, shift, shift.ranges[0][0]), 
                self.get_real_datetime(shift_day, shift, shift.ranges[-1][-1])))
            start_shift = self.next_shift(shift_day, shift)
        shift_day, shift = end_shift
        splits.append((end_shift, self.get_real_datetime(shift_day, shift, shift.ranges[0][0]), end))

        return splits

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return str(self.shifts)


class IdentifyShiftFromTimestamp:

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Identifies the shift that was active when data was received by using the timestamp on the data.', 
            'input': [
                {
                    'name': 'timestamp',
                    'description': 'Specify the timestamp data item on which to base your calculation.',
                    'type': 'DATA_ITEM',
                    'required': True,
                    'dataType': 'TIMESTAMP'
                },
                {
                    'name': 'shifts',
                    'description': 'Specify the shift plan in JSON syntax. For example, {"1": [7, 30, 16, 30]} Where 1 is the shift ID, 7 is the start hour, 30 is the start minutes, 16 is the end hour, and 30 is the end minutes. You can enter multiple shifts separated by commas.',
                    'type': 'CONSTANT',
                    'required': True,
                    'dataType': 'JSON'
                },
                {
                    'name': 'cross_day_to_next',
                    'description': 'If a shift extends past midnight, count it as the first shift of the next calendar day.',
                    'type': 'CONSTANT',
                    'required': False,
                    'dataType': 'BOOLEAN'
                }
            ], 
            'output': [
                {
                    'name': 'shift_day',
                    'description': 'The staring timestamp of a day, as identified by the timestamp and the shift plan.',
                    'dataType': 'TIMESTAMP',
                    'tags': ['DIMENSION']
                },
                {
                    'name': 'shift_id',
                    'description': 'The shift ID, as identified by the timestamp and the shift plan.',
                    'dataType': 'LITERAL',
                    'tags': ['DIMENSION']
                },
                {
                    'name': 'shift_start',
                    'description': 'The starting time of the shift, as identified by the timestamp and the shift plan.',
                    'dataType': 'TIMESTAMP',
                    'tags': ['DIMENSION']
                },
                {
                    'name': 'shift_end',
                    'description': 'The ending time of the shift, as identified by the timestamp and the shift plan.',
                    'dataType': 'TIMESTAMP',
                    'tags': ['DIMENSION']
                },
                {
                    'name': 'hour_no',
                    'description': 'The hour of the day, as identified by the timestamp and the shift plan.',
                    'dataType': 'NUMBER',
                    'tags': ['DIMENSION']
                }
            ],
            'tags': [
                'JUPYTER'
            ] 
        })

    def __init__(self, shift_day=None, shift_id=None, shift_start=None, shift_end=None, hour_no=None, timestamp="timestamp", shifts=None, cross_day_to_next=True):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if shift_day is None:
            raise RuntimeError("argument shift_day must be provided: shift_day is None")
        if shift_id is None:
            raise RuntimeError("argument shift_id must be provided: shift_id is None")
        if shift_start is not None and not isinstance(shift_start, str):
            raise RuntimeError("argument shift_start must be a string")
        if shift_end is not None and not isinstance(shift_end, str):
            raise RuntimeError("argument shift_end must be a string")
        if hour_no is not None and not isinstance(hour_no, str):
            raise RuntimeError("argument hour_no must be a string")
        if timestamp is None:
            raise RuntimeError("argument timestamp must be provided: timestamp is None")
        if shifts is None or not isinstance(shifts, dict) and len(shifts) > 0:
            raise RuntimeError("argument shifts must be provided and is a non-empty dict")

        self.shift_day = shift_day
        self.shift_id = shift_id
        self.shift_start = shift_start if shift_start is not None and len(shift_start.strip()) > 0 else None
        self.shift_end = shift_end if shift_end is not None and len(shift_end.strip()) > 0 else None
        self.hour_no = hour_no if hour_no is not None and len(hour_no.strip()) > 0 else None
        self.timestamp = timestamp

        self.shifts = ShiftPlan(shifts, cross_day_to_next=cross_day_to_next)

    def execute(self,df):
        generated_values = {
            self.shift_day: [], 
            self.shift_id: [], 
            'self.shift_start': [],
            'self.shift_end': [],
            'self.hour_no': [],
        }
        df[self.shift_day] = self.shift_day
        df[self.shift_id] = self.shift_id
        if self.shift_start is not None:
            df[self.shift_start] = self.shift_start
        if self.shift_end is not None:
            df[self.shift_end] = self.shift_end
        if self.hour_no is not None:
            df[self.hour_no] = self.hour_no

        for idx in df.index:
            timestampIndex = df.index.names.index(self.timestamp) if self.timestamp in df.index.names else None
            t = idx[timestampIndex] if timestampIndex is not None else df.loc[idx, self.timestamp]
            if isinstance(t, str):
                t = pd.to_datetime(t)
            
            ret = self.shifts.get_shift(t)
            if ret is None:
                continue

            shift_day, shift = ret

            generated_values[self.shift_day].append(pd.to_datetime(shift_day.strftime('%Y-%m-%d')))
            generated_values[self.shift_id].append(shift.name)
            generated_values['self.shift_start'].append(shift.start_time(shift_day))
            generated_values['self.shift_end'].append(shift.end_time(shift_day))
            generated_values['self.hour_no'].append(t.hour)

        df.loc[:, self.shift_day] = generated_values[self.shift_day]
        df.loc[:, self.shift_id] = generated_values[self.shift_id]
        if self.shift_start is not None:
            df.loc[:, self.shift_start] = generated_values['self.shift_start']
        if self.shift_end is not None:
            df.loc[:, self.shift_end] = generated_values['self.shift_end']
        if self.hour_no is not None:
            df.loc[:, self.hour_no] = generated_values['self.hour_no']
        
        return df


class SplitDataByActiveShifts:

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Identifies the shift that was active when data was received by using the timestamp on the data.', 
            'input': [
                {
                    'name': 'start_timestamp',
                    'description': 'Specify the timestamp data item on which the data to be split must be based.',
                    'type': 'DATA_ITEM',
                    'required': True,
                    'dataType': 'TIMESTAMP'
                },
                {
                    'name': 'end_timestamp',
                    'description': 'Specify the timestamp data item on which the data to be split must be based.',
                    'type': 'DATA_ITEM',
                    'required': True,
                    'dataType': 'TIMESTAMP'
                },
                {
                    'name': 'shifts',
                    'description': 'Specify the shift plan in JSON syntax. For example, {"1": [7, 30, 16, 30]} Where 1 is the shift ID, 7 is the start hour, 30 is the start minutes, 16 is the end hour, and 30 is the end minutes. You can enter multiple shifts separated by commas.',
                    'type': 'CONSTANT',
                    'required': True,
                    'dataType': 'JSON'
                },
                {
                    'name': 'cross_day_to_next',
                    'description': 'If a shift extends past midnight, count it as the first shift of the next calendar day.',
                    'type': 'CONSTANT',
                    'required': False,
                    'dataType': 'BOOLEAN'
                }
            ], 
            'output': [
                {
                    'name': 'shift_day',
                    'description': 'The staring timestamp of a day, as identified by the timestamp and the shift plan.',
                    'dataType': 'TIMESTAMP',
                    'tags': ['DIMENSION']
                },
                {
                    'name': 'shift_id',
                    'description': 'The shift ID, as identified by the timestamp and the shift plan.',
                    'dataType': 'LITERAL',
                    'tags': ['DIMENSION']
                },
                {
                    'name': 'shift_start',
                    'description': 'The starting time of the shift, as identified by the timestamp and the shift plan.',
                    'dataType': 'TIMESTAMP',
                    'tags': ['DIMENSION']
                },
                {
                    'name': 'shift_end',
                    'description': 'The ending time of the shift, as identified by the timestamp and the shift plan.',
                    'dataType': 'TIMESTAMP',
                    'tags': ['DIMENSION']
                }
            ],
            'tags': [
                'JUPYTER'
            ] 
        })
    
    def __init__(self, start_timestamp, end_timestamp, ids='id', shift_day=None, shift_id=None, shift_start=None, shift_end=None, shifts=None, cross_day_to_next=True):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if ids is None:
            raise RuntimeError("argument ids must be provided")
        if start_timestamp is None:
            raise RuntimeError("argument start_timestamp must be provided")
        if end_timestamp is None:
            raise RuntimeError("argument end_timestamp must be provided")
        if shift_day is None:
            raise RuntimeError("argument shift_day must be provided")
        if shift_id is None:
            raise RuntimeError("argument shift_id must be provided")
        if shift_start is not None and not isinstance(shift_start, str):
            raise RuntimeError("argument shift_start must be a string")
        if shift_end is not None and not isinstance(shift_end, str):
            raise RuntimeError("argument shift_end must be a string")
        if shifts is None or not isinstance(shifts, dict) and len(shifts) > 0:
            raise RuntimeError("argument shifts must be provided and is a non-empty dict")

        self.ids = ids
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp

        self.shift_day = shift_day
        self.shift_id = shift_id
        self.shift_start = shift_start if shift_start is not None and len(shift_start.strip()) > 0 else None
        self.shift_end = shift_end if shift_end is not None and len(shift_end.strip()) > 0 else None

        self.shifts = ShiftPlan(shifts, cross_day_to_next=cross_day_to_next)

    def execute(self,df):
        generated_rows = []
        generated_values = {
            self.shift_day: [], 
            self.shift_id: [], 
            'self.shift_start': [],
            'self.shift_end': [],
        }
        append_generated_values = {
            self.shift_day: [], 
            self.shift_id: [], 
            'self.shift_start': [],
            'self.shift_end': [],
        }
        df[self.shift_day] = self.shift_day
        df[self.shift_id] = self.shift_id
        if self.shift_start is not None:
            df[self.shift_start] = self.shift_start
        if self.shift_end is not None:
            df[self.shift_end] = self.shift_end

        # self.logger.debug('df_index_before_move=%s' % str(df.index.to_frame().dtypes.to_dict()))
        indexes_moved_to_columns = []
        for idx in df.index.names:
            indexes_moved_to_columns.append(idx)
            df = df.reset_index(level=idx, drop=False)
        # self.logger.debug('df_index_after_move=%s, df_columns=%s' % (str(df.index.to_frame().dtypes.to_dict()), str(df.dtypes.to_dict())))

        cnt = 0
        cnt2 = 0
        for idx, row in df.iterrows():
            if cnt % 1000 == 0:
                self.logger.debug('%d rows processed, %d rows added' % (cnt, cnt2))

            cnt += 1
            if pd.notna(row[self.start_timestamp]) and pd.notna(row[self.end_timestamp]):
                result_rows = self.shifts.split(pd.to_datetime(row[self.start_timestamp]), pd.to_datetime(row[self.end_timestamp]))
            elif pd.notna(row[self.start_timestamp]):
                shift_day, shift = self.shifts.get_shift(pd.to_datetime(row[self.start_timestamp]))
                generated_values[self.shift_day].append(pd.to_datetime(shift_day.strftime('%Y-%m-%d')))
                generated_values[self.shift_id].append(shift.name)
                generated_values['self.shift_start'].append(shift.start_time(shift_day))
                generated_values['self.shift_end'].append(shift.end_time(shift_day))
                continue
            else:
                generated_values[self.shift_day].append(None)
                generated_values[self.shift_id].append(None)
                generated_values['self.shift_start'].append(None)
                generated_values['self.shift_end'].append(None)
                continue
            # self.logger.debug(result_rows)

            row_copy = None
            if len(result_rows) > 1:
                # do the selection up front only once otherwise we'll select what just been appended
                # row_copy = df.loc[idx].copy()
                row_copy = row.copy()

            for i, result_row in enumerate(result_rows):
                shift_day, shift = result_row[0]
                start_timestamp = result_row[1]
                end_timestamp = result_row[2]

                if i == 0:
                    # accessing original row must not be through the itterrows's row since that's a copy
                    # but accessing by loc slicing is really slow, so we only do it when needed, and it is 
                    # assumed cross-shift is relatively rare
                    if len(result_rows) > 1:
                        df.loc[idx, self.start_timestamp] = start_timestamp
                        df.loc[idx, self.end_timestamp] = end_timestamp

                    generated_values[self.shift_day].append(pd.to_datetime(shift_day.strftime('%Y-%m-%d')))
                    generated_values[self.shift_id].append(shift.name)
                    generated_values['self.shift_start'].append(shift.start_time(shift_day))
                    generated_values['self.shift_end'].append(shift.end_time(shift_day))
                else:
                    cnt2 += 1
                    new_row = row_copy.copy()
                    new_row[self.start_timestamp] = start_timestamp
                    new_row[self.end_timestamp] = end_timestamp
                    generated_rows.append(new_row)

                    append_generated_values[self.shift_day].append(pd.to_datetime(shift_day.strftime('%Y-%m-%d')))
                    append_generated_values[self.shift_id].append(shift.name)
                    append_generated_values['self.shift_start'].append(shift.start_time(shift_day))
                    append_generated_values['self.shift_end'].append(shift.end_time(shift_day))

        self.logger.debug('original_rows=%d, rows_added=%d' % (cnt, cnt2))
        if len(generated_rows) > 0:
            # self.logger.debug('df_shape=%s' % str(df.shape))
            df = df.append(generated_rows, ignore_index=True)
            self.logger.debug('df_shape=%s' % str(df.shape))

            generated_values[self.shift_day].extend(append_generated_values[self.shift_day])
            generated_values[self.shift_id].extend(append_generated_values[self.shift_id])
            generated_values['self.shift_start'].extend(append_generated_values['self.shift_start'])
            generated_values['self.shift_end'].extend(append_generated_values['self.shift_end'])

        self.logger.debug('length_generated_values=%s, length_generated_rows=%s' % (len(generated_values[self.shift_day]), len(generated_rows)))

        df[self.shift_day] = generated_values[self.shift_day]
        df[self.shift_id] = generated_values[self.shift_id]
        if self.shift_start is not None:
            df[self.shift_start] = generated_values['self.shift_start']
        if self.shift_end is not None:
            df[self.shift_end] = generated_values['self.shift_end']

        df = df.set_index(keys=indexes_moved_to_columns, drop=True, append=False)

        return df


class MergeByFirstValid:

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Create alerts that are triggered when data values reach a particular range.', 
            'input': [
                {
                    'name': 'sources',
                    'description': 'Select one or more data items to be merged.',
                    'type': 'DATA_ITEM',
                    'required': True,
                    'dataType': 'ARRAY',
                    'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#","title": "sources","type": "array","minItems": 1,"items": {"type": "string"}}
                }
            ], 
            'output': [
                {
                    'name': 'name',
                    'description': 'The new data item name for the merge result to create.',
                    'dataTypeFrom': 'sources'
                }
            ],
            'tags': [
                'EVENT',
                'JUPYTER'
            ] 
        })

    def __init__(self, name=None, sources=None):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if name is None or not isinstance(name, str):
            raise RuntimeError("argument name must be provided and must be a string")

        self.name = name
        self.sources = sources
        
    def execute(self, df):
        sources_not_in_column = []
        for idx in df.index.names:
            sources_not_in_column.append(idx)
            df = df.reset_index(level=idx, drop=False)

        df[self.name] = df[self.sources].bfill(axis=1).iloc[:, 0]
        msg = 'MergeByFirstValid %s' % df[self.name].unique()[0:50]
        self.logger.debug(msg)

        msg = 'Null merge key: %s' % df[df[self.name].isna()].head(1).transpose()
        self.logger.debug(msg)

        # move back index
        df = df.set_index(keys=sources_not_in_column)

        return df


class DurationFromPrevious:

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Create alerts that are triggered when data values reach a particular range.', 
            'input': [
                {
                    'name': 'source',
                    'description': 'Select one or more data items to be merged.',
                    'type': 'DATA_ITEM',
                    'required': True
                },
                {
                    'name': 'timestamp',
                    'description': 'Select one or more data items to be merged.',
                    'type': 'DATA_ITEM',
                    'required': True
                },
                {
                    'name': 'unit',
                    'description': 'Select one or more data items to be merged.',
                    'type': 'CONSTANT',
                    'required': False,
                    'dataType': 'LITERAL'
                },
                {
                    'name': 'changed',
                    'description': 'Select one or more data items to be merged.',
                    'type': 'CONSTANT',
                    'required': False,
                    'dataType': 'BOOLEAN'
                },
                {
                    'name': 'changefrom',
                    'description': 'Select one or more data items to be merged.',
                    'type': 'CONSTANT',
                    'required': False,
                    'dataType': 'ARRAY'
                }
            ], 
            'output': [
                {
                    'name': 'name',
                    'description': 'The new data item name for the merge result to create.',
                    'dataType': 'NUMBER'
                }
            ],
            'tags': [
                'EVENT',
                'JUPYTER'
            ] 
        })

    def __init__(self, name, source, timestamp='timestamp', unit='s', isin=None, notin=None, changed=False, changefrom=None):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if name is None:
            raise RuntimeError("argument name must be provided")
        if source is None:
            raise RuntimeError("argument source must be provided")
        if timestamp is not None and not isinstance(timestamp, str):
            raise RuntimeError("argument timestamp must be a string specifiying the time series index")
        if unit not in {"ns", "us", "ms", "s", "m", "h", "D"}:
            raise RuntimeError('argument unit must be one of "ns", "us", "ms", "s", "m", "h", "D"')
        if changefrom is not None:
            # if not isinstance(changefrom, list) or not all([(isinstance(cf, list) and len(cf) == 2) for cf in changefrom]):
            if not isinstance(changefrom, list) or not (isinstance(changefrom, list) and len(changefrom) == 2):
                raise RuntimeError('argument changefrom must be 2-element list [current, previous]')

        self.name = name
        self.source = source
        self.timestamp = timestamp
        self.unit = unit
        if isin is not None:
            isin = asList(isin)
        self.isin = isin
        if notin is not None:
            notin = asList(notin)
        self.notin = notin
        self.changed = changed
        self.changefrom = changefrom
        
    def execute(self, df):
        df_final = df.copy()

        # first, leave only the source column for processing
        df_final = df_final.dropna(subset=[self.source])

        # apply filters

        if self.changed:
            df_final = df_final[df_final.shift() != df_final]

        if self.isin is not None and self.notin is not None:
            df_final = df_final[df_src[self.source].isin(self.isin) & ~df_src[self.source].isin(self.notin)]
        elif self.isin is not None:
            df_final = df_final[df_src[self.source].isin(self.isin)]
        elif self.notin is not None:
            df_final = df_final[~df_src[self.source].isin(self.notin)]

        self.logger.debug("!!! df_columns=%s, df_indexes=%s, df=\n%s" % (df_final.dtypes.to_dict(), df_final.index.to_frame().dtypes.to_dict(), df_final.head()))

        # move timestamp from index to column
        df_final = df_final.reset_index(1)

        self.logger.debug("!!! df_columns=%s, df_indexes=%s, df=\n%s" % (df_final.dtypes.to_dict(), df_final.index.to_frame().dtypes.to_dict(), df_final.head()))

        if df_final.empty:
            df[self.name] = None
            return df.astype({self.name: float})

        # calculate the diff (duration) from immediate rows
        df_final[self.name] = (df_final[self.timestamp].diff() / np.timedelta64(1, self.unit)).astype(float)

        # apply changefrom filter which must be applied after the duration is calculated, otherwise, 
        # the "immediate" previous row would be wrong
        if self.changefrom is not None and isinstance(self.changefrom, list) and len(self.changefrom) > 0:
            prev_source = 'prev_%s' % self.source
            df_final[prev_source] = df_final[self.source].shift(1)
            boolean_series = None
            current, previous = self.changefrom
            if boolean_series is None:
                boolean_series = (df_final[self.source] == current) & (df_final[prev_source] == previous)
            else:
                boolean_series = boolean_series | (df_final[self.source] == current) & (df_final[prev_source] == previous)
            df_final = df_final[boolean_series].drop(columns=[prev_source])

        # move timestamp back to index
        df_final = df_final.set_index([self.timestamp], append=True).drop([self.source], axis=1)

        # before merge, leave only the new duration column in the right side
        df_final = df_final[[x for x in df_final.columns if x in [self.name]]]

        # merge into the original dataframe
        df_final = df.merge(df_final, how='left', left_index=True, right_index=True)

        return df_final


class MissingData:
    
    def __init__(self, name, source, missing_fill=None, dropna=False, ids='id'):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if name is None:
            raise RuntimeError("argument name must be provided")
        if source is None:
            raise RuntimeError("argument source must be provided")
        if missing_fill is None:
            raise RuntimeError("argument missing_fill must be provided")

        self.names = asList(name)
        self.sources = asList(source)
        self.missing_fill = asList(missing_fill)
        self.dropna = dropna
        self.ids = ids
        if self.ids is not None:
            self.ids = asList(self.ids)

        if len(self.names) != len(self.sources):
            raise RuntimeError("argument sources must be of the same length of names")
        if len(self.names) != len(self.missing_fill):
            raise RuntimeError("argument missing_fill must be of the same length of names")
        
    def __calc(self, df):
        for src, name, fill in list(zip(self.sources, self.names, self.missing_fill)):
            if src in df:
                if fill in ['bfill','backfill','pad','ffill']:
                    df[name] = df[src].fillna(method=fill)
                else:
                    df[name] = df[src].fillna(value=fill)

        if self.dropna:
            df = df.replace([np.inf, -np.inf], np.nan)
            df = df.dropna()

        return df
    
    def execute(self, df):
        if isinstance(df.index, pd.core.index.MultiIndex):
            group_base = []
            if self.ids is not None and len(self.ids) > 0:
                for idcol in self.ids:
                    group_base.append(pd.Grouper(axis=0, level=df.index.names.index(idcol)) if idcol in df.index.names else idcol)

            df = df.groupby(group_base).apply(self.__calc)
        else:
            df = self.__calc(df)
            
        return df


class ChangeFromPrevious:
    
    def __init__(self, name=None, source=None, ids='id'):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if name is None:
            raise RuntimeError("argument name must be provided")
        if source is None:
            raise RuntimeError("argument source must be provided")

        self.names = asList(name)
        self.sources = asList(source)
        self.ids = asList(ids)

        if len(self.names) != len(self.sources):
            raise RuntimeError("argument sources must be provided and of the same length of names")
        
    def __calc(self, df):
        dff =  df[self.sources].diff()
        dff.columns = self.names

        df = df.join(dff, how='inner', lsuffix="_l")

        return df

    def execute(self, df):
        if isinstance(df.index, pd.core.index.MultiIndex):
            if set(self.ids).issubset(df.index.names):
                gf = df.groupby(level=[self.ids], as_index=False, group_keys=False)
            else:
                gf = df.groupby([self.ids], as_index=False, group_keys=False)
            df = gf.apply(self.__calc)
        else:
            df = self.__calc(df)

        return df


class ExcludeCols:
    
    def __init__(self, sources=None):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if sources is None:
            raise RuntimeError("argument sources must be provided")
        self.sources = asList(sources)
        
    def execute(self, df):
        cols = [x for x in df.columns if x not in self.sources]
        df = df[cols]
        return df

