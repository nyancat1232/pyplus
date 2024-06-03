import pandas as pd
from sqlalchemy.sql import text
import sqlalchemy
from typing import Literal,Self
from datetime import date,tzinfo
from zoneinfo import ZoneInfo
import numpy as np
import pyplus.builtin as bp
import networkx as nx
from warnings import warn

stmt_find_identity = text(f'''SELECT attname as identity_column
FROM pg_attribute 
JOIN pg_class 
    ON pg_attribute.attrelid = pg_class.oid
JOIN pg_namespace
    ON pg_class.relnamespace = pg_namespace.oid
WHERE nspname = :schema
AND relname = :table
AND attidentity = 'a';
''')

stmt_get_types = text(f'''
SELECT column_name, data_type, udt_name, domain_name
FROM information_schema.columns
WHERE table_schema = :schema AND 
table_name = :table;
''')
stmt_get_types_col=['column_name','data_type','udt_name','domain_name']

def _apply_escaping(sentence:str):
    return sentence.replace("'","''")

def _conversion_Sql_value(val:None|int|np.integer|float|np.floating|str|date|pd.Timestamp|list)->str:
    match val:
        case None:
            return 'NULL'
        case int()|np.integer():
            return f"'{str(val)}'"
        case float()|np.floating():
            return f"'{str(val)}'"
        case str():
            return f"'{_apply_escaping(val)}'"
        case pd.Timestamp():
            if val.strftime('%z') != '+0000':
                raise NotImplementedError('Not UTC timezone.')
            return f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'"
        case date():
            if val is not pd.NaT:
                escape_str = val.strftime("%Y-%m-%d")
                return f"'{escape_str}'"
            else:
                return 'NULL'
        case list():
            val_after = [f'"{str(v)}"' for v in val]
            return "'{"+f"{','.join(val_after)}"+"}'"
        case _:
            raise NotImplementedError(type(val))

def _convert_pgsql_type_to_pandas_type(pgtype:str,precision:Literal['ns']='ns',
                                       tz:str|int|tzinfo|None=ZoneInfo('UTC')):
    #https://pandas.pydata.org/pandas-docs/stable/user_guide/basics.html#basics-dtypes
    match pgtype:
        case 'bigint':
            return pd.Int64Dtype() #Int vs int
        case 'integer':
            return pd.Int32Dtype() #Int vs int
        case 'boolean':
            return pd.BooleanDtype()
        case 'text':
            return pd.StringDtype()
        case 'double precision':
            return pd.Float64Dtype()
        case 'date':
            return 'object'
        case 'timestamp without time zone':
            return f'datetime64[{precision}]'
        case 'timestamp with time zone':
            return pd.DatetimeTZDtype(precision,tz=tz)
        case 'interval':
            return 'str'
        case 'ARRAY':
            return 'object'
        case _:
            raise NotImplementedError(pgtype)

def _ret_a_line(key:str,dtype:str):
    def _default_value_of_type(type:str):
        match type:
            case 'date':
                return ['DEFAULT','now()']
            case 'timestamp without time zone':
                return ['DEFAULT','now()']
            case 'timestamp with time zone':
                return ['DEFAULT','now()']
            case _:
                return None
            
    ret= [f'"{key}"',dtype]
    if (retplus := _default_value_of_type(dtype)) is not None:
        ret.extend(retplus)
    return ret

_reserved_columns = ['id']

class TableStructure:
    '''
    TableStructure is a class that easily operate Create, Read, Update databases especially a table with foreign columns.
    '''
    schema_name : str
    table_name : str
    engine : sqlalchemy.Engine

    column_identity : str

    def _get_foreign_tables_list(self):
        sql = text(f'''
        SELECT KCU.column_name AS current_column_name,
            CCU.table_schema AS upper_schema, 
            CCU.table_name AS upper_table
        FROM information_schema.key_column_usage AS KCU
        JOIN information_schema.constraint_column_usage AS CCU ON KCU.constraint_name = CCU.constraint_name
        JOIN information_schema.table_constraints AS TC ON KCU.constraint_name = TC.constraint_name
        WHERE TC.constraint_type = 'FOREIGN KEY'
        AND KCU.table_schema=:schema
        AND KCU.table_name=:table;
        ''')
        sql_col=['current_column_name','upper_schema','upper_table']
        with self.engine.connect() as conn:
            result = conn.execute(sql,self._get_default_parameter_stmt())
            
            df_types=pd.DataFrame([[getattr(row,col) for col in sql_col] for row in result],columns=sql_col)
            df_types=df_types.set_index('current_column_name')
            df_types = df_types.drop_duplicates()
            return df_types
        
    def get_foreign_tables(self)->dict[str,Self]:
        dd=self._get_foreign_tables_list().reset_index().to_dict('records')
        ret = {val['current_column_name']:
               TableStructure(val['upper_schema'],val['upper_table'],self.engine) 
               for val in dd}
        return ret
    
    def check_selfref_table(self,ts:Self)->bool:
        match (ts.schema_name,ts.table_name):
            case (self.schema_name,self.table_name):
                return True
            case _:
                return False
        
    def check_if_not_local_column(self,column:str)->bool:
        if '.' not in column:
            return False
        current_column = column.split(".")[0]

        if current_column in self.get_foreign_tables():
            return True
        else:
            return False

    def get_default_value(self):
        sql = f'''SELECT column_name, column_default
        FROM information_schema.columns
        WHERE table_schema = '{self.schema_name}'
        AND table_name = '{self.table_name}'
        AND column_default IS NOT NULL ;
        '''
        return self._execute_sql_read_legacy(sql).set_index('column_name')
    
    def _get_default_parameter_stmt(self):
        return {"schema":self.schema_name,"table":self.table_name}
    
    def __init__(self,schema_name:str,table_name:str,
                 engine:sqlalchemy.Engine):
        self.schema_name = schema_name
        self.table_name = table_name
        self.engine = engine

        with self.engine.connect() as conn:
            result = conn.execute(stmt_find_identity,self._get_default_parameter_stmt())
            self.column_identity = [row.identity_column for row in result]
        #self.column_identity = self.execute_sql_read(sql_find_identity)['identity_column'].to_list()
    def refresh_identity(self):
        warn('refresh_identity of TableStructure will be deprecated. Use column_identity instead.',DeprecationWarning,stacklevel=2)
        return self.column_identity

    def _execute_sql_read_legacy(self,sql,index_column:str|None=None,drop_duplicates:bool=False)->pd.DataFrame:
        warn('_execute_sql_read_legacy method will be deprecated ')
        with self.engine.connect() as conn:
            ret = pd.read_sql_query(sql=sql,con=conn)

            if drop_duplicates:
                ret = ret.drop_duplicates()

            if index_column:
                return ret.set_index(index_column)
            else:
                try:
                    return ret.set_index(self.column_identity)
                except:
                    return ret
                
    def execute_sql_write(self,sql):
        with self.engine.connect() as conn:
            conn.execute(sql)
            conn.commit()
            
        
        return self.read()
    
    def append_column(self,**type_dict):
        for rc in _reserved_columns:
            if rc in type_dict:
                raise ValueError(f'{rc} is reserved.')
        
        qlines = [" ".join(['ADD COLUMN']+_ret_a_line(key,type_dict[key])) for key in type_dict]
        query = text(f'''ALTER TABLE {self.schema_name}.{self.table_name} {','.join(qlines)};''')
        return self.execute_sql_write(query)

    def _read_process(self,ascending=False,columns:list[str]|None=None,remove_original_id=False):
        with self.engine.connect() as conn:
            result = conn.execute(stmt_get_types,self._get_default_parameter_stmt())
            
            df_types=pd.DataFrame([[getattr(row,col) for col in stmt_get_types_col] for row in result],columns=stmt_get_types_col)
            df_types=df_types.set_index('column_name')
            
        yield df_types.copy(), 'get types'

        sql_content = text(f"SELECT * FROM {self.schema_name}.{self.table_name}")
        df_content = self._execute_sql_read_legacy(sql_content)
        column_identity = df_content.index.name

        conv_type = {column_name:_convert_pgsql_type_to_pandas_type(df_types['data_type'][column_name]) for column_name 
                     in df_types.index}
        df_content = df_content.reset_index()
        df_content = df_content.astype(conv_type)
        df_content = df_content.set_index(column_identity)

        df_rwof = df_content.sort_index(ascending=ascending)
        yield df_rwof.copy(), 'read without foreign'

        foreign_tables_ts = self.get_foreign_tables()
        for foreign_col in foreign_tables_ts:
            if not self.check_selfref_table(foreign_tables_ts[foreign_col]):
                ts = foreign_tables_ts[foreign_col]
                df_ftable_types=ts.get_types_expanded()
                row_changer={row:f'{foreign_col}.{row}' for row in df_ftable_types.index.to_list()}
                df_ftable_types=df_ftable_types.rename(index=row_changer)
                df_types = pd.concat([df_types,df_ftable_types])
                if remove_original_id:
                    df_types = df_types.drop(index=foreign_col)

                df_ftable=ts.read_expand(ascending=ascending)
                column_changer={col:f'{foreign_col}.{col}' for col in df_ftable.columns.to_list()}
                df_ftable=df_ftable.rename(columns=column_changer)
                df_content = pd.merge(df_content,df_ftable,'left',left_on=foreign_col,right_index=True)
                if remove_original_id:
                    del df_content[foreign_col]
            else:
                if df_content[foreign_col].isnull().all():
                    continue
                else:
                    try:
                        tos=df_content.replace({np.nan: None}).index.to_list()
                        froms=df_content[foreign_col].replace({np.nan: None}).to_list()
                        exclude_none = [(fr,to) for fr,to in zip(froms,tos)
                                        if (fr is not None) and( to is not None)]

                        gr=nx.DiGraph(exclude_none)
                        nx.find_cycle(gr)
                    except nx.NetworkXNoCycle as noc:
                        df_content_original = df_content.copy()

                        current_selfref=foreign_col
                        while not df_content[current_selfref].isnull().all():
                            renamer = {f'{col}__selfpost':f'{current_selfref}.{col}' for col in df_content.columns}
                            df_content = pd.merge(df_content,df_content_original,'left',
                                                left_on=current_selfref,right_index=True,
                                                suffixes=('','__selfpost'))
                            df_content =df_content.rename(columns=renamer)

                            current_selfref=f'{current_selfref}.{foreign_col}'
                        else:
                            del df_content[current_selfref]
                    

                    

        yield df_types.copy(), 'get types with foreign'

        df_rwf = df_content.sort_index(ascending=ascending)
        yield df_rwf.copy(), 'read with foreign'

        df_address=df_rwof.copy()
        col_sub = {col:col[:col.find('.')] for col in df_rwf.columns if col.find('.')!=-1}
        for col in col_sub:
            df_address[col] = df_address[col_sub[col]]
        yield df_address.copy(), 'addresses'

    def read(self,ascending=False,columns:list[str]|None=None)->pd.DataFrame:
        df_res = bp.select_yielder(self._read_process(ascending,columns),
                                 'read without foreign') 
        if columns is not None:
            df_res = df_res[columns]
        return df_res.copy()
    
    def read_expand(self,ascending=False,remove_original_id=False,columns:list[str]|None=None)->pd.DataFrame:
        df_res = bp.select_yielder(self._read_process(ascending,remove_original_id=remove_original_id,columns=columns),
                                 'read with foreign')
        if columns is not None:
            df_res = df_res[columns]
        return df_res.copy()
        
    def get_types(self)->pd.DataFrame:
        return bp.select_yielder(self._read_process(),'get types')
    
    def get_types_expanded(self)->pd.DataFrame:
        return bp.select_yielder(self._read_process(),'get types with foreign')
    
    def get_local_val_to_id(self,column:str):
        convert_table:pd.DataFrame = bp.select_yielder(self._read_process(),'read without foreign')
        ser_filtered = convert_table[column].dropna()
        ser_filtered.index = ser_filtered.index.astype('int')
        ret = ser_filtered.to_dict()
        ret = {ret[key]:key for key in ret}
        return ret
        
    def _get_local_foreign_id(self,row,column)->int:
        '''
        get a local foreign id of expanded dataframe.
        
        Parameters
        --------
        row
            a row of a local table.
        column : str
            a foreign column of a local table such as 'fruit_id.color'

        Returns
        --------
        int
            foreign id.
        
        '''
        df = bp.select_yielder(self._read_process(),'addresses')

        return df.loc[row,column]

    def upload(self,id_row:int,**kwarg):
        cp = kwarg.copy()
        for column in kwarg:
            if self.check_if_not_local_column(column):
                local_column=column.split(".")[0]
                foreign_column=".".join(column.split(".")[1:])
                foreign_val = kwarg[column]
                foreign_upload_dict = {foreign_column:foreign_val}

                local_foreign_id = self._get_local_foreign_id(id_row,column)
                foreign_ts=self.get_foreign_tables()[local_column]

                if local_foreign_id is pd.NA:
                    foreign_index = set(foreign_ts.read().index.to_list())
                    df_foreign_after =foreign_ts.upload_append(**foreign_upload_dict)

                    def get_foreign_returned():
                        foreign_index_after = set(df_foreign_after.index.to_list())
                        foreign_index_diff = foreign_index_after - foreign_index
                        foreign_index_list_diff = [v for v in foreign_index_diff]
                        if len(foreign_index_list_diff)!=1:
                            raise NotImplementedError("The amount of changed index in foreign is not one.")
                        return foreign_index_list_diff[0]
                    
                    foreign_id = get_foreign_returned()
                    upload_local = {local_column:foreign_id}
                    self.upload(id_row,**upload_local)
                else:
                    foreign_upload_dict = {foreign_column:foreign_val}
                    foreign_ts.upload(local_foreign_id,**foreign_upload_dict)

                del cp[column]
        
        if len(cp)<1:
            return self.read()

        original=",".join(["=".join([key,f"{_conversion_Sql_value(cp[key])}"]) for key in cp])
        
        sql = text(f"""
        UPDATE {self.schema_name}.{self.table_name}
        SET {original}
        WHERE {self.column_identity[0]} = {id_row};
        """)
        
        return self.execute_sql_write(sql)
    
    def upload_append(self,**kwarg):
        cp = kwarg.copy()
        col_deletion = []
        for column in kwarg:
            if self.check_if_not_local_column(column):
                col_deletion.append(column)
            else:
                match cp[column]:
                    case pd.NaT:
                        col_deletion.append(column)
                    case None:
                        col_deletion.append(column)
        for column in col_deletion:
            del cp[column]
        

        columns = ','.join([f'"{key}"' for key in cp])
        values = ','.join([_conversion_Sql_value(cp[key]) for key in cp])
        sql = text(f"""
        INSERT INTO {self.schema_name}.{self.table_name} ({columns})
        VALUES ({values})
        """)
        
        return self.execute_sql_write(sql)
    def __repr__(self):
        return f"Table_structure. \nSchema is {self.schema_name}\nTable is {self.table_name}"


    
def get_table_list(engine:sqlalchemy.Engine):
    '''
    Get table list in a database.
    ## Parameters:
    engine : sqlalchemy.Engine
    a engine.
    ## See Also:
    
    ## Examples:
    import streamlit as st
    eng = st.connection(name='postgresql',type='sql').engine

    df_list=get_table_list(eng)
    '''


    sql = f'''SELECT DISTINCT table_schema,table_name
    FROM information_schema.table_constraints;
    '''
    with engine.connect() as con_con:
        ret = pd.read_sql_query(sql,con=con_con)
    
        return ret


class SchemaStructure:
    schema_name : str
    engine : sqlalchemy.Engine

    def __init__(self,schema_name:str,
                 engine:sqlalchemy.Engine):
        self.schema_name = schema_name
        self.engine = engine

                
    def execute_sql_write(self,sql):
        with self.engine.connect() as conn:
            conn.execute(sql)
            conn.commit()

    def create_table(self,table_name:str,**type_dict):
        '''
        create a table
        
        Parameters
        ----------
        table_name : str
            New table.
        type_dict : dict
            {column_name : column_type}.
        '''
        for rc in _reserved_columns:
            if rc in type_dict:
                raise ValueError(f'{rc} is reserved.')
        
        qlines = [" ".join(_ret_a_line(key,type_dict[key])) for key in type_dict]
        qlines.insert(0,"id bigint NOT NULL GENERATED ALWAYS AS IDENTITY")
        qlines.append("PRIMARY KEY (id)")
        query=text(f'''CREATE TABLE {self.schema_name}.{table_name} (
            {','.join(qlines)}
        );''')
        
        self.execute_sql_write(query)

        ts = TableStructure(self.schema_name,table_name,self.engine)
        return ts

def _execute_globally(engine:sqlalchemy.Engine,sql:str):
    with engine.connect() as conn:
        conn.execute(sql)
        conn.commit()


def create_domain(engine:sqlalchemy.Engine,type_name:str,base_type:str):
    sql = text(f'''CREATE DOMAIN "{type_name}" AS {base_type};''')
    _execute_globally(engine,sql)

def create_schema(engine:sqlalchemy.Engine,schema_name:str)->SchemaStructure:
    sql = text(f'''CREATE SCHEMA "{schema_name}" ''')
    _execute_globally(engine,sql)
    return SchemaStructure(schema_name,engine)

class SQLALchemyPlus:
    engine : sqlalchemy.Engine
    tables : list[TableStructure]

    def __init__(self,engine:sqlalchemy.Engine):
        self.engine = engine
    
    def add_tables(self,schema_name:str,table_name:str):
        self.tables.append(TableStructure(schema_name,table_name))