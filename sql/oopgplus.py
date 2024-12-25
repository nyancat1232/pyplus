import pandas as pd
from sqlalchemy.sql import text
import sqlalchemy
from typing import Literal,Self,Any
from datetime import date,tzinfo
from zoneinfo import ZoneInfo
import numpy as np
import checkpoint as chpo
import networkx as nx
from warnings import warn

stmt_find_identity = text(f'''
SELECT attname as identity_column
  FROM pg_attribute 
       JOIN pg_class 
            ON pg_attribute.attrelid = pg_class.oid
       JOIN pg_namespace
            ON pg_class.relnamespace = pg_namespace.oid
 WHERE nspname = :schema
       AND relname = :table
       AND attidentity = 'a';
''')

stmt_foreign_col=['current_column_name','upper_schema','upper_table']
stmt_foreign = text(f'''
SELECT KCU.column_name AS {stmt_foreign_col[0]},
       CCU.table_schema AS {stmt_foreign_col[1]}, 
       CCU.table_name AS {stmt_foreign_col[2]}
  FROM information_schema.key_column_usage AS KCU
       JOIN information_schema.constraint_column_usage AS CCU 
            ON KCU.constraint_name = CCU.constraint_name
       JOIN information_schema.table_constraints AS TC 
            ON KCU.constraint_name = TC.constraint_name
 WHERE TC.constraint_type = 'FOREIGN KEY'
       AND KCU.table_schema=:schema
       AND KCU.table_name=:table;
''')

stmt_default_col=['column_name','column_default']
stmt_default = text(f'''
SELECT {stmt_default_col[0]},
       {stmt_default_col[1]}
  FROM information_schema.columns
 WHERE table_schema = :schema
       AND table_name = :table
       AND column_default IS NOT NULL ;
''')

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
                if len(val.strftime('%z'))==0:
                    return f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'"
                else:
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


_reserved_columns = ['id']

class TableStructure:
    '''
    TableStructure is a class that easily operate Create, Read, Update databases especially a table with foreign columns.
    '''
    schema_name : str
    table_name : str
    engine : sqlalchemy.Engine

    def _get_default_parameter_stmt(self):
        return {"schema":self.schema_name,"table":self.table_name}

    def __init__(self,schema_name:str,table_name:str,
                 engine:sqlalchemy.Engine):
        self.schema_name = schema_name
        self.table_name = table_name
        self.engine = engine

    def __repr__(self):
        ret = f'{self.schema_name}.{self.table_name}'
        ret += repr(chpo.CheckPointFunction(self._iter_read).read_with_foreign())
        return ret

    #Creation
    def append_column(self,**type_dict):
        for rc in _reserved_columns:
            if rc in type_dict:
                raise ValueError(f'{rc} is reserved.')
        
        with self.engine.connect() as conn:
            for key in type_dict:
                query = text(f'ALTER TABLE IF EXISTS {self.schema_name}.{self.table_name} ADD COLUMN "{key}" {type_dict[key]};')
                conn.execute(query)
            conn.commit()
            
    
    #Read
    def _execute_to_pandas(self,stmt,stmt_columns):
        with self.engine.connect() as conn:
            result = conn.execute(stmt,self._get_default_parameter_stmt())
            
            df_types=pd.DataFrame([[getattr(row,col) for col in stmt_columns] for row in result],columns=stmt_columns)
            return df_types
        
    def _iter_foreign_tables(self):
        df_types = self._execute_to_pandas(stmt_foreign,stmt_foreign_col)
        df_types = df_types.drop_duplicates()
        df_types=df_types.set_index('current_column_name')
        yield df_types.copy(), 'get_foreign_tables_list'

        dd=df_types.reset_index().to_dict('records')
        ret = {val['current_column_name']:
               TableStructure(val['upper_schema'],val['upper_table'],self.engine) 
               for val in dd}
        yield ret.copy(), 'get_foreign_tables'
    def get_foreign_tables(self)->dict[str,Self]:
        return chpo.CheckPointFunction(self._iter_foreign_tables).get_foreign_tables()
    
    def check_if_not_local_column(self,column:str)->bool:
        if '.' not in column:
            return False
        current_column = column.split(".")[0]

        if current_column in chpo.CheckPointFunction(self._iter_foreign_tables).get_foreign_tables():
            return True
        else:
            return False
    
    def _iter_read(self,ascending=False,columns:list[str]|None=None,remove_original_id=False):
        with self.engine.connect() as conn:
            result = conn.execute(stmt_find_identity,self._get_default_parameter_stmt())
            column_identity = [row.identity_column for row in result]
        yield column_identity, 'get_identity'

        stmt_get_types = text(f'''
        SELECT column_name,
            column_default,
            data_type,
            CASE 
                WHEN domain_name IS NOT NULL THEN domain_name
                ELSE data_type
            END AS display_type,
            is_generated
        FROM information_schema.columns
        WHERE table_schema = '{self.schema_name}'
            AND table_name = '{self.table_name}';
        ''')
        with self.engine.connect() as conn:
            df_types = pd.read_sql_query(sql=stmt_get_types,con=conn)
        df_types=df_types.set_index('column_name')
        yield df_types.copy(), 'get_types'

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
                    return f'datetime64[{precision}]'
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

        conv_type = {column_name:_convert_pgsql_type_to_pandas_type(df_types['data_type'][column_name]) for column_name 
                     in df_types.index}

        sql_content = text(f'SELECT * FROM {self.schema_name}.{self.table_name}')
        with self.engine.connect() as conn:
            df_content = pd.read_sql_query(sql=sql_content,con=conn,dtype=conv_type,index_col=column_identity)

        yield df_content.copy(), 'read_without_foreign'
        
        def check_selfref_table(ts:Self)->bool:
            match (ts.schema_name,ts.table_name):
                case (self.schema_name,self.table_name):
                    return True
                case _:
                    return False
        
        foreign_tables_ts =chpo.CheckPointFunction(self._iter_foreign_tables).get_foreign_tables() 
        for col_local_foreign in foreign_tables_ts:
            if not check_selfref_table(foreign_tables_ts[col_local_foreign]):
                ts = foreign_tables_ts[col_local_foreign]
                df_ftable_types=ts.get_types_expanded()
                row_changer={row:f'{col_local_foreign}.{row}' for row in df_ftable_types.index.to_list()}
                df_ftable_types=df_ftable_types.rename(index=row_changer)
                df_types = pd.concat([df_types,df_ftable_types])
                if remove_original_id:
                    df_types = df_types.drop(index=col_local_foreign)

                df_ftable=ts.read_expand(ascending=ascending)
                column_changer={col:f'{col_local_foreign}.{col}' for col in df_ftable.columns.to_list()}
                df_ftable=df_ftable.rename(columns=column_changer)
                df_content = pd.merge(df_content,df_ftable,'left',left_on=col_local_foreign,right_index=True)
                if remove_original_id:
                    del df_content[col_local_foreign]
            else:
                if df_content[col_local_foreign].isnull().all():
                    continue
                else:
                    try:
                        tos=df_content.replace({np.nan: None}).index.to_list()
                        froms=df_content[col_local_foreign].replace({np.nan: None}).to_list()
                        exclude_none = [(fr,to) for fr,to in zip(froms,tos)
                                        if (fr is not None) and( to is not None)]

                        gr=nx.DiGraph(exclude_none)
                        nx.find_cycle(gr)
                    except nx.NetworkXNoCycle as noc:
                        df_content_original = df_content.copy()

                        current_selfref=col_local_foreign
                        while not df_content[current_selfref].isnull().all():
                            renamer = {f'{col}__selfpost':f'{current_selfref}.{col}' for col in df_content.columns}
                            df_content = pd.merge(df_content,df_content_original,'left',
                                                left_on=current_selfref,right_index=True,
                                                suffixes=('','__selfpost'))
                            df_content =df_content.rename(columns=renamer)

                            current_selfref=f'{current_selfref}.{col_local_foreign}'
                        else:
                            del df_content[current_selfref]
        yield df_types.copy(), 'get_types_with_foreign'

        df_rwf = df_content.sort_index(ascending=ascending)
        yield df_rwf.copy(), 'read_with_foreign'

        df_address=df_content.copy()
        col_sub = {col:col[:col.find('.')] for col in df_rwf.columns if col.find('.')!=-1}
        for col in col_sub:
            df_address[col] = df_address[col_sub[col]]
        yield df_address.copy(), 'addresses'
    def get_identity(self):
        return chpo.CheckPointFunction(self._iter_read).get_identity() 
        
    def get_default_value(self):
        df_ret_new:pd.DataFrame = chpo.CheckPointFunction(self._iter_read).get_types()
        df_ret_new = df_ret_new.dropna(subset='column_default')
        ser_ret_new = df_ret_new['column_default']
        return ser_ret_new        
    def get_types(self)->pd.DataFrame:
        return chpo.CheckPointFunction(self._iter_read).get_types()
    def read(self,ascending=False,columns:list[str]|None=None)->pd.DataFrame:
        df_content = chpo.CheckPointFunction(self._iter_read).read_without_foreign()
        df_rwof = df_content.sort_index(ascending=ascending)
        df_res = df_rwof
        if columns is not None:
            df_res = df_res[columns]
        return df_res.copy()

    def read_expand(self,ascending=False,remove_original_id=False,columns:list[str]|None=None)->pd.DataFrame:
        df_res = chpo.CheckPointFunction(self._iter_read)(ascending,remove_original_id=remove_original_id,columns=columns).read_with_foreign()
        if columns is not None:
            df_res = df_res[columns]
        return df_res.copy()
    def __getitem__(self, item)->pd.DataFrame:
        return self.read_expand()[item]

    def get_types_expanded(self)->pd.DataFrame:
        return chpo.CheckPointFunction(self._iter_read).get_types_with_foreign()
    
    def get_local_val_to_id(self,column:str):
        convert_table:pd.DataFrame = chpo.CheckPointFunction(self._iter_read).read_without_foreign()
        ser_filtered = convert_table[column].dropna()
        ser_filtered.index = ser_filtered.index.astype('Int64')
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
        df = chpo.CheckPointFunction(self._iter_read).addresses()

        return df.loc[row,column]
    
    #upload
    def change_column_name(self,**kwarg):
        cp = kwarg.copy()
        with self.engine.connect() as conn:
            for name in cp:
                sql =text( f'ALTER TABLE IF EXISTS {self.schema_name}.{self.table_name} RENAME {name} TO {cp[name]};')
                conn.execute(sql)
            conn.commit()
        return self.read()

    def upload(self,id_row:int,**kwarg):
        column_identity = chpo.CheckPointFunction(self._iter_read).get_identity()
        cp = kwarg.copy()
        for column in kwarg:
            if self.check_if_not_local_column(column):
                local_column=column.split(".")[0]
                foreign_column=".".join(column.split(".")[1:])
                foreign_val = kwarg[column]
                foreign_upload_dict = {foreign_column:foreign_val}

                local_foreign_id = self._get_local_foreign_id(id_row,column)
                foreign_ts=chpo.CheckPointFunction(self._iter_foreign_tables).get_foreign_tables()[local_column]

                if local_foreign_id is pd.NA:
                    #Add when local column of the row has no foreign columns.
                    foreign_index = set(foreign_ts.read().index.to_list())
                    df_foreign_after =foreign_ts.append(**foreign_upload_dict)
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

        original=",".join([f'"{key}" = {_conversion_Sql_value(cp[key])}' for key in cp])
        
        sql = text(f'''
        UPDATE {self.schema_name}.{self.table_name}
        SET {original}
        WHERE {column_identity[0]} = {id_row};
        ''')
        
        with self.engine.connect() as conn:
            conn.execute(sql)
            conn.commit()
            
        
        return self.read()

    def upload_dataframe(self,df:pd.DataFrame):
        dict_df = df.to_dict('index')
        for row in dict_df:
            self.upload(row,**dict_df[row])

    def upload_appends(self,*row:dict[str,Any]):
        '''
        Append rows
        
        Parameters
        ----------
        row : dict[str,Any]
            A row such as {'column1':'value1','column2':'value2',...}.
        
        See Also
        --------
        upload
        
        Returns
        --------
        pd.DataFrame
            A dataframe without expanding foreign ids.
        '''
        def process_each_row(**kwarg):
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
            return cp
        
        processed_rows = [process_each_row(**row) for row in row]
        
        with self.engine.connect() as conn:
            for row in processed_rows:   
                if len(row) == 0:
                    print('No values has been added.')
                    continue

                columns = ','.join([f'"{col}"' for col in row])
                values = ','.join([_conversion_Sql_value(row[col]) for col in row])
                stmt = text(f'''
                INSERT INTO {self.schema_name}.{self.table_name} ({columns})
                VALUES ({values})
                ''')
                conn.execute(stmt)
            conn.commit()

        return self.read()

    def append(self,**kwarg:Any):
        return self.upload_appends(kwarg)
    
    def connect_foreign_column(self,ts_foreign:Self,local_col:str):
        stmt=text(f'''
        ALTER TABLE IF EXISTS {self.schema_name}.{self.table_name}
        ADD FOREIGN KEY ({local_col})
        REFERENCES {ts_foreign.schema_name}.{ts_foreign.table_name} ({ts_foreign.get_identity()[0]}) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;
                  ''')
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
    
    #delete
    def delete_column(self, column:str):
        stmt=text(f'''ALTER TABLE IF EXISTS {self.schema_name}.{self.table_name}
        DROP COLUMN IF EXISTS {column};''')
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()

    def delete_row(self,row:int):
        column_identity = chpo.CheckPointFunction(self._iter_read).get_identity()
        stmt=text(f'''DELETE FROM {self.schema_name}.{self.table_name}
                  WHERE {column_identity[0]}={row};
                  ''')
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()

Table = TableStructure
    
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

        ret = ret[~ret['table_schema'].str.startswith('pg_')]
        return ret

def get_schema_list(engine:sqlalchemy.Engine):
    sql = f'''SELECT DISTINCT table_schema
    FROM information_schema.table_constraints;
    '''
    with engine.connect() as con_con:
        ret = pd.read_sql_query(sql,con=con_con)

        ret = ret[~ret['table_schema'].str.startswith('pg_')]
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
        
        with self.engine.connect() as conn:
            query=text(f'''CREATE TABLE {self.schema_name}.{table_name}
                        (
                         id bigint NOT NULL GENERATED ALWAYS AS IDENTITY,
                         PRIMARY KEY (id)
                        );
            ''')
            conn.execute(query)
            conn.commit()

        ts = TableStructure(self.schema_name,table_name,self.engine)
        ts.append_column(**type_dict)
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