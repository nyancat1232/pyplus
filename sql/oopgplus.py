import pandas as pd
from sqlalchemy.sql import text
from dataclasses import dataclass
import sqlalchemy
from typing import List,Self,Dict

class TableStructure:
    schema_name : str
    table_name : str
    engine : sqlalchemy.Engine
    parent_table : Self
    generation : int

    _identity_column : str
    
    def get_foreign_table(self):
        sql = f'''
        SELECT KCU.column_name AS current_column_name,
            CCU.table_schema AS upper_schema, 
            CCU.table_name AS upper_table,
            CCU.column_name AS  upper_column_name
        FROM information_schema.key_column_usage AS KCU
        JOIN information_schema.constraint_column_usage AS CCU ON KCU.constraint_name = CCU.constraint_name
        JOIN information_schema.table_constraints AS TC ON KCU.constraint_name = TC.constraint_name
        WHERE TC.constraint_type = 'FOREIGN KEY'
        AND KCU.table_schema='{self.schema_name}'
        AND KCU.table_name='{self.table_name}';
        '''
        return self.execute_sql(sql,index_column='current_column_name',drop_duplicates=True)
    
    def detect_child_tables(self):
        child_tables=[]
        
        df_foreign_keys = self.get_foreign_table()
        
        for foreign_key_index,foreign_key_series in df_foreign_keys.iterrows():
            current_foreign_schema =  foreign_key_series['upper_schema']
            current_foreign_table =  foreign_key_series['upper_table']
            child_tables.append(TableStructure(schema_name=current_foreign_schema,
                                                    table_name=current_foreign_table,
                                                    engine=self.engine,
                                                    parent_table=self,
                                                    generation=self.generation+1))
        return child_tables
    
    def get_identity(self):
        sql = f'''SELECT attname as identity_column
        FROM pg_attribute 
        JOIN pg_class 
            ON pg_attribute.attrelid = pg_class.oid
        JOIN pg_namespace
            ON pg_class.relnamespace = pg_namespace.oid
        WHERE nspname = '{self.schema_name}'
        AND relname = '{self.table_name}'
        AND attidentity = 'a';
        '''
        self._identity_column = self.execute_sql(sql)['identity_column'].to_list()
        return self._identity_column

    def __init__(self,schema_name:str,table_name:str,engine:sqlalchemy.Engine,parent_table:Self=None,generation:int=0):
        self.schema_name = schema_name
        self.table_name = table_name
        self.engine = engine
        if parent_table:
            self.parent_table = parent_table
        self.generation = generation
        self._identity_column = self.get_identity()


    def execute_sql(self,sql,index_column=None,drop_duplicates=False)->pd.DataFrame:
        with self.engine.connect() as conn:
            ret = pd.read_sql_query(sql=sql,con=conn)

            if drop_duplicates:
                ret = ret.drop_duplicates()

            if index_column:
                return ret.set_index(index_column)
            else:
                try:
                    return ret.set_index(self._identity_column)
                except:
                    return ret
            
    def get_all_children(self):
        children = self.detect_child_tables()
        if len(children)>0:
            rr = [self]
            for ts_child in children:
                rr.extend(ts_child.get_all_children())
            rr.sort(key=lambda l:l.generation,reverse=False)

            return rr
        else:
            return [self]
            

    def read(self):
        sql = f'''SELECT * FROM {self.schema_name}.{self.table_name}
        '''
        return self.execute_sql(sql)
    


class SQLALchemyPlus:
    engine : sqlalchemy.Engine
    tables : List[TableStructure]

    def __init__(self,engine:sqlalchemy.Engine):
        self.engine = engine
    
    def add_tables(self,schema_name:str,table_name:str):
        self.tables.append(TableStructure(schema_name,table_name))