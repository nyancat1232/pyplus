import pandas as pd
from sqlalchemy.sql import text
from warnings import warn
def get_identity(schema_name:str,table_name:str,st_conn):
    warn('get_identity() will be deprecated',category=DeprecationWarning)
    sql = f'''SELECT attname as identity_column
  FROM pg_attribute 
  JOIN pg_class 
    ON pg_attribute.attrelid = pg_class.oid
  JOIN pg_namespace
    ON pg_class.relnamespace = pg_namespace.oid
 WHERE nspname = '{schema_name}'
   AND relname = '{table_name}'
   AND attidentity = 'a';
'''
    
    with st_conn.connect() as con_con:
        ret = pd.read_sql_query(sql,con=con_con)
    
        return ret['identity_column']

def read_from_server(schema_name:str,table_name:str,st_conn):
    warn('read_from_server() will be deprecated',category=DeprecationWarning)
    '''
    read a table form schema_name.table_name by using SQL query.
    ## Parameters:
    schema_name : str
    a schema name for finding a table.
    table_name : str
    a table name for finding a table.
    st_conn : SQLconnection
    a connection of streamlit.
    ## See Also:
    expand_foreign_column
    ## Examples:
    >>>> import streamlit as st
    >>>> from pyplus.sql.pgplus import expand_foreign_column
    >>>> 
    >>>> conn=st.connection('postgresql',type='sql')
    >>>> df_pivot_list = read_from_server(schema_name='study',table_name='event',st_conn=conn)
    >>>> df_pivot_list
    '''
    with st_conn.connect() as conn_conn:
        result = pd.read_sql_table(table_name=table_name,con=conn_conn,schema=schema_name)
        identity = get_identity(schema_name=schema_name,table_name=table_name,st_conn=st_conn)

        result = result.set_index(identity.to_list())
        result = result.sort_index(ascending=False)
        return result

def expand_foreign_column(schema_name:str,table_name:str,st_conn):
    warn('expand_foreign_column() will be deprecated',category=DeprecationWarning)
    '''
    function of read_from_server as expanded column of foreign key
    ## Parameters:
    schema_name : str
    a schema name for finding a table.
    table_name : str
    a table name for finding a table.
    st_conn : SQLconnection
    a connection of streamlit.
    ## See Also:
    read_from_server
    ## Examples:
    >>>> import streamlit as st
    >>>> from pyplus.sql.pgplus import expand_foreign_column
    >>>> 
    >>>> conn=st.connection('postgresql',type='sql')
    >>>> df_pivot_list = expand_foreign_column(schema_name='study',table_name='event',st_conn=conn)
    >>>> df_pivot_list
    '''
    df_result=read_from_server(schema_name=schema_name,table_name=table_name,st_conn=st_conn)
    df_result=df_result.reset_index(drop=True)
    
    fks=get_foreign_keys(schema_name=schema_name,table_name=table_name,st_conn=st_conn)

    for foreign_key_index,foreign_key_series in fks.iterrows():
        df_right=read_from_server(foreign_key_series['upper_schema'],foreign_key_series['upper_table'],st_conn).reset_index()

        #df_right[foreign_key_series['upper_column_name']] = df_right[foreign_key_series['upper_column_name']].astype('object')
        current_foreign_schema =  foreign_key_series['upper_schema']
        current_foreign_table =  foreign_key_series['upper_table']
        current_foreign_connect_column = foreign_key_series['upper_column_name']
        column_name_before_foreign = foreign_key_index
        
        replace_right_column_to = f"{column_name_before_foreign}."+df_right.columns

        df_right = df_right.rename(columns={fr:to for fr,to in zip(df_right.columns,replace_right_column_to) if fr != current_foreign_connect_column})

        try:
            df_result=pd.merge(left=df_result,right=df_right,left_on=df_result[foreign_key_index],right_on=df_right[current_foreign_connect_column],how='left')
            df_result=df_result.drop(columns=['key_0',foreign_key_index,current_foreign_connect_column])
        except:
            'no rows in foreign keys'

    return df_result

def get_foreign_id_table(to_column:str,schema_name:str,table_name:str,st_conn):
    warn('get_foreign_id_table() will be deprecated',category=DeprecationWarning)
    '''
    ## Examples:
    >> id foreign_id
    >> 1 2
    >> 2 3

    >> foreign_id name age
    >> 1 a 1
    >> 2 b 11
    >> 3 c 111

    result
    >> {
        b:2,
        c:3,
    }
    '''
    fks=get_foreign_keys(schema_name=schema_name,table_name=table_name,st_conn=st_conn)
    for _,foreign_key_series in fks.iterrows():
        df_right=read_from_server(foreign_key_series['upper_schema'],foreign_key_series['upper_table'],st_conn).reset_index()
        df_result = df_right[[foreign_key_series['upper_column_name'],to_column]]

    df_result = {v: k for k, v in zip(df_result[foreign_key_series['upper_column_name']],df_result[to_column])}
    return df_result

def get_columns(schema_name:str,table_name:str,st_conn):
    warn('get_columns() will be deprecated',category=DeprecationWarning)
    sql = f'''SELECt column_name,data_type
  FROM information_schema.columns
 WHERE table_schema='{schema_name}'
   AND table_name='{table_name}';
    '''
    with st_conn.connect() as con_con:
        ret = pd.read_sql_query(sql,con=con_con)
    
        return ret.set_index('column_name')

def get_foreign_keys(schema_name:str,table_name:str,st_conn):
    warn('get_foreign_keys() will be deprecated',category=DeprecationWarning)
    '''
    get foreign key of a table
    ## Parameters:
    schema_name : str
    a schema name for finding a table.
    table_name : str
    a table name for finding a table.
    st_conn : SQLconnection
    a connection of streamlit.
    ## See Also:
    (sa_description)
    ## Examples:
    import streamlit as st
    from pyplus.sql.pgplus import get_foreign_keys

    conn = st.connection(name='postgresql',type='sql')

    df_fks = get_foreign_keys(schema_name='<<your schema>>',table_name='<<your table>>',st_conn=conn)
    df_fks
    '''
    foreign_key_sql = f'''
    SELECT KCU.column_name AS current_column_name,
        CCU.table_schema AS upper_schema, 
        CCU.table_name AS upper_table,
        CCU.column_name AS  upper_column_name
    FROM information_schema.key_column_usage AS KCU
    JOIN information_schema.constraint_column_usage AS CCU ON KCU.constraint_name = CCU.constraint_name
    JOIN information_schema.table_constraints AS TC ON KCU.constraint_name = TC.constraint_name
    WHERE TC.constraint_type = 'FOREIGN KEY'
    AND KCU.table_schema='{schema_name}'
    AND KCU.table_name='{table_name}';
    '''

    with st_conn.connect() as con_con:
        ret = pd.read_sql_query(foreign_key_sql,con=con_con).drop_duplicates()
    
        return ret.set_index('current_column_name')
    
def get_table_list(st_conn):
    warn('get_table_list() will be deprecated',category=DeprecationWarning)
    '''
    Get table list in a database.
    ## Parameters:
    st_conn : SQLconnection
    a connection of streamlit.
    ## See Also:
    
    ## Examples:
    import streamlit as st
    st_conn = st.connection(name='postgresql',type='sql')

    df_list=get_table_list(st_conn)
    '''
    sql = f'''SELECT DISTINCT table_schema,table_name
    FROM information_schema.table_constraints;
    '''
    with st_conn.connect() as con_con:
        ret = pd.read_sql_query(sql,con=con_con)
    
        return ret
    
def get_default_value(schema_name:str,table_name:str,st_conn):
    warn('get_default_value() will be deprecated',category=DeprecationWarning)
    sql = f'''SELECT column_name, column_default
  FROM information_schema.columns
 WHERE table_schema = '{schema_name}'
   AND table_name = '{table_name}'
   AND column_default IS NOT NULL ;
'''
        
    with st_conn.connect() as con_con:
        ret = pd.read_sql_query(sql,con=con_con)
    
        return ret.set_index('column_name')
    
def create_empty_with_id_with_column(columns:dict,schema_name:str,table_name:str,st_conn):
    warn('create_empty_with_id_with_column() will be deprecated',category=DeprecationWarning)
    additional_column=''
    for column in reversed(columns.items()):
        additional_column = ",".join([" ".join(column),additional_column])
    sql = text(f'''CREATE TABLE IF NOT EXISTS {schema_name}.{table_name}
        (
            id bigint NOT NULL GENERATED ALWAYS AS IDENTITY,
            {additional_column}
            PRIMARY KEY (id)
        );
    ''')

    with st_conn.connect() as session:
        session.execute(sql)
        session.commit()

def create_columns(columns:dict,schema_name:str,table_name:str,st_conn):
    warn('create_columns() will be deprecated',category=DeprecationWarning)
    additional_column=''
    for column in reversed(columns.items()):
        if len(columns)>1:
            additional_column = ",".join([" ".join(column),additional_column])
        else:
            additional_column = " ".join(column)


    sql = text(f'''ALTER TABLE  {schema_name}.{table_name}
  ADD COLUMN {additional_column};
    ''')

    with st_conn.connect() as session:
        session.execute(sql)
        session.commit()

      
def write_to_server(df:pd.DataFrame,schema_name:str,table_name:str,st_conn):
    warn('write_to_server() will be deprecated',category=DeprecationWarning)
    '''
    append a dataframe to database by using df.to_sql.
    ## Parameters:
    df : pd.DataFrame
    a dataframe to append.
    ## Examples:
    import streamlit as st
    import pandas as pd

    >public.test
    >  A B
    >  1 2
    >  3 4

    conn = st.connection(name='postgresql',type='sql')
    df = pd.DataFrame({'A':[5,6],'B':[7,8]})

    write_to_server(df,'public','test',conn)


    >public.test
    >  A B
    >  1 2
    >  3 4
    >  5 7
    >  6 8


    '''
    with st_conn.connect() as conn_conn:
        return df.to_sql(name=table_name,con=conn_conn,schema=schema_name,if_exists='append',index=False)

def upload_to_sql_by_value(schema_name,table_name,st_conn,select_column,select_val,edit_column,edit_value):
    warn('upload_to_sql_by_value() will be deprecated',category=DeprecationWarning)
    '''
    Select column and then value for selecting a row. Edit a value in a row.

    ## Examples:
    > aaaa.bbbb
    >    a  b  c
    > 0  1  2  3
    > 1  4  5  6

    conn = st.connection(...)
    upload_to_sql_by_value(aaaa,bbbb,conn,'b',5,'c',2)
    > aaaa.bbbb
    >    a  b  c
    > 0  1  2  3
    > 1  4  5  2
    '''
    update_query = text(f"""
    UPDATE {schema_name}.{table_name}
    SET {edit_column} = {edit_value}
    WHERE {select_column} = '{select_val}';
    """)
    update_query
    with st_conn.session as session:
        session.execute(update_query)
        session.commit()

def upload_to_sql_by_id(sr:pd.Series,schema_name,table_name,st_conn,id_row):
    warn('upload_to_sql_by_id() will be deprecated',category=DeprecationWarning)
    '''
    Upload by id.
    
    ## Examples:
    
    >current aaaa.bbbb in db
    > aaaa.bbbb
    >    a  b  c
    > 0  2  0  0
    > 1  0  0  0

    conn = st.connection(...)

    df = pd.DataFrame(...)
    df
    >    a  b  c
    > 0  1  2  3
    > 1  4  5  6

    upload_to_sql_by_value(df,aaaa,bbbb,conn,0)

    > aaaa.bbbb
    >    a  b  c
    > 0  1  2  3
    > 1  0  0  0
    '''
    id_column = get_identity(schema_name,table_name,st_conn).to_list()[0]

    original=sr.copy().to_dict()
    original=",".join(["=".join([key,f"'{str(original[key])}'"]) for key in original])

    update_query = text(f"""
    UPDATE {schema_name}.{table_name}
    SET {original}
    WHERE {id_column} = {id_row};
    """)
    
    with st_conn.session as session:
        session.execute(update_query)
        session.commit()
