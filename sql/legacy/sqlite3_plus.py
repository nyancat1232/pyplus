import sqlite3
from warnings import warn

def _db_query_exec(sql:str,db:str='out.db',show:bool=False):
    warn('_db_query_exec() will be deprecated',category=DeprecationWarning)
    con = sqlite3.connect(db)
    cur = con.cursor()

    rr = cur.execute(sql)
    if show:
        for r in rr:
            print(r)
    con.commit()
    con.close()

def create(table,dbfilename='out.db',**columns):
    warn('create() will be deprecated',category=DeprecationWarning)
    sql = f"CREATE TABLE IF NOT EXISTS {table}({','.join(' '.join([key,val]) for key,val in columns.items())});"
    
    _db_query_exec(sql,dbfilename)

def read(table,*columns):
    warn('read() will be deprecated',category=DeprecationWarning)
    sql = f"SELECT {','.join(columns)} FROM {table}"
    return sql
    _db_query_exec(sql,True)

def update(table,**adds):
    warn('update() will be deprecated',category=DeprecationWarning)
    keys = ['"'+c+'"' for c in adds.keys()]
    values = ['"'+c+'"' for c in adds.values()]
    sql = f"INSERT INTO {table} ({','.join(keys)}) VALUES({','.join(values)})"
    print(sql)
    _db_query_exec(sql)

def delete(table):
    warn('delete() will be deprecated',category=DeprecationWarning)
    sql = f'''DROP TABLE {table}
    '''
    _db_query_exec(sql)