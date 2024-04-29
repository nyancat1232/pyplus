import pandas as pd
from typing import Callable
import pyplus.builtin as bp

def create_new_dataframe_keep_columns(df:pd.DataFrame,index:pd.Series|None = None)->pd.DataFrame:
    return pd.DataFrame(columns=df.columns,index=index).astype(dtype=df.dtypes).copy()

def to_date(sr:pd.Series)->pd.Series:
    return sr.dt.date

def horizontal_applier(df:pd.DataFrame,func:Callable[[tuple[pd.Series]],pd.Series],*cols:tuple[str]):
    l_cols = list(cols)
    return df[l_cols].apply(lambda c : func(*c),axis=1)

def compress_df(df:pd.DataFrame):
    df_copy=df.copy()
    col_super = {col:col[:col.find('.')] for col in df_copy.columns if col.find('.')!=-1}
    col_super_inv = bp.inverse_dict(col_super)
    for col in col_super_inv:
        cols_sub=col_super_inv[col]

        df_temp:pd.DataFrame=df_copy[cols_sub].copy()

        df_copy[col]=[[v for v in di.values()] for di in df_temp.to_dict(orient='records')]
        for col_sub in cols_sub:
            del df_copy[col_sub]
    return df_copy

def empty_records(df:pd.DataFrame)->pd.DataFrame:
    '''
    Empty values in dataframe.
    
    Parameters
    ----------
    df : pd.DataFrame
        A dataframe for referencing dtypes.
    
    Examples
    --------
    >>> d = {'col1': [1, 2], 'col2': [3, 4]}
    >>> df_append = df_empty_records(df_expanded)
    ???
    '''
    
    df_ret = df.copy()
    if len(df_ret) > 0:
        df_ret = df_ret.loc[0:0]
    
    df_ret.index.append(pd.Index([0]))
    return df_ret