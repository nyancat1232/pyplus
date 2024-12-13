import streamlit as st
import numpy as np
from typing import Literal
from typing import Generator,Any
from dataclasses import dataclass
from warnings import warn

def divide(old_func):
    def new_func(*parg,**kwarg):
        st.divider()
        old_func(*parg,**kwarg)
        st.divider()
    return new_func

def show_process(gen:Generator[tuple[Any,str],Any,None],column_configs:dict[str,st.column_config.Column]=None):
    '''
    For a generation for pyplus.builtin
    
    Parameters
    ----------
    gen : Generator
        A generator that can be run by pyplus.builtin.
    
    See Also
    --------
    pyplus.buintin
    
    Returns
    --------
    dict
        The key indicates the name of process, the value indicates the result of process in the time.
    '''
    dd= {}
    for df,proc_msg in gen:
        dd[proc_msg] = df
    
    tp = TabsPlus(titles=[key for key in dd],layout='tab')
    for key in dd:
        with tp[key]:
            st.dataframe(dd[key],column_config=column_configs)
    return dd

class TabsPlus:
    '''
    st.tabs (in streamlit) with reading of text input

    Parameters
    --------
    titles: list[str]
        titles for displaying each tabs
    layout: 'tab', 'column', 'popover'
        specifies how to display each tabs.
        If 'tab', tabs will be shown as st.tabs.
        If 'column', tabs will be shown as st.column.
        If 'popover', tabs will be shown as st.popover.

    See Also
    --------
    st.tabs
    st.column
    st.popover

    Examples
    --------
    >>> tabs = TabsPlus(titles=['apple','banana'],layout='tab')
    >>> with tabs['apple']:
    >>>     ...

    is eqaul to

    >>> tabs = st.tabs(['apple','banana'])
    >>> with tabs[0]:
    >>>     ...
    '''
    def __init__(self,*,titles:list[str],
                 layout:Literal['tab','column','popover']='tab',hide_titles=True):
        tab_information={tab_str:ind for ind,tab_str in enumerate(titles)}
        ret_list=[]
        if len(titles)>0:
            match layout:
                case 'tab':
                    ret_list = st.tabs(titles)
                case 'column':
                    ret_list = st.columns(len(titles))
                    if hide_titles==False:
                        for col,tab_name in zip(ret_list,titles):
                            col.subheader(tab_name)
                case 'popover':
                    cols = st.columns(len(titles))
                    for col,tab_name in zip(cols,titles):
                        ret_list.append(col.popover(tab_name))
                case _:
                    raise NotImplementedError(f'no connection {layout}')
            self._streamlit_display_index_num = ret_list
            self._strs_to_num = tab_information

    def __getitem__(self,item):
        return self._streamlit_display_index_num[self._strs_to_num[item]]
    
    def __getattr__(self,attr):
        return self._streamlit_display_index_num[self._strs_to_num[attr]]