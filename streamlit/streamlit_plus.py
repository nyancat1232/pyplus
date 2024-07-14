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

def write_columns(*positional_data,**keyword_data):
    '''
    write by columns
    ## Parameters:
    positional_data : Any
        ...
    keyword_data : Any
        ....
    ## See Also:
    st.write
    st.columns
    ## Examples:
    >>> from pyplus.streamlit.streamlit_plus import write_columns
    >>> import streamlit as st
    >>> xx=np.array([['x1','x2'],['x3','x4']])
    >>> ww=np.array([['w1','w2'],['w3','w4']])
    >>> write_columns(X1=xx,W1=ww)
    X1           W1
    0    1       0       1
    x1   x2      w1      w2
    x3   x4      w3      w4

    '''
    if len(positional_data)+len(keyword_data)<1:
        st.write('No arguments')
        return
    
    dict_add = {num:val for num,val in enumerate(positional_data)}
    for key in keyword_data:
        dict_add[key] = keyword_data[key]
    tp = TabsPlus(layout='column',titles=dict_add)
    for key in dict_add:
        with tp[key]:
            st.write(key)
            st.write(dict_add[key])
    return dict_add    

def list_text_input_by_vals(*attribute_list,**kwarg_text_input):
    return {attr : st.text_input(label=f'{attr}',**kwarg_text_input) for attr in attribute_list}

def list_checkbox(*names):
    return {name : st.checkbox(label=name,value=False) for name in names}

def list_text_inputs(label):
    row_amount = st.slider(label=f'{label}\'s row',min_value=1,max_value=100,value=1)
    col_amount = st.slider(label=f'{label}\'s column',min_value=1,max_value=100,value=1)
    def gen_names(current_label,max_num):
        cur_name=0
        ret = f'{current_label}_{cur_name}'
        yield ret
        while (cur_name:=cur_name+1)<max_num:
            ret = f'{current_label}_{cur_name}'
            yield ret

    input_data=[]
    columns=st.columns(col_amount)
    for col_ind, column in enumerate(columns):
        with column:
            row_data=[]
            for row_ind in range(row_amount):
                row_data.append(st.text_input(label=f'{label}\'s c{col_ind} r{row_ind}'))
            input_data.append(row_data)
    

    return input_data


def list_slider_inputs(label,row_nums=2,col_nums=2):
    res = np.zeros(shape=(row_nums,col_nums))

    cols = st.columns(col_nums)
    for row_ind in range(row_nums):
        for col_ind,_ in enumerate(cols):
            with cols[col_ind]:
                res[row_ind,col_ind]=st.slider(f'{label},row_{row_ind},column_{col_ind}',-1.,1.,0.)
    return res