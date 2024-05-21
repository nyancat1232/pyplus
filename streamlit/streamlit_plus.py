import streamlit as st
import numpy as np
from typing import Sequence,Callable,Literal
from dataclasses import dataclass

def divide(old_func):
    def new_func(*parg,**kwarg):
        st.divider()
        old_func(*parg,**kwarg)
        st.divider()
    return new_func

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

    column_table = st.columns(len(positional_data)+len(keyword_data))

    total_data = dict(enumerate(positional_data))
    total_data.update(keyword_data)

    for index,key in enumerate(total_data):
        with column_table[index]:
            st.write(key)
            st.write(total_data[key])


class TabsPlus:
    '''
    st.tabs (in streamlit) with reading of text input

    See Also
    --------
    st.tabs

    Examples
    --------
    >>> tabs = TabsPlus('tab',['apple','banana'])
    >>> with tabs['apple']:
    >>>     ...

    is eqaul to

    >>> tabs = st.tabs(['apple','banana'])
    >>> with tabs[0]:
    >>>     ...
    '''
    def __init__(self,connection:Literal['tab','column','popover']='tab',*tabs:str):
        '''
        (description)
        
        Parameters
        ----------
        connection : 'tab' or 'column' or 'popover
            How to display.
        tabs : list[str]
            What to display.
        
        Examples
        --------
        >>> tabs = stp.TabsPlus('column',['first','second','third'])
        >>> with tabs['first']:
        >>>     ....
        '''
        tab_information={tab_str:ind for ind,tab_str in enumerate(tabs)}
        ret_list=[]
        match connection:
            case 'tab':
                ret_list = st.tabs(tabs)
            case 'column':
                ret_list = st.columns(len(tabs))
            case 'popover':
                cols = st.columns(len(tabs))
                for col,tab in zip(cols,tabs):
                    ret_list.append(col.popover(tab))
        self._streamlit_display_index_num = ret_list
        self._strs_to_num = tab_information

    def __getitem__(self,item):
        return self._streamlit_display_index_num[self._strs_to_num[item]]

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