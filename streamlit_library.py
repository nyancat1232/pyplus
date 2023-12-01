import streamlit as st

def divide(old_func):
    def new_func(*parg,**kwarg):
        st.divider()
        old_func(*parg,**kwarg)
        st.divider()
    return new_func

def write_col_table(*positional_data,**keyword_data):
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

def write_tabs(*funcs,names=None):
    '''
    from amesho import streamlit_library as stl

    func_list=[]
    @stl.add_tab_func(func_list):
    def func_name():
        pass
    stl.write_tabs(*func_list)

    '''
    if names is None:
        names = [f'{funcs[num].__name__}' for num in range(len(funcs))]

    tabs = st.tabs(names)
    tabs_func = funcs
    for tab,tab_func in zip(tabs,tabs_func):
        with tab:
            tab_func()

def add_tab_func(func_list):
    '''Decoration for write_tabs
    func_list => which collects a function.
    '''
    def ret_func(func):
        func_list.append(func)
    return ret_func

import pandas as pd
import tabula as tb

def from_csv_to_dataframe(label):
    if file := st.file_uploader(label=label,type="csv"):
        return pd.read_csv(filepath_or_buffer=file)


def from_pdf_to_dataframe(label,number=0):
    if file := st.file_uploader(label=label,type="pdf"):
        return tb.read_pdf(file)[number]

def from_txt_to_dataframe(label,preprocess_function):
    if text := st.text_area(label=label):
        return preprocess_function(text)