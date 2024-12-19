import streamlit as st
import pandas as pd
from dataclasses import dataclass
from typing import Callable,Self
import re
from unicodedata import normalize

@dataclass
class FileDescription:
    file_regex_str : str
    read_method : Callable
    dataframe_post_process : Callable|None = None
    var_name : str|None = None
    read_method_kwarg : dict|None = None
    dataframe_post_process_kwarg : dict|None  = None

class FileExecutor:
    '''
    Accept multiple files and reads some files you want.
    
    ## Examples:
    >>> import pyplus.streamlit as stp
    >>> 
    >>> fe = stp.FileExecutor()
    >>> fe += '^train.csv$',pd.read_csv
    >>> fe += '^codebook.csv$',pd.read_csv
    >>> dfs = fe()

    '''
    behaviors : list[FileDescription] = []
    
    def __iadd__(self,other:tuple[str,Callable] | tuple[str,Callable,Callable,str|None,dict,dict] )->Self:
        '''
        add behavior
        
        Parameters
        ----------
        file_regex : str
            regex for finding a file.
        read_method : Callable
            function how to convert a file to a dataframe
        dataframe_post_process : Callable
            function how to do after converting to a dataframe
        var_name : str|None
            key name for file
        read_method_kward : dict|None
            ??
        dataframe_post_process_kwarg : dict|None
            ??
        
        See Also
        --------
        (sa_description)
        
        Examples
        --------
        (descripton_of_line)
        >>> (ex_code)
        (result_of_code)
        '''
        self.behaviors.append(FileDescription(*other))
        return self
    
    def __call__(self,show:bool=False,label:str='multifiles test')->dict[str,pd.DataFrame]:
        '''
        Accept multiple files and reads some files you want.
        ## Parameters:
        behaviors : ...
            Case that accept files.
        show : bool
            Verbose. Show if accept files.
        ## Examples:
        >>> import streamlit as st
        >>> import pandas as pd
        >>> from pyplus.streamlit.streamlit_plus_utility import FileExecutor,FileDescription
        >>> 
        >>> with st.sidebar:
        >>>     fe = FileExecutor()
        >>>     fe.behaviors.append( FileDescription('ratings.txt',pd.read_table) )
        >>>     dfs = fe.execute_file_descriptions()
        >>> 
        >>> dfs
        '''
        input_df={}
        multi_files=st.file_uploader(label,accept_multiple_files=True)
        for file in multi_files:
            for behavior in self.behaviors:
                if show:
                    st.write(re.compile(behavior.file_regex_str))
                    st.write(file.name)

                if re.compile(normalize("NFC",behavior.file_regex_str)).match(normalize("NFC",file.name)) is not None:
                    if behavior.var_name:
                        input_key = behavior.var_name
                    else:
                        input_key = "_".join(file.name.split(".")[:-1])

                    if show:
                        st.write(f"Assuming {file.name} is a {input_key}")
                    
                    try:
                        _temp_df = behavior.read_method(file,**behavior.read_method_kwarg)
                    except Exception as e1:
                        st.error(e1)
                        try:
                            _temp_df = behavior.read_method(file)
                        except Exception as e:
                            st.error(e)
                    
                    try:
                        input_df[input_key] = behavior.dataframe_post_process(_temp_df,**behavior.dataframe_post_process_kwarg)
                    except:
                        try:
                            input_df[input_key] = behavior.dataframe_post_process(_temp_df)
                        except Exception as e:
                            input_df[input_key] = _temp_df
                            st.error(e)

        return input_df

