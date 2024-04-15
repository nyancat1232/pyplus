from bs4 import BeautifulSoup,ResultSet,Tag
from requests import get
from requests.exceptions import ConnectionError
import pandas as pd
from typing import Callable
from dataclasses import dataclass,field
from time import sleep
import aiohttp
import asyncio

@dataclass
class SoupElement:
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36'}
    my_headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS\
            X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko)\
            Chrome/71.0.3578.98 Safari/537.36", \
            "Accept":"text/html,application/xhtml+xml,application/xml;\
            q=0.9,image/webp,image/apng,*/*;q=0.8"}
    name:str
    url:str
    bs_result:BeautifulSoup = field(init=False)
    last_table:pd.DataFrame = field(init=False)

    def open_bs(self,max_trial:int=3):
        for current in range(max_trial):
            try:
                resp = get(url=self.url,headers=SoupElement.my_headers)
                self.bs_result=BeautifulSoup(markup=resp.content,features='html5lib')
                return self.bs_result
            except ConnectionError as ce:
                print(f"failed at {current}")
                sleep(1.0)
        print("No connetion")
    
    def get_all_tables(self)->pd.DataFrame:
        tables:ResultSet[Tag] = self.bs_result.find_all(name='table')

        result_tables=[]
        for index,table in enumerate(tables):
            
            current_table = []
            for index,row in enumerate(table.find_all('tr')):
                row : Tag
                current_row = []
                for data in row.find_all('td'):
                    data : Tag
                    current_row.append(data.text)
                current_table.append(current_row)
            result_tables.append(pd.DataFrame(current_table))

        self.last_table = result_tables
        return self.last_table
    
    def find_all(self,name,attrs:dict|None=None)->ResultSet[Tag]:
        rets = self.bs_result.find_all(name=name,attrs=attrs)
        for ret in rets:
            assert isinstance(ret,Tag)

        return rets

class BSPlus:
    bss : list[SoupElement]
    session : aiohttp.ClientSession
    num_of_repeat : int

    def __init__(self,lbss:list[SoupElement],num_of_repeat=5,aiosession=None):
        self.session = aiosession
        self.bss = []
        for bs in lbss:
            self.bss.append(bs)
        self.num_of_repeat=num_of_repeat
            
    def __call__(self,
                 pre_callback_func:Callable|None=None,
                 post_callback_func:Callable|None=None):
        return self.do_process(pre_callback_func=pre_callback_func,
                               post_callback_func=post_callback_func)
    
    def do_process(self,
                 pre_callback_func:Callable|None=None,
                 post_callback_func:Callable|None=None):
        for bs in self.bss:
           if pre_callback_func is not None:
               pre_callback_func(bs)
           bs.open_bs() 
           if post_callback_func is not None:
               post_callback_func(bs)
        return self
    
    def append(self,se:SoupElement):
        self.bss.append(se)
        return self.bss

    def get_all_tables(self)->dict[str,pd.DataFrame]:
        return {bs.name: bs.get_all_tables() for bs in self.bss}
    
    def find_all(self,name,attrs:dict|None=None)->dict[str,ResultSet[Tag]]:
        return {bs.name: bs.find_all(name,attrs) for bs in self.bss}