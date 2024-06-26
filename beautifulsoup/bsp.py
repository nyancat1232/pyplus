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

    def open_bs(self,max_trial:int=3,time_wait=1.0,
                pre_callback:Callable[[str],None]|None=None,
                post_callback:Callable[[str],None]|None=None,
                retry_callback:Callable[[str],None]|None=None):
        for current in range(max_trial):
            if pre_callback is not None:
                pre_callback(self.name)
            try:
                resp = get(url=self.url,headers=SoupElement.my_headers)
                self.bs_result=BeautifulSoup(markup=resp.content,features='html5lib')
                if post_callback is not None:
                    post_callback(self.name)
                return self.bs_result
            except Exception as e:
                if retry_callback is not None:
                    retry_callback(f'{self.name} {current}')
                sleep(time_wait)
        if retry_callback is not None:
            retry_callback('No connection')
    
    def find_all(self,name,attrs:dict|None=None)->ResultSet[Tag]:
        rets = self.bs_result.find_all(name=name,attrs=attrs)
        for ret in rets:
            assert isinstance(ret,Tag)

        return rets

class BSPlus:
    bss : list[SoupElement]
    num_of_repeat : int
    time_wait : float
    session : aiohttp.ClientSession
    pre_callback : Callable[[str],None]|None
    post_callback : Callable[[str],None]|None
    retry_callback : Callable[[str],None]|None

    def __init__(self,
                 pre_callback:Callable[[str],None]|None=None,
                 post_callback:Callable[[str],None]|None=None,
                 retry_callback:Callable[[str],None]|None=None,
                 num_of_repeat=5,time_wait:float=0.2,aiosession=None):
        self.bss = []

        self.num_of_repeat=num_of_repeat
        self.time_wait=time_wait
        self.session = aiosession
        self.pre_callback=pre_callback
        self.post_callback=post_callback
        self.retry_callback=retry_callback
            
    def append_url(self,se:SoupElement)->list[SoupElement]:
        self.bss.append(se)
    
    def __iadd__(self,se:SoupElement):
        '''
        Add a soup element
        
        Parameters
        ----------
        se : SoupElement
            SoupElement for an url.
        
        Examples
        --------
        >>> bsp = BSPlus(num_of_repeat=5,time_wait=7.5)
        >>> bsp += SoupElement(name='google',url=f'https://www.google.com')

        See Also
        --------
        append_url(self,se)
        '''
        self.append_url(se)
        return self
    
    def __call__(self):
        return self.do_process()
    
    def do_process(self):
        for bs in self.bss:
            bs.open_bs(max_trial=self.num_of_repeat,time_wait=self.time_wait,
                       pre_callback=self.pre_callback,post_callback=self.post_callback,
                       retry_callback=self.retry_callback) 
        return self
    
    def append(self,se:SoupElement):
        self.bss.append(se)
        return self.bss

    def get_all_tables(self)->dict[str,pd.DataFrame]:
        return {bs.name: bs.get_all_tables() for bs in self.bss}
    
    def find_all(self,name,attrs:dict|None=None)->dict[str,ResultSet[Tag]]:
        return {bs.name: bs.find_all(name,attrs) for bs in self.bss}