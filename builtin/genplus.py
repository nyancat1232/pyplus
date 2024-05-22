from typing import Generator,Any,Literal,Iterable,Callable
import itertools as its

def select_yielder(gen:Generator[tuple[Any,str],Any,None],begin_msg:str,
                   rettype:Literal['data','generator']='data'):
    '''
    catch a return of generator. yield type must be like (value:Any,begin_msg:str).

    Parameters
    ----------
    gen : Generator
        generator that yields (value,begin_msg).
    begin_msg : str
        return when generator approches to the begin_msg.
    rettype : Literal['data','generator']
        return data of beginning if data or gererator if generator. 
    
    Returns
    --------
    Any
        a value of yield.
    
    Examples
    --------
    >>> def test_yield(first_msg):
    >>>     yield 3,f'{first_msg}: apple'
    >>>     yield 1,f'{first_msg}: banaba'
    >>>     yield 2,f'{first_msg}: cherry'
    >>> 
    >>> aa = select_yielder('Rick: banaba',test_yield('Rick'))
    >>> aa
    1
    
    Recommend using for a private method
    >>> import pandas as pd
    >>> import pyplus.builtin as bp
    >>> 
    >>> class Test:
    >>>     def _private_method(self):
    >>>         ...
    >>>         yield df.copy(), 'first return'
    >>>         ...
    >>>         yield df.copy(), 'second return'
    >>>         ...
    >>>
    >>>     def first_method(self):
    >>>         return bp.select_yielder(self._private_method(),'first return') 
    >>>
    >>>     def second_method(self):
    >>>         return bp.select_yielder(self._private_method(),'second return') 
    '''
    
    for ret,current_msg in gen:
        if current_msg == begin_msg:
            match rettype:
                case 'data':
                    return ret
                case 'generator':
                    return gen
                case _:
                    raise NotImplementedError(f'No {rettype}')

def pass_sender(gen:Generator[tuple[Any,str],Any,None],**passer:Any)->Generator[tuple[Any,str],Any,None]:
    msg=None
    try:
        while True:
            if msg in passer:
                val,msg=gen.send(passer[msg])
            else:
                val,msg=next(gen)
            yield val,msg
    except StopIteration as si:
        pass

def pass_senders_parallel(func:Callable,**passer:Iterable[Any])->Generator[tuple[Any,str],Any,None]:
    passer_iterable_check=(isinstance(passer[key],Iterable) for key in passer)
    if not all(passer_iterable_check):
        raise TypeError('All must be iterable')
    
    def apply_prod(*pr):
        return [r for r in its.product(*pr)]
    passer_vals=(apply_prod([key],passer[key]) for key in passer)
    all_case = (case for case in its.product(*passer_vals))
    ret=[]
    for case in all_case:
        di_res=dict()
        for one_passer in case:
            di_res[one_passer[0]]=one_passer[1]
        ret+=[di_res]
    all_case_gen = (pass_sender(func(),**case) for case in ret)
    return all_case_gen