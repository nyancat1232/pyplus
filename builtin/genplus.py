from typing import Generator,Any,Literal

def select_yielder(gen:Generator[tuple[Any,str],Any|Generator,None],msg:str,
                   senders:dict[str,Any]|None=None,
                   rettype:Literal['data','generator']='data'):
    '''
    catch a return of generator. yield type must be like (value:Any,msg:str).

    Parameters
    ----------
    gen : Generator
        generator that yields (value,msg).
    msg : str
        return when generator approches to the msg.
    rettype : Literal['data','generator']
        return data if data or gererator if generator. If sender is needed, choose 'generator'.
    
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
    if senders is not None:
        raise NotImplementedError(f'sender not implemented')
    for ret,current_msg in gen:
        if current_msg == msg:
            match rettype:
                case 'data':
                    return ret
                case 'generator':
                    return gen
                case _:
                    raise NotImplementedError(f'No {rettype}')