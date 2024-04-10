from typing import Generator

def select_yielder(gen:Generator,msg:str):
    '''
    catch a return of generator. yield type must be like (msg:str,value:Any).
    
    Parameters
    ----------
    gen : Generator
        generator that yields (msg,value).
    
    Returns
    --------
    Any
        a value of yield.
    
    Examples
    --------
    >>> def test_yield(first_msg):
    >>>     yield f'{first_msg}: apple', 3
    >>>     yield f'{first_msg}: banaba', 1
    >>>     yield f'{first_msg}: cherry', 2
    >>> 
    >>> aa = select_yielder(test_yield('Rick'),'Rick: banaba')
    >>> aa
    1
    '''
    for current_msg,ret in gen:
        if current_msg == msg:
            return ret