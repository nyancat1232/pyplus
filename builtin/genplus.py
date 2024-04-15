from typing import Generator,Any

def select_yielder(gen:Generator[tuple[Any,str],Any,None],msg:str):
    '''
    catch a return of generator. yield type must be like (value:Any,msg:str).

    Parameters
    ----------
    gen : Generator
        generator that yields (value,msg).
    
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
    '''
    for ret,current_msg in gen:
        if current_msg == msg:
            return ret