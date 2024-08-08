from typing import Generator,Any

class CheckPointFunction:
    '''
    Making a function return by checkpoints

    
    Examples
    --------
    >>> def itertest(val):
    >>>     val +=1
    >>>     received = yield val, 'first'
    >>>     if received is not None:
    >>>         val += received
    >>>     yield val, 'second'
    >>> bp.CheckPointFunction(itertest,{'first':10}).second(1)
    12
    '''
    def __init__(self,func:Generator[tuple[Any,str],Any,Any],sender_value:dict[str,Any]=dict()) -> None:
        self.func=func
        self.sender_value=sender_value
    def __getattr__(self,checkpoint:str):
        def new_func(*parg,**kwarg):
            gen = self.func(*parg,**kwarg)
            sender_memory = None
            while tup := gen.send(sender_memory):
                sender_memory = None
                if tup[1] == checkpoint:
                    return tup[0]
                elif tup[1] in self.sender_value:
                    sender_memory = self.sender_value[tup[1]]
        return new_func
    def __call__(self, *args: Any, **kwds: Any) -> Any:
        gen = self.func(*args,**kwds)
        sender_memory = None
        try:
            while tup := gen.send(sender_memory):
                if tup[1] in self.sender_value:
                    sender_memory = self.sender_value[tup[1]]
        except StopIteration as ret:
            return ret.value

def dec_checkpoint_function(sender_value:dict[str,Any]):
    '''
    Decorator for a function
    
    Parameters
    ----------
    sender_value : dict[str,Any]
        Apply a value when checkpoint meets.
    
    Examples
    --------
    >>> @dec_checkpoint_function({'first':10})
    >>> def itertest(val):
    >>>     val +=1
    >>>     received = yield val, 'first'
    >>>     if received is not None:
    >>>         val += received
    >>>     yield val, 'second'
    >>> itertest.second(1)
    12
    '''
    return lambda func:CheckPointFunction(func,sender_value)