from typing import Generator,Any
from warnings import warn
import inspect

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
        warn('CheckPointFunction v1 will be deprecated')
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
    warn('dec_checkpoint_function v1 will be deprecated')
    return lambda func:CheckPointFunction(func,sender_value)

class CheckPointFunctionV2:
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
    >>> bp.CheckPointFunctionV2(itertest).second(1,first=10)
    12
    '''
    def __init__(self,func:Generator[tuple[Any,str],Any,Any]) -> None:
        self.func=func
    def __getattr__(self,checkpoint:str):
        class CheckPointFunctionContexted:
            def __init__(self,func:Generator[tuple[Any,str],Any,Any],checkpoint:str) -> None:
                self.func=func
                self.checkpoint = checkpoint

            def __call__(self, *args: Any, **kwds: Any) -> Any:
                params_func = inspect.signature(self.func).parameters
                param_kwds = set(kwds) & set(params_func)
                param_args = {key:kwds[key] for key in param_kwds}

                gen = self.func(*args,**param_args)

                sender_kwds =  set(kwds) - set(params_func)
                sender_args = {key:kwds[key] for key in sender_kwds}

                sender_memory = None
                while tup := gen.send(sender_memory):
                    sender_memory = None
                    if tup[1] == self.checkpoint:
                        return tup[0]
                    elif tup[1] in sender_args:
                        sender_memory = sender_args[tup[1]]
        return CheckPointFunctionContexted(self.func,checkpoint)
    

def dec_checkpoint_function_v2(func:Generator[tuple[Any,str],Any,Any]):
    return CheckPointFunctionV2(func)