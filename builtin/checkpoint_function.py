from typing import Generator,Any
from warnings import warn
import inspect

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
                '''
                Executes until checkpoint reaches.
                
                Parameters
                ----------
                *args : Any
                    Positional arguments
                **kwds : Any
                    keyword arguments including value for sender.
                    Value will be sended when this function reaches the keyword.
                
                Returns
                --------
                Any
                    Return value when reaches the end of checkpoint.
                '''
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
            def __iter__(self):
                self._gen = self.func()
                self._stop_iter = False
                return self
            def __next__(self):
                while tup := next(self._gen):
                    if self._stop_iter:
                        raise StopIteration
                    if tup[1] == self.checkpoint:
                        self._stop_iter = True
                    return tup[0]
                    
        return CheckPointFunctionContexted(self.func,checkpoint)
    

def CheckPointFunctionDecoration(func:Generator[tuple[Any,str],Any,Any]):
    return CheckPointFunctionV2(func)