from typing import Generator,Any
from warnings import warn
import inspect
from typing import Self

class CheckPointFunctionV3:
    '''
    Making a function return by checkpoints

    
    Examples
    --------
    Assing val to 1, executes, send 10 when checkpoint 'first', and return when checkpoint 'second
    >>> @CheckPointFunctionDecoration
    >>> def itertest(val):
    >>>    val +=1
    >>>    received = yield val, 'first'
    >>>    if received is not None:
    >>>        val += received
    >>>    yield val, 'second'
    >>> itertest(1).second(first=10)
    12
    '''
    def __init__(self,func:Generator[tuple[Any,str],Any,Any]) -> None:
        self.func=func
        self.init_args=tuple()
        self.init_kwargs=dict()
    def __getattr__(self,checkpoint:str):
        class CheckPointFunctionContexted:
            def __init__(self,func:Generator[tuple[Any,str],Any,Any],
                         init_args:tuple,init_kwargs:dict,checkpoint:str) -> None:
                self.func=func
                self.checkpoint = checkpoint
                self.init_args = init_args
                self.init_kwargs = init_kwargs.copy()

            def __call__(self, **kwds: Any) -> Any:
                '''
                Executes until checkpoint reaches.
                
                Parameters
                ----------
                **kwds : Any
                    keyword arguments including value for sender.
                    Value will be sended when this function reaches the keyword.
                
                Returns
                --------
                Any
                    Return value when reaches the end of checkpoint.
                '''

                gen = self.func(*self.init_args,**self.init_kwargs)

                sender_memory = None
                while tup := gen.send(sender_memory):
                    sender_memory = None
                    if tup[1] == self.checkpoint:
                        return tup[0]
                    elif tup[1] in kwds:
                        sender_memory = kwds[tup[1]]
            def __iter__(self):
                self._gen = self.func(*self.init_args,**self.init_kwargs)
                self._stop_iter = False
                return self
            def __next__(self):
                while tup := next(self._gen):
                    if self._stop_iter:
                        raise StopIteration
                    if tup[1] == self.checkpoint:
                        self._stop_iter = True
                    return tup[0]
                    
        return CheckPointFunctionContexted(self.func,self.init_args,self.init_kwargs,checkpoint)
    def __call__(self, *args: Any, **kwds: Any) -> Self:
        '''
        Set arguments when initializing
        
        Parameters
        ----------
        *args : Any
            Positional arguments when initializing.
        *kwds : Any
            Keyword arguments when initializing.

        Returns
        --------
        Self
            Returns itself.
        
        Examples
        --------
        Initialize value val to 1.
        >>> @CheckPointFunctionDecoration
        >>> def itertest(val):
        >>>    val +=1
        >>>    received = yield val, 'first'
        >>>    if received is not None:
        >>>        val += received
        >>>    yield val, 'second'
        >>> itertest(1)
        '''
        self.init_args= args
        self.init_kwargs = kwds.copy()
        return self


def CheckPointFunctionDecoration(func:Generator[tuple[Any,str],Any,Any]):
    return CheckPointFunctionV3(func)