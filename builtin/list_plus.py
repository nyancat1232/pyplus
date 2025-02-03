from typing import Any

def swap_in_list(list_,index1:int,index2:int,mutable:bool=False):
    if mutable:
        temp = list_[index1]
        list_[index1] = list_[index2]
        list_[index2] = temp
    else:
        ret = list_.copy()
        ret[index1] = list_[index2]
        ret[index2] = list_[index1]
        return ret
    
def change_position(list_:list[Any],index_from:int,index_to:int,mutable=False):
    if mutable:
        raise Exception("No implementation")
    else:
        temp = list_.copy()
        temp_val = temp[index_from]
        temp.pop(index_from)
        temp.insert(index_to,temp_val)
        return temp
    
def split_list(list_in, split_col_num=1):
    return [list_in[col::split_col_num] for col in range(split_col_num)]

def transpose_list(list_in,default_val=None):
    list_temp = list_in.copy()
    if len(list_temp[-1])-len(list_temp[0]) != 0:
        for l in list_temp:
            l.append(default_val)
    return list(map(list, zip(*list_temp)))

def split_list_by_key(list_key,list_in):
    return {key: list_in[ind::len(list_key)] for ind,key in enumerate(list_key)}


def apply(multi_list,func):
    rr = []
    for v in multi_list:
        if type(v)==list:
            rr.append(apply(v,func))
        else:
            rr.append(func(v))
    return rr

class ListPlus(list):
    def append(self, object):
        super().append(object)
        return self
    def fillna(self,value):
        for ind,_ in enumerate(super().__iter__()):
            if (val:=super().pop(ind)) is None:
                super().insert(ind,value)
            else:
                super().insert(ind,val)
        return self