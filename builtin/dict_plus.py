
def inverse_dict(dict_current:dict)->dict:
    '''
    Inverse dictionary between key and value, and if there are duplicate values, then use list.
    
    Parameters
    ----------
    dict_current : dict
        Dictionary what you want to apply.
    
    See Also
    --------
    (sa_description)
    
    Returns
    --------
    dict
        dictionary.
    
    Examples
    --------
    
    >>> import pyplus.builtin as bp
    >>> vv = {'apple':'fruit','banana':'fruit','cat':'animal'}
    >>> bp.inverse_dict(vv)
    {'fruit': ['apple', 'banana'], 'animal': ['cat']}
    '''
    dict_inverse = dict()
    for key in dict_current:
        if dict_current[key] in dict_inverse:
            dict_inverse[dict_current[key]] += [key]
        else:
            dict_inverse[dict_current[key]] = [key]
    return dict_inverse