
def inverse_dict(dict_current:dict)->dict:
    dict_inverse = dict()
    for key in dict_current:
        if dict_current[key] in dict_inverse:
            dict_inverse[dict_current[key]] += [key]
        else:
            dict_inverse[dict_current[key]] = [key]
    return dict_inverse