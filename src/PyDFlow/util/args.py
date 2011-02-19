
def is_indexable(var):
    try:
        var[0]
        return True
    except:
        return False

def is_iterable(var):
    return  hasattr(var, '__iter__') 
