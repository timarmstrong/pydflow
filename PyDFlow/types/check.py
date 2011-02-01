from logical import flvar

"""
Logic to define function input and output type signatures and to check
lists of arguments against them.
"""

class FlTypeError(ValueError):
    def __init__(self, *args, **kwargs):
        ValueError.__init__(self, *args, **kwargs)

class InputSpec(object):
    """
    Details of an input argument to a function
    """
    def __init__(self, name, fltype):
        self.name = name
        self.fltype = fltype
        self.raw = fltype is None or (
                    not issubclass(self.fltype, flvar))

    def __repr__(self):
        return 'flinput: %s %s' % (self.name, repr(self.fltype))

    def isRaw(self):
        """
        Not a variable to be processed by PyDFlow, it will be passed through
        as a plain python variable
        """
        return self.raw


def check_logicaltype(spec, var):
    """
    Check that a type spec and a variable match up.
    The type spec may be None, in which case this means that
    a regular python variable is to be passed directly through
    """
    # First check that the logical type matches
    if spec.fltype is None:
        return var
    else:
        # A "normal" python variable is expected to be passed to the
        # function.  Don't need to do anything, except pass whatever
        # we got in
        # Check type of matched variable matches the type signature
        if not isinstance(var, spec.fltype):
            try:
                if len(var) == 1:
                    var = var[0]
            except TypeError:
                raise FlTypeError("Var %s:%s not a swift variable and not subscriptable" % (
                    spec.name, repr(var)))
            # check the subscripted type now
            if not isinstance(var, spec.fltype):
                raise FlTypeError("Var %s:%s not a subtype of specified type. \
                    Required type %s, actual type %s" % (
                    spec.name, repr(var), repr(spec.fltype), repr(var),
                    repr(spec.fltype)))
    return var 

def validate_inputs(input_spec, args, kwargs):
    """
    Checks that the arguments match input_spec.
    If they do match, return the arguments marshalled into an array 
    with the arguments in the order of input_spec.
    Otherwise it will raise an FlTypeError
    """
    # Check to see which go with the args (matched by position)
    # and which go with the kwargs
    arg_len = len(args)
    spec_len = len(input_spec)
    if arg_len > spec_len:
        raise FlTypeError("Too many positional arguments %d for input specification %d" % (arg_len, len(input_spec)))

    # Array to build list of args
    # Process positional args first
    # Assume all is ok, check_type will throw an exception
    # if there is an issue
    call_args = [check_logicaltype(spec, match)
        for spec, match in zip(input_spec, args)]

    # Try to match the rest by name
    for spec in input_spec[arg_len:]:
        # try to find in kwargs
        match = kwargs.pop(spec.name, None)
        if not match:
            raise FlTypeError("Could not find arg %s" % spec.name)
        check_logicaltype(spec, match) 
        call_args.append(match) # build up the args list

    return call_args

