from logical import flvar

"""
Logic to define function input and output type signatures and to check
lists of arguments against them.
"""

class Multiple:
    """
    Represents the type of the remainder of arg types
    Ie. if the python function signature is:
    def example(a, b, *args):

        then a type signature could be:
        (int, int, Multiple(FooFile))
    """
    def __init__(self, internal):
        self.internal = internal

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
        self.raw = isRaw(fltype)

    def __repr__(self):
        return 'flinput: %s %s' % (self.name, repr(self.fltype))

    def isRaw(self):
        """
        Not a variable to be processed by PyDFlow, it will be passed through
        as a plain python variable
        """
        return self.raw

    def isMulti(self):
        return isinstance(self.fltype, Multiple)

def isRaw(fltype):
    if isinstance(fltype, Multiple):
        fltype = fltype.internal
    return fltype is None or (not issubclass(fltype, flvar))
                    


def check_logicaltype(thetype, var, name=None):
    """
    Check that a type spec and a variable match up.
    The type spec may be None, in which case this means that
    a regular python variable is to be passed directly through
    """
    # First check that the logical type matches
    if thetype is None:
        return var
    else:
        # A "normal" python variable is expected to be passed to the
        # function.  Don't need to do anything, except pass whatever
        # we got in
        # Check type of matched variable matches the type signature
        if not isinstance(var, thetype):
            try:
                if len(var) == 1:
                    var = var[0]
            except TypeError:
                raise FlTypeError("Var %s:%s not a swift variable and not subscriptable" % (
                    repr(var), repr(var)))
            # check the subscripted type now
            if not isinstance(var, thetype):
                raise FlTypeError("Var %s:%s not a subtype of specified type. \
                    Required type %s, actual type %s" % (
                    repr(name), repr(var), repr(thetype),
                    type(type(var))))
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
    if spec_len > 0 and input_spec[-1].isMulti():
        # All args will be positional
        if len(kwargs) != 0:
            raise FlTypeError("Cannot provide positional arguments along with Multi():\
                function was called with kwargs dict %s" % repr(kwargs))

        rep_type = input_spec[-1].fltype.internal
        for i, arg in enumerate(args):
            if i < spec_len - 1:
                check_logicaltype(input_spec[i].fltype, arg, 
                        input_spec[i].name)
            else:
                check_logicaltype(rep_type, arg, input_spec[-1].name)
#        print "Processed args:", repr(args)
        return args
    else:
        if arg_len > spec_len:
            raise FlTypeError("Too many positional arguments %d for input specification %d" % (arg_len, spec_len))

        # Array to build list of args
        # Process positional args first
        # Assume all is ok, check_type will throw an exception
        # if there is an issue
        call_args = [check_logicaltype(spec.fltype, match)
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

def spec_zip(inputs, input_spec):
    for i, spec in enumerate(input_spec):
        if spec.isMulti():
            if i != len(input_spec) - 1:
                raise Exception("Unexpected problem: Multiple was not \
                    last type in input spec")
            inlen = len(inputs)
            while i < inlen:
                #TODO: hack
                yield (inputs[i], spec)
                i += 1
        else:
            yield inputs[i], spec
    return
    
