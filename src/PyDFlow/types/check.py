# Copyright 2010-2011 Tim Armstrong <tga@uchicago.edu>
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from logical import flvar, Placeholder
import inspect

"""
Logic to define function input and output type signatures and to check
lists of arguments against them.
@author: Tim Armstrong
"""

def unpack(fltype):
    multi = False
    lazy = False
    while True:
        if isinstance(fltype, Multiple):
            fltype = fltype.internal
            multi = True
        elif isinstance(fltype, Lazy):
            fltype = fltype.internal
            lazy = True
        else:
            break
    raw = fltype is None or (not issubclass(fltype, flvar))
    return fltype, raw, multi, lazy


class Lazy(object):
    """
    Specifies that argument should be lazy
    """
    def __init__(self, internal):
        self.internal = internal

class Multiple(object):
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

NoDefault = object()

class InputSpec(object):
    """
    Details of an input argument to a function
    """
    def __init__(self, name, fltype, default_val):
        self.name = name
        self.fltype, self.raw, self.multi, self.lazy = unpack(fltype)
        self.default_val = default_val
        
    def __repr__(self):
        return 'flinput: %s %s' % (self.name, repr(self.fltype))

    def isRaw(self):
        """
        Not a variable to be processed by PyDFlow, it will be passed through
        as a plain python variable
        """
        return self.raw

    def isMulti(self):
        return self.multi
    
    def isLazy(self):
        return self.lazy

class TaskDescriptor(object):
    """
    Store type and do type checking for a task.
    """
    def __init__(self, wrapped, input_types, output_types):
        """
        wrapped is a funciton to be wrapped
        input_types and output_types are lists or tuples
        """
        self.input_types = input_types
        self.output_types = output_types
        self._output_wrapper = None
        
        rawspec = inspect.getargspec(wrapped)
        arg_names = rawspec[0]
        remainder_name = rawspec[1]
        

        if remainder_name is None:
            if len(arg_names) != len(self.input_types):
                #TODO
                raise Exception("Mismatch between function argument count %d \
                        and input type tuple length %d for function %s" % (
                        len(arg_names), len(self.input_types), 
                        wrapped.__name__))
        else:
            if len(arg_names) + 1 != len(self.input_types):
                raise Exception("Mismatch between function argument count %d \
                        and input type tuple length %d for function %s" % (
                        len(arg_names)+1, len(self.input_types), 
                        wrapped.__name__))
        
        # add default value information to arg spec
        default_vals = rawspec[3]
        if default_vals is None:
            padded_default_vals = [NoDefault] * len(arg_names)
        else:
            default_padding = [NoDefault] * (len(arg_names) - len(default_vals))
            padded_default_vals = default_padding + list(default_vals)
                
        #TODO: validate not lazy if needed??
        # Build the input specification for the function using introspection
        self.input_spec = [InputSpec(name, t, default_val) 
                    for t, name, default_val 
                    in zip(self.input_types, arg_names, padded_default_vals)]

        if remainder_name is not None:
            self.input_spec.append(InputSpec(remainder_name,
                    self.input_types[-1], NoDefault))

    def validate_inputs(self, args, kwargs):
        return validate_inputs(self.input_spec, args, kwargs)

    def validate_outputs(self, outputs):
        """
        Checks a list of outputs
        """
        # Pack into a tuple if needed
        if not isinstance(outputs, (list, tuple)):
            outputs = (outputs,)
        if len(outputs) != len(self.output_types):
            raise FlTypeError("_outputs must match length of output_types")
        err = [(ivar, t) for ivar, t in zip(outputs, self.output_types)
                if not issubclass(t, ivar.__class__)]
        if err:
            raise FlTypeError("Output I-var(s) of wrong type provided: %s"
                % (repr(err)))

    def set_output_wrapper(self, wrapper):
        self._output_wrapper = wrapper

    def make_outputs(self):
        """
        Initialize a set of output I-vars with correct type.
        """
        if self._output_wrapper is not None:
            return [self._output_wrapper(var_cls)() for var_cls in self.output_types]
        else:
            return [var_cls() for var_cls in self.output_types]
    
    def input_count(self):
        return len(self.input_spec)

    def zip(self, other):
        return spec_zip(self.input_spec, other)
    


                    


def check_logicaltype(thetype, var, name=None):
    """
    Check that a type spec and a variable match up.
    The type spec may be None, in which case this means that
    a regular python variable is to be passed directly through
    """
    # First check that the logical type matches
    if thetype is None:
        return var
    if isinstance(var, Placeholder):
        if issubclass(var._expected_class, thetype):
            return var
        else:
            raise FlTypeError("Proxy has wrong expected class %s, not a subclass of %s" % (
                                    (repr(var._expected_class), repr(thetype))))
        
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
        except AttributeError:
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
    
    TODO: use default value info in input spec to allow use of default arguments
    """
    # Check to see which go with the args (matched by position)
    # and which go with the kwargs
    arg_len = len(args)
    spec_len = len(input_spec)
    # Array to build list of args
    # Process positional args first
    # Assume all is ok, check_type will throw an exception
    # if there is an issue
    call_args = [check_logicaltype(spec.fltype, match)
        for spec, match in zip(input_spec, args)]
    
    if arg_len > spec_len:
        if spec_len > 0 and input_spec[-1].isMulti():
            rep_type = input_spec[-1].fltype
            for arg in args[spec_len:]: 
                check_logicaltype(rep_type, arg, input_spec[-1].name)
                call_args.append(arg)
        else:
            raise FlTypeError("Too many positional arguments %d for input specification %d" % (arg_len, spec_len))
    elif arg_len < spec_len:
        # Try to match the rest by name
        for spec in input_spec[arg_len:]:
            # try to find in kwargs
            match = kwargs.pop(spec.name, None)
            if not match:
                if spec.default_val is NoDefault:
                    raise FlTypeError("Could not find arg %s" % spec.name)
                else:
                    match = spec.default_val
            check_logicaltype(spec.fltype, match)                 
            call_args.append(match) # build up the args list
    if len(kwargs) > 0:
        raise FlTypeError("%d extra keyword args supplied: %s" % (len(kwargs), repr(kwargs)))
    return call_args

def validate_swap(new_ivars, old_ivars):
    """
    Validate that a list of old_ivars can be replaced with new_ivars without breaking types.
    """
    if len(new_ivars) != len(old_ivars):
        raise FlTypeError("new ivars list has different length %d from old ivars %d" %
                          (len(new_ivars), len(old_ivars)))
        
    for new, old in zip(new_ivars, old_ivars):
        if not isinstance(new, old.__class__):
            raise FlTypeError(("new I-var %s is not a subclass of the class of" +  
                " old I-var %s, cannot replace") % (repr(new), repr(old)))


def spec_zip(input_spec, other):
    for i, spec in enumerate(input_spec):
        if spec.isMulti():
            if i != len(input_spec) - 1:
                raise Exception("Unexpected problem: Multiple was not \
                    last type in input spec")
            othlen = len(other)
            while i < othlen:
                #TODO: hack
                yield spec, other[i]
                i += 1
        else:
            yield spec, other[i]
    return
    
