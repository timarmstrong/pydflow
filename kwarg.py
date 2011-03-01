class Blah:
    def __init__(self, __bind_location, **kwargs):
        self.b = kwargs['__bind_location']
    def __repr__(self):
        return repr(self.b)

x = Blah(__bind_location="hello")

print x
