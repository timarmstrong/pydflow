from decorators import task_decorator

class compound(task_decorator):
    def __init__(self, *args, **kwargs):
        super(compound, self).__init__(self, *args, **kwargs)
        
        self.task_class = CompoundTask
        
        
class CompoundTask:
    # Function takes 