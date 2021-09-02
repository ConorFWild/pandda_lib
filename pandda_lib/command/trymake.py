import os


class TryMake:

    def __init__(self, path):
        self.path = path


    def __call__(self, *args, **kwargs):
        if self.path.exists():
            return self.path
        else:
            os.mkdir(self.path)
            return self.path
