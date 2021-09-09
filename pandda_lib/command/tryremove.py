import shutil
from pathlib import Path


class TryRemove:

    def __init__(self, path: Path):
        self.path = path

    def __call__(self, *args, **kwargs):
        if self.path.exists():
            shutil.rmtree(str(self.path))
            return self.path

        else:
            return self.path
