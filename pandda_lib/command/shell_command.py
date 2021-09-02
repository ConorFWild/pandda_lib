import subprocess

class ShellCommand:
    def __init__(self, command):
        self.command = command

    def run(self):
        p = subprocess.Popen(
            self.command,
            shell=True,
        )

        p.communicate()