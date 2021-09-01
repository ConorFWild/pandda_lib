from dataclasses import dataclass
from pathlib import Path
import subprocess

command: str = "sshpass -p '{password}' rsync --progress --exclude 'autoprocessing' --exclude 'dimple' --exclude 'jpg' --exclude '*.map' --exclude 'processed' --exclude '*.png' --exclude '*.pickle' --exclude '*.jpg' -L -avzh -e ssh zoh22914@ssh.diamond.ac.uk:{path_to_remote_dir}/ {path_to_loca_dir}"


@dataclass()
class RsyncDirs:
    command: str

    def run(self):
        p = subprocess.Popen(
            self.command,
            shell=True,
        )

        p.communicate()

    @staticmethod
    def from_paths(
            path_to_remote_dir: Path,
            path_to_local_dir: Path,
            password: str,
    ):
        return RsyncDirs(
            command.format(
                path_to_remote_dir=path_to_remote_dir,
                path_to_loca_dir=path_to_local_dir,
                password=password,
            )
        )
