from dataclasses import dataclass
from pathlib import Path
import subprocess

command: str = "sshpass -p '{password}' rsync --progress --exclude 'autoprocessing' --exclude 'dimple' --exclude 'jpg' --exclude '*.map' --exclude 'processed' --exclude '*.png' --exclude '*.pickle' --exclude '*.jpg' -L -avzh -e ssh zoh22914@ssh.diamond.ac.uk:{path_to_remote_dir}/ {path_to_loca_dir}"


@dataclass()
class RsyncPanDDADirs:
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
        return RsyncPanDDADirs(
            command.format(
                path_to_remote_dir=path_to_remote_dir,
                path_to_loca_dir=path_to_local_dir,
                password=password,
            )
        )


command_aws: str = "rsync --progress --exclude 'autoprocessing' --exclude 'dimple' --exclude " \
                   "'jpg' --exclude '*.map' --exclude 'processed' --exclude '*.png' --exclude '*.pickle' --exclude " \
                   "'*.jpg' -L -avzh -e ssh {path_to_local_dir} mx-static-highmem-htcondor-submit.diamond.ac.uk:{path_to_remote_dir}/"

@dataclass()
class RsyncPanDDADirsToAWS:
    # def __init__(self):
    command: str = command_aws

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
            # password: str,
    ):
        return RsyncPanDDADirs(
            command_aws.format(
                path_to_remote_dir=path_to_remote_dir,
                path_to_local_dir=path_to_local_dir,
                # password=password,
            )
        )


command: str = "sshpass -p '{password}' rsync --progress -L -avzh -e ssh {path_to_local_dir} zoh22914@ssh.diamond.ac.uk:{path_to_remote_dir}/"


@dataclass()
class RsyncDirToDiamond:
    command: str

    def run(self):
        p = subprocess.Popen(
            self.command,
            shell=True,
        )

        p.communicate()

    @staticmethod
    def from_paths(
            path_to_local_dir: Path,
            path_to_remote_dir: Path,
            password: str,
    ):
        return RsyncPanDDADirs(
            command.format(
                path_to_remote_dir=path_to_remote_dir,
                path_to_local_dir=path_to_local_dir,
                password=password,
            )
        )



@dataclass()
class RsyncFileToDiamond:
    command: str

    def run(self):
        p = subprocess.Popen(
            self.command,
            shell=True,
        )

        p.communicate()

    @staticmethod
    def from_paths(
            path_to_local_dir: Path,
            path_to_remote_dir: Path,
            password: str,
    ):
        command: str = "sshpass -p '{password}' rsync --progress -L -avzh -e ssh {path_to_local_file} zoh22914@ssh.diamond.ac.uk:{path_to_remote_file}"

        return RsyncPanDDADirs(
            command.format(
                path_to_remote_dir=path_to_remote_dir,
                path_to_local_dir=path_to_local_dir,
                password=password,
            )
        )
