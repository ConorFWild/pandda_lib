from pathlib import Path

import fire
from pymongo import MongoClient

from pandda_lib import constants


def main():
    client = MongoClient()

    mongo_diamond_paths = client[constants.mongo_pandda][constants.mongo_diamond_paths]

    for system in mongo_diamond_paths.find():
        print(system)


if __name__ == "__main__":
    fire.Fire(main)
