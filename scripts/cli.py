import json

from prompt_toolkit.shortcuts import button_dialog

import mongo_populate

if __name__ == "__main__":

    config_file = "cli.json"

    with open(config_file, "r") as f:
        config = json.load(f)

    functions = {
        "mongo_populate": lambda: mongo_populate.main(
            config["mongo_populate"]["model_dirs"],
            config["mongo_populate"]["pandda_dirs"],
        )
    }

    f = button_dialog(
        title='Button dialog example',
        text='Do you want to confirm?',
        buttons=[
            [(name, name) for name in config]
        ],
    ).run()

    f()