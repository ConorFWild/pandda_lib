import fire

class CLI:
    def init(self, options_json: str):
        """
        Initialize a directory for PanDDA 2 analysis

        :param options_json:
        :return:
        """
        ...

    def populate(self):
        ...

    def process_panddas(self):
        ...

    def analyse_panddas(self):
        ...

    def build_refining_table(self):
        ...

    def refine(self):
        ...

    def reanalyse(self):
        ...



if __name__ == "__main__":
    fire.Fire(CLI)