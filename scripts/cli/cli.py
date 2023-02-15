import fire
from pydantic import BaseModel

class Constants:
    database_file:str = "database.sqlite"
    inspect_dir: str = "inspect"
    graph_dir: str = "graphs"
    rmsd_graph_dir: str = "rmsd"


class Options(BaseModel):
    source_dir: str

class CLI:
    def init(self, options_json: str):
        """
        Initialize a directory for PanDDA 2 analysis and the analysis database.
        :param options_json:
        :return:
        """
        options=load_options(options_json)
        database = new_database(options)

    def populate(self, options_json: str):
        """
        Get PanDDAs and their datasources from diamond with ligand bound models and add them to database.
        :param options_json:
        :return:
        """
        options=load_options(options_json)
        database = load_database(options)
        database.populate_panddas()
        database.populate_datasources()

    def process_panddas(self, options_json: str):
        """
        Reprocess each datasource and add the results to the database.
        :param options_json:
        :return:
        """
        options=load_options(options_json)
        database=load_database(options)
        for datasource in database.datasources():
            pandda_result = process_pandda(datasource)
            database.add_new_pandda(pandda_result)

    def analyse_panddas(self, options_json: str):
        """
        Compare each new PanDDA to PanDDAs on the original datasource and add results to database.
        :param options_json:
        :return:
        """
        options = load_options(options_json)
        database = load_database(options)
        for new_pandda, comparator_events in database.new_panddas():
            events = get_events(new_pandda, comparator_events)
            database.add_events(events)

    def build_refining_table(self, options_json: str):
        """
        Make a fake PanDDA dir so that pandda.inspect can be used to look at interesting new events.
        :param options_json:
        :return:
        """
        options = load_options(options_json)
        database = load_database(options)


    def refine(self, options_json: str):
        """
        Use pandda.inspect to annotate interesting new events.
        :param options_json:
        :return:
        """
        options = load_options(options_json)
        database = load_database(options)
        inspect_new_events(options)

    def add_refined_event_annotations(self, options_json: str):
        """
        Add event annotations from refinement to tables.
        :param options_json:
        :return:
        """
        options = load_options(options_json)
        database = load_database(options)

    def graph_rmsds(self, options_json: str):
        """
        Produce graphs of rmsds
        :param options_json:
        :return:
        """
        options = load_options(options_json)
        database = load_database(options)


    # def reanalyse(self, options_json: str):
    #     """
    #
    #     :param options_json:
    #     :return:
    #     """
    #     ...



if __name__ == "__main__":
    fire.Fire(CLI)