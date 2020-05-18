from utility_classes.file_reader import file_reader
from utility_classes.postgres_table import postgres_table
from utility_classes.url_factory import url_factory
import yaml

##Class used for handling extraction and loading of player data
##Contains instances of the url_factory, postgres_table, and file_reader
##execute_steps method will run all steps defined in relevant sections of config

class player(file_reader,postgres_table, url_factory):

    def __init__(self, table, config, db_config, file_store):

        url_factory.__init__(self,config = config)
        postgres_table.__init__(self,table=table, db_config=db_config)
        file_reader.__init__(self,config = config, file_store = file_store)

        with open(r'config/urls.yaml') as file:
            self.urls = yaml.load(file, Loader=yaml.FullLoader)

        with open(r'config/' + config) as file:
            self.config = yaml.load(file, Loader=yaml.FullLoader)

        self.file_store = file_store
        self.table = table
        self.existing_ids = self.get_existing_ids()
        self.roster_ids = self.get_roster_ids()
        self.ids = set(self.roster_ids) - set(self.existing_ids)
        self.request_urls = self.create_urls()
        self.steps = self.config['steps'][self.table]

    def create_urls(self, ids=None):

        ids = self.ids if ids is None else ids

        param_dicts= [{'player_id':a} for a in ids]
        urls = [self.update_url('player', a) for a in param_dicts]

        return urls

    def get_roster_ids(self):

       query =  self.id_query.format('player_id', 'rosters','roster_40')
       roster_ids = self.engine.execute(query).fetchall()
       roster_ids = [str(a[0]) for a in roster_ids]

       return roster_ids

    def populate_table(self, load_type, load_method):

        df = self.read_all_type_files(self.table)
        self.transform_and_load(df, load_type=load_type)

    def execute_steps(self):

        if 'extract' in self.steps.keys():
            self.make_all_requests()

        if 'load' in self.steps.keys():
            self.populate_table(load_type=self.steps['load']['load_type'],
                                load_method= self.steps['load']['load_method'])
