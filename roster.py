from utility_classes.file_reader import file_reader
from utility_classes.postgres_table import postgres_table
from utility_classes.archive.url_factory import url_factory
import yaml


class loader(file_reader,postgres_table, url_factory):

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
        self.create_urls = self.pick_url_method()
        self.request_urls = self.create_urls()
        self.steps = self.config['steps'][self.table]

    def pick_url_method(self):

        if self.table == 'team':
           return self.create_team_urls

        elif self.table == 'player':
            return self.create_player_urls

        elif self.table == 'roster_40':
            return self.create_40_man_urls

        elif self.table == 'roster_40':
            return self.create_40_man_urls

        elif self.table == 'roster_past':
            return self.create_past_roster_urls

        elif self.table == 'transaction':
            return self.create_transaction_urls

        else:
            raise NotImplementedError('URL creation not implemented for %s', self.table)

    def populate_table(self, load_type, load_method):
        df = self.read_all_type_files(self.table)
        self.transform_and_load(df, load_type=load_type)

    def execute_steps(self,):
        if 'extract' in self.steps.keys():
            self.make_all_requests()

        if 'load' in self.steps.keys():
            self.populate_table(load_type=self.steps['load']['load_type'],
                                load_method=self.steps['load']['load_method'])






