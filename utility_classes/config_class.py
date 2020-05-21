import yaml
import psycopg2 as pg
import sqlalchemy as sql
from utility_classes.parmeter_constructor  import parameter_constructor
import time
import requests


class config_class:

    def __init__(self, config_file, db_config):

        with open(config_file) as file:
            self.config = yaml.load(file, Loader=yaml.FullLoader)

        with open(r'config/' + db_config) as file:
            db_config = yaml.load(file, Loader=yaml.FullLoader)

        self.engine = sql.create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}" \
                                        .format(db_config['user'],
                                                db_config['password'],
                                                db_config['host'],
                                                db_config['port'],
                                                db_config['database']))

        self.constructor = parameter_constructor(endpoints = 'config/api.yaml')

        self.endpoints =  self.config['include'].keys()

        self.parameters = None

        self.urls = {ep:'' for ep in self.endpoints}

    def get_missing_player_ids(self):

        ##Used to get players who are on rosters but not in the players table
        ##Values will be used if All is indicated for player ids in config file

        query = """select player_id from rosters.missing_players"""
        res = self.engine.execute(query)
        player_ids = [a[0] for a in res.fetchall()]

        return player_ids

    def get_team_ids(self):

        ##Used to substitute All values for team_id in config files

        with open(r'config/teams.yaml') as file:
            teams = yaml.load(file, Loader=yaml.FullLoader)

        team_ids = [team['team_id'] for team in teams]

        return team_ids

    def get_parameters(self, endpoint):

        parameters = self.config['include'][endpoint]

        needs_replacement = [k for k in parameters.keys() if parameters.get(k) in ['All','ALL']]

        if 'team' in needs_replacement:

            parameters['team'] = self.get_team_ids()

        if 'player_id' in needs_replacement:
            parameters['player_id'] = self.get_missing_player_ids()

        output_parameters = []
        for key in parameters.keys():
            output_parameters.append([{key:v} for v in parameters[key]])

        self.parameters = self.constructor.create_request_params(output_parameters)

    def generate_urls(self):

        ##generate all urls needed to make required requests

        for ep in self.endpoints:

            parameters = self.get_parameters(ep)
            api = self.constructor.endpoints['apiMap'][ep]
            base_url = self.endpoints['api'][ep]['base']

            urls = [base_url + params for params in parameters]

            self.urls[ep].append(urls)

    def make_requests(self, endpoint, zzz=0):

        for url in self.urls[endpoint]:

            response = requests.get(url)

            writer.write_file()

            time.sleep(zzz)


















