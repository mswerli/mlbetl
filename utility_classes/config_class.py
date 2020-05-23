import yaml
import psycopg2 as pg
import sqlalchemy as sql
from utility_classes.parmeter_constructor  import parameter_constructor
import time
import requests
import os
import json
import datetime


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

        self.endpoints =  list(self.config['include'].keys())

        self.parameters = None

        self.urls = {ep:[] for ep in self.endpoints}

    def get_missing_player_ids(self):

        ##Used to get players who are on rosters but not in the players table
        ##Values will be used if All is indicated for player ids in config file

        query = """select player_id from rosters.missing_players"""
        res = self.engine.execute(query)
        player_ids = [a[0] for a in res.fetchall()]

        return player_ids

    def get_team_ids(self, id_type='team_id'):

        ##Used to substitute All values for team_id in config files

        with open(r'config/teams.yaml') as file:
            teams = yaml.load(file, Loader=yaml.FullLoader)

        team_ids = [team[id_type] for team in teams]

        return team_ids

    def get_parameters(self, endpoint):

        ##For an endpoint, get all parameters

        parameters = self.config['include'][endpoint]

        for key in parameters.keys():
            if type(parameters.get(key))  != list:
                parameters.update({key: [parameters.get(key)]})

        needs_replacement = [k for k in parameters.keys() if parameters.get(k) in [['All'],['ALL']]]

        if 'team' in needs_replacement:
            id_type = self.constructor.endpoints['apiMap'][endpoint]['team']
            parameters['team'] = self.get_team_ids(id_type)

        if 'player_id' in needs_replacement:
            parameters['player_id'] = self.get_missing_player_ids()

        if 'dates' in self.constructor.endpoints['apiMap'][endpoint].keys():
            date_params = self.constructor.endpoints['apiMap'][endpoint]['dates']['params']
            if all([ k in date_params for k in parameters.keys()]):
                dates = []
                for p in date_params:
                    dates.append(parameters.get(p))
                dates.sort()

                if  self.constructor.endpoints['apiMap'][endpoint]['dates']['format'] == 'interval':
                    parameters[date_params[0]], parameters[date_params[1]] = self.constructor.\
                        build_date_interval(start=dates[0][0], end=dates[1][0], days=30)

                if  self.constructor.endpoints['apiMap'][endpoint]['dates']['format'] == 'range':
                    self.constructor.build_date_range(start=dates[0][0], end=dates[1][0])

        output_parameters = []
        for key in parameters.keys():
            output_parameters.append([{key:str(v)} for v in parameters[key]])

        params = self.constructor.create_request_params(output_parameters,
                                                        endpoint)

        return params

    def generate_urls(self):

        ##generate all urls needed to make required requests


        for ep in self.endpoints:
            print(ep)

            parameters = self.get_parameters(endpoint=ep)
            api = self.constructor.endpoints['apiMap'][ep]['api']
            base_url = self.constructor.endpoints[api]['base']
            path =  self.constructor.endpoints[api]['endpoints'][ep]['template']
            urls = [base_url +  path + '&'+ params for params in parameters]

            self.urls[ep].append(urls)


    def make_requests(self, endpoint, zzz=0):

        ##Make requests and save files

        for url in self.urls[endpoint]:

            response = requests.get(url)
            file_name = url.replace('/','_')

            if response.status_code != 200 & zzz != 0:
                time.sleep(zzz)
                response = requests.get(url)

                if response.status_code != 200:
                    raise Exception(url + ': Request failed' + response.status_code)

            self.write_file(endpoint, response.content, file_name)
            time.sleep(zzz)

    def write_file(self, endpoint, raw_data, file_name):

        dir_exists = os.path.isdir(self.config['output'][self.file_store]['dir'] + self.table)
        format = self.endpoints['apiMap'][endpoint]['format']

        if not dir_exists:
            os.mkdir(self.config['output'][self.file_store]['dir']+self.table)

        if format == 'json':
            with open(file_name, 'w') as outfile:
                    json.dump(raw_data, outfile)

        elif format == 'csv':
            decoded_content = raw_data.content.decode('utf-8')
            with open(file_name, 'w', newline='\n') as f:
                for line in decoded_content.splitlines():
                    f.writelines(line + '\n')
        else:
            raise Exception(format + " format not supported")


















