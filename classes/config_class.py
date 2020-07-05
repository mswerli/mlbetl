import yaml
import sqlalchemy as sql
import pandas as pd
from classes.parmeter_constructor  import parameter_constructor


class config_class:

    def __init__(self, config_file, db_config, tables):
        print(config_file)
        print(db_config)
        print(tables)

        with open(config_file) as file:
            self.config = yaml.load(file, Loader=yaml.FullLoader)

        with open(r'config/' + db_config) as file:
            db_config = yaml.load(file, Loader=yaml.FullLoader)

        with open(r'config/' + tables) as file:
            self.table_map = yaml.load(file, Loader=yaml.FullLoader)

        with open('config/global/transform.yaml') as file:
            self.transform_map = yaml.load(file, Loader=yaml.FullLoader)

        self.staged_files = {}

        self.engine=sql.create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}"\
            .format(db_config['user'],
                    db_config['password'],
                    db_config['host'],
                    db_config['port'],
                    db_config['database']))

        self.col_type_query = """                              
                            SELECT 
                                column_name, 
                                data_type, 
                                ordinal_position,
                                character_maximum_length,
                                numeric_precision,
                                udt_name,
                                is_nullable
                            FROM information_schema.columns
                            WHERE table_schema='{}' and table_name='{}'
                        """

        self.schemas = {}

        self.constructor = parameter_constructor(endpoints = 'config/global/api.yaml')

        self.endpoints =  list(self.config['extract']['sources'].keys())

        self.parameters = None

        self.urls = {ep:[] for ep in self.endpoints}

        self.run_config()

    def get_staged_files(self, staging_dir):

        for file in self.config['transform']['types']:
            self.staged_files[file] = staging_dir + '/' + file + '.csv'

    def get_missing_player_ids(self):

        ##Used to get players who are on rosters but not in the players table
        ##Values will be used if All is indicated for player ids in config file

        query = """select player_id from rosters.missing_players"""
        res = self.engine.execute(query)
        player_ids = [a[0] for a in res.fetchall()]

        return player_ids

    def get_missing_game_ids(self):

        ##Used to get players who are on rosters but not in the players table
        ##Values will be used if All is indicated for player ids in config file

        query = """select game_pk from league.missing_games"""
        res = self.engine.execute(query)
        game_ids = [a[0] for a in res.fetchall()]

        return game_ids

    def get_schema(self):

        for endpoint in self.config['transform']['types']:

            query = self.col_type_query.format(self.table_map['tables'][endpoint]['schema'],
                                               self.table_map['tables'][endpoint]['table'])

            schema = self.engine.execute(query)\
                .fetchall()

            schema=pd.DataFrame.from_dict(schema) \
                .rename(columns={0:'column_name',
                         1:'data_type_long',
                         2:'ordinal_position',
                         3:'character_length',
                         4:"numeric_precision",
                         5:"data_type_short",
                         6:"is_nullable"})

            self.schemas[endpoint] = schema

    def encode_hfsea(self, season):
        ##the savant api has expects %7C to be appended to the season parameter
        ##the simplest way of handling his is explicitly

        season = str(season) + '%7C'

        return season

    def get_team_ids(self, id_type='team_id'):

        ##Used to substitute All values for team_id in config files

        with open(r'config/global/teams.yaml') as file:
            teams = yaml.load(file, Loader=yaml.FullLoader)

        team_ids = [team[id_type] for team in teams]

        return team_ids

    def get_parameters(self, endpoint, param_type='params'):

        ##For an endpoint, get all parameters

        parameters = self.config['extract']['sources'][endpoint][param_type]

        for key in parameters.keys():
            if type(parameters.get(key))  != list:
                parameters.update({key: [parameters.get(key)]})

            if key == 'hfSea':
                ##This is an obnoxious one off, so I'm not going to solve for it beyond an if statement
                parameters['hfSea'] = [self.encode_hfsea(sea) for sea in  parameters['hfSea']]

        needs_replacement = [k for k in parameters.keys() if parameters.get(k) in [['All'],['ALL']]]

        if 'team' in needs_replacement or 'team_id' in needs_replacement:
            team_param = 'team_id' if 'team_id' in needs_replacement else 'team'
            id_type = self.constructor.endpoints['apiMap'][endpoint]['team']
            parameters[team_param] = self.get_team_ids(id_type)

        if 'player_id' in needs_replacement:
            parameters['player_id'] = self.get_missing_player_ids()

        if 'game_pk' in needs_replacement:
            parameters['game_pk'] = self.get_missing_game_ids()

        dates = None
        if 'dates' in self.constructor.endpoints['apiMap'][endpoint].keys():
            date_params = self.constructor.endpoints['apiMap'][endpoint]['dates']['params']
            if all([ k in date_params for k in parameters.keys()]):
                dates = []
                for p in date_params:
                    dates.append(parameters.get(p))
                dates.sort()

                del parameters[date_params[0]], parameters[date_params[1]]

                if  self.constructor.endpoints['apiMap'][endpoint]['dates']['format'] == 'interval':
                    delim = self.constructor.endpoints['apiMap'][endpoint]['dates']['delim']
                    dates = self.constructor. \
                        build_date_interval(start=dates[0][0], end=dates[1][0], days=30, delim=delim)

                if  self.constructor.endpoints['apiMap'][endpoint]['dates']['format'] == 'range':
                    self.constructor.build_date_range(start=dates[0][0], end=dates[1][0])

        output_parameters = []
        for key in parameters.keys():
            output_parameters.append([{key: str(v)} for v in parameters[key]])

        params = self.constructor.create_request_params(output_parameters,
                                                        endpoint,
                                                        param_type)
        if dates is not None:
            if params != ['']:
                params = [[param + date for param in params] for date in dates]
            else:
                params = dates

        return params

    def find_path_parameters(self, endpoint):

        path_params = set(self.config['extract']['sources'][endpoint]['pathParams'].keys())
        required_params = set(self.constructor.endpoints['apiMap'][endpoint].keys())
        if (required_params - path_params) == set():
            return self.config['extract']['sources'][endpoint]['pathParams']
        else:
            missing_parameters = list(required_params - path_params)
            raise Exception('Required parameters missing: ' + ','.join(missing_parameters))

    def generate_urls(self):
        ##generate all urls needed to make required requests
        for ep in self.endpoints:
            api = self.constructor.endpoints['apiMap'][ep]['api']
            base_url = self.constructor.endpoints[api]['base']
            path =  self.constructor.endpoints[api]['endpoints'][ep]['template']
            url = base_url +  path
            if 'params' in self.constructor.endpoints[api]['endpoints'][ep].keys():
                parameters = self.get_parameters(endpoint=ep)
                urls = [url + '&'+ params for params in parameters]
            else:
                urls = url
            if 'pathParams' in self.constructor.endpoints[api]['endpoints'][ep].keys():
                path_params = self.get_parameters(ep, param_type='pathParams')
                urls = [url.format(**param) for param in path_params]
                urls = [{url:path_param} for url,path_param in zip(urls,path_params)]
            else:
                urls = [{url:None} for url in urls]

            self.urls[ep].append(urls)

    def run_config(self):

        self.generate_urls()
        self.get_schema()

