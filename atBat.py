from utility_classes.postgres_table import postgres_table
from utility_classes.url_factory import url_factory
from utility_classes.file_reader import file_reader
import urllib
import time
import requests
import os


class atBat(postgres_table, url_factory, file_reader):

    def __init__(self, config, table,db_config, file_store='local', ):

        url_factory.__init__(self,config = config)
        postgres_table.__init__(self,table=table, db_config=db_config)
        file_reader.__init__(self,config = config, file_store = file_store)

        self.base_url = self.urls['urls']['atbat']

        self.request_urls=self.create_all_urls()
        self.steps = self.config['steps']['atbat']

    def _build_param(self,name, value):

        param = {name: value}

        return param

    def update_url(self, url_type, params):

        all_params={}

        vals = list(params.values())
        names = list(params.keys())

        for a,b in zip(names,vals):
            print(a)
            print(b)
            all_params.update(self._build_param(a,b))

        url = self.urls['urls'][url_type].format( **all_params)
        url_parts = urllib.parse.parse_qs(url)

        url_keys = list(url_parts.keys())

        url_prefix = url_parts[url_keys[0]]
        url_prefix =  urllib.parse.urlunparse(components = url_prefix)

        del url_parts[url_keys[0]]

        url_query = urllib.parse.urlencode(url_parts)

        url = url_prefix + url_query

        return url

    def make_request(self, url,  url_type = 'atbat', zzz=20):

        time.sleep(zzz)
        req = requests.get(url)

        print(req.status_code)

        if req.status_code != 200:
            print('Sleeping for 5 mins')
            time.sleep(300)
            req = requests.get(url)

        file_name = self.config['output'][self.file_store]['dir'] + url_type + "/" + \
                    ''.join(url.split('?')[1]).replace("'", '') + '.csv'

        self.write_to_file_store(url_type, req, file_name)

    def _local_file_store_writer(self, url_type, raw_data, file_name):
        dir_exists = os.path.isdir(self.config['output'][self.file_store]['dir'] + url_type)

        if not dir_exists:
            os.mkdir(self.config['output'][self.file_store]['dir'] + url_type)

        decoded_content = raw_data.content.decode('utf-8')
        with open(file_name, 'w', newline='\n') as f:
            for line in decoded_content.splitlines():
                f.writelines(line + '\n')

    def add_seasons(self, season, params):

        for param in params:
            param.update(self._build_param("hfSea",season ))

        return param


    def create_all_params(self):

        params = []

        teams = self.team_abbr if self.config['include']['atbat']['team'] == ['All'] \
            else self.config['include']['atbat']['team']

        seasons = self.config['include']['atbat']['hfSea']

        greater_than = self.config['include']['atbat']['game_date_gt']
        less_than = self.config['include']['atbat']['game_date_lt']

        params = [[{"team": team, "hfSea": str(season) + '%7C'} for team in teams]\
                  for season in seasons][0]

        return params

    def create_all_urls(self):

        params = self.create_all_params()

        greater_than = self.config['include']['atbat']['game_date_gt']
        less_than = self.config['include']['atbat']['game_date_lt']

        urls = []

        for param in params:

            param.update({"game_date_gt": greater_than,
                          "game_date_lt": less_than})

            urls.append(self.base_url.format( ** param))

        return urls

    def make_request(self, url):

        req = requests.get(url)

        if req.status_code != 200:
            print('Sleeping for 5 mins')
            time.sleep(300)
            req = requests.get(url)

        file_name = self.config['output'][self.file_store]['dir'] + 'atbat' + "/" + \
                    ''.join(url.split('hfSea')[1]).replace("'", '').split('position')[0]\
                        .replace("'", '') + '.csv'

        self.write_to_file_store(url_type='atBat',
                                 raw_data=req,
                                 file_name=file_name)




    def populate_table(self, load_method='append', load_type='copy', min_date=None, max_date=None):

       if load_type == 'reload':
           self.truncate_table()

       if load_type == 'date_range':
           self.remove_rows_in_date_range(date_col = 'game_date',
                                           min_date = min_date,
                                           max_date = max_date)

       df = self.read_all_type_files('atbat')

       self.transform_and_load(df, load_method=load_method,
                                       load_type=load_type)

    def execute_steps(self):

        if 'extract' in self.steps.keys():
            self.make_all_requests()

        if 'load' in self.steps.keys():
            self.populate_table(load_type=self.steps['load']['load_type'],
                                load_method=self.steps['load']['load_method'])






