import requests
import json
import yaml
import os
import datetime
import time
import random
import urllib
import csv


##Class for genrating API request urls and executing requests
##End points defined in configs/urls
##ids argument can be used to store primary keys requests should be made for

class url_factory:

    def __init__(self, config, file_store='local', ids=None):

        with open(r'config/urls.yaml') as file:
            self.urls = yaml.load(file, Loader=yaml.FullLoader)

        with open(r'config/teams.yaml') as file:
            teams = yaml.load(file, Loader=yaml.FullLoader)

        self.team_ids = [a['team_id'] for a in teams]
        self.team_abbr = [a['abbr'] for a in teams]

        with open(r'config/' + config) as file:
            self.config = yaml.load(file, Loader=yaml.FullLoader)

        self.file_store = file_store
        self.write_to_file_store = self.get_file_store_method(file_store)

        self.files={'team':[],
                     'roster_40':[],
                     'roster_past':[],
                     'player':[],
                     'transaction':[],
                     'atbat': []}

        self.ids = ids

    def _local_file_store_writer(self, raw_data, file_name):
        ##Write Files locally

        dir_exists = os.path.isdir(self.config['output'][self.file_store]['dir']+self.table)

        if not dir_exists:
            os.mkdir(self.config['output'][self.file_store]['dir']+self.table)

        with open(file_name, 'w') as outfile:
                json.dump(raw_data, outfile)

        return file_name

    def _build_param(self, file_type, name, value):
        ##Create url friendly parameters
        ##Parameters come from data type section of config file

        param = "&" + name + "=" + "'" + value + "'"

        return (param)

    def get_file_store_method(self, file_store):
        ##Eventually will support other locations for output files

        if file_store == 'local':
            return(self._local_file_store_writer)

        else:
            raise ValueError(file_store)

    def update_url(self, url_type, params):
        ##For a given request, takes params provided and adds them to base url

        all_params=''

        vals = list(params.values())
        names = list(params.keys())

        for a,b in zip(names,vals):
            all_params= all_params + self._build_param(url_type,a,b)

        url = self.urls['urls'][url_type] + all_params

        return url

    ### Next methods are data type specific
    ### Have not found a way to generalize
    ### Should think about whether to move these to specific classes

    def create_team_urls(self):

        param_dicts = [{'season':a} for a in self.config['include']['team']['season']]
        urls = [self.update_url('team',a) for a in param_dicts]

        return urls

    def create_40_man_urls(self):

        config_ids = self.config['include']['roster_40']['team_id']
        team_ids = self.team_ids if config_ids == ['ALL']  else config_ids
        param_dicts= [{'team_id':a} for a in team_ids]

        urls = [self.update_url('roster_40', a) for a in param_dicts]

        return urls

    def create_transaction_urls(self):
        start_date=self.config['include']['transaction']['start_date']
        end_date=self.config['include']['transaction']['end_date']
        duration = (end_date - start_date)

        if duration > datetime.timedelta(days=30):
            ##Since there are lots of records returned for transactions, we need to chunk requests
            print(duration)

            interval_start = start_date
            param_dicts=[]
            while interval_start < end_date:
                start =  str(interval_start).replace('-','')
                end = str(interval_start + datetime.timedelta(days=30)).replace('-','')

                param_dicts.append({'start_date':start, 'end_date':end})
                interval_start = interval_start + datetime.timedelta(days=31)

        else:
            param_dicts = [{'start_date': str(start_date).replace('-',''), 'end_date': str(end_date).replace('-','')}]

        urls = [self.update_url('transaction', a) for a in param_dicts]

        return urls


    def create_past_roster_urls(self):

        config_ids = self.config['include']['roster_past']['team_id']
        years = list(range(self.config['include']['roster_past']['start_season'],
                      self.config['include']['roster_past']['end_season'] + 1))

        team_ids = self.team_ids if config_ids == ['ALL'] else config_ids

        param_dicts= [[{'team_id':a, 'start_season':str(y), 'end_season':str(y)} for a in team_ids] for y in years]

        urls = [[self.update_url('roster_past', a) for a in d] for d in param_dicts][0]

        return urls

    def make_request(self, url, zzz=0):
        ## Take a url generated above, send request
        ## zzz argument will pause between requests to not overwhelm endpoints

        time.sleep(zzz)
        req = requests.get(url)

        raw_data = req.json()
        file_name = self.config['output'][self.file_store]['dir'] + self.table + "/"+ \
                    ''.join(url.split('?')[1]).replace("'",'') + '.json'

        self.write_to_file_store(raw_data,file_name)

    def make_all_requests(self):

        for url in self.request_urls:
            print(url)
            self.make_request(url)






