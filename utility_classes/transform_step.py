import yaml
import os
import json
import pandas as pd
from jsonpath_ng import jsonpath, parse

class transform_step:

    def __init__(self, config, staging_dir = 'staging'):

        self.config=config
        self.endpoints=config['include'].keys()

        with open('config/api.yaml') as file:
            self.api_map = yaml.load(file, Loader=yaml.FullLoader)

        self.files=None

        self.staging_dir = staging_dir
        os.mkdir(staging_dir)

        self.transform_files()

    def get_al_files(self):

        for ep in self.endpoints:
            files = os.listdir(self.config['output']['dir'] +\
                               self.config['output'][ep])

            self.files[ep] = files

    def collapse_files_to_csv(self, endpoint):

        jsonpath_expr = parse(self.api_map['api_map'][endpoint]['format']['json_path'])
        all_data = []
        for file in self.files[endpoint]:
            with open(self.config['output']['dir'] + file) as f:
                data = json.load(f)
                data = [match.value for match in jsonpath_expr.find(data)]
                all_data.append(data)

        dfs = [pd.json_normalize(item) for item in all_data]
        df = pd.concat(dfs)
        df.to_csv(self.staging_dir + '/' + endpoint)

    def transform_files(self):

        for ep in self.endpoints:
            self.collapse_files_to_csv(ep)