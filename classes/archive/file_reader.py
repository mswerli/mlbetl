import json
import yaml
import os
import itertools
import pandas as pd



class file_reader:

    def __init__(self, config, file_store):

        with open(r'config/' + config) as file:
            self.config = yaml.load(file, Loader=yaml.FullLoader)

        with open(r'config/urls.yaml') as file:
            self.urls = yaml.load(file, Loader=yaml.FullLoader)

        self.file_store = file_store

    def read_json(self, file_name):

        with open(file_name) as f:
            data=json.load(f)

        top_key = data.keys()

        if data[list(top_key)[0]]['queryResults']['totalSize'] != '0':
            if data[list(top_key)[0]]['queryResults']['totalSize'] > '1':

                data = data[list(top_key)[0]]['queryResults']['row']
                data = pd.DataFrame.from_dict(data)

            else:
                data = data[list(top_key)[0]]['queryResults']['row']
                vals= [[a] for a in list(data.values())]
                data = pd.DataFrame.from_dict({a:b for a,b in zip(data.keys(), vals)})

            return data

    def read_csv(self, file_name):

        data = pd.read_csv(file_name)
        #drop_cols = list(df.columns.values[[col_indexes]])
        #data = data.drop(columns=drop_cols, axis = 1)
        return data

    def pick_file_reader(self, file_type):

        if self.urls['format'][file_type] == 'csv':
            return self.read_csv
        elif self.urls['format'][file_type] == 'json':
            return self.read_json
        else:
            NotImplementedError("File read method not defined")

    def read_all_type_files(self, file_type):

        files = self.get_all_file_type_paths(file_type)

        all_data=[]

        file_reader = self.pick_file_reader(file_type)

        for f in files:
            print(f)
            all_data.append(file_reader(f))

        df = pd.concat(all_data)

        return df

    def get_all_file_type_paths(self, file_type):

        dir = self.config['output'][self.file_store]['dir']
        path = self.config['output'][self.file_store]['locations'][file_type]
        path = dir + path

        files = os.listdir(path)
        files=[path + a for a in files if a.endswith(self.urls['format'][file_type])]

        return files



