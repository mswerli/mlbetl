import yaml
import time
import requests
import os
import json

class extract_step:

    def __init__(self, config):

        self.config_object = config
        self.config=config.config
        self.endpoints=config.endpoints
        self.urls=config.urls
        with open('config/global/api.yaml') as file:
            self.api_map = yaml.load(file, Loader=yaml.FullLoader)

        self.run()

    def make_requests(self, endpoint, zzz=0):

        ##Make requests and save files

        urls =  self.urls[endpoint][0]
        format = self.api_map['apiMap'][endpoint]['format']

        for pair in urls:
            url = list(pair.keys())[0]
            print(url)

            response = requests.get(url)
            file_name = url.replace('/','_')

            if response.status_code != 200 & zzz != 0:
                time.sleep(zzz)
                response = requests.get(url)

                if response.status_code != 200:
                    raise Exception(url + ': Request failed' + response.status_code)
            if format == 'json':
                raw_data = response.json()
                if list(pair.values())[0] is not None:
                    raw_data.update(list(pair.values())[0])
            elif format == 'csv':
                raw_data = response.content.decode('utf-8')
            else:
                raise Exception(format + " format not supported")

            self.write_file(endpoint, raw_data, file_name)
            time.sleep(zzz)

    def write_file(self, endpoint, raw_data, file_name):

        path = self.config['extract']['output']['dir'] + \
               self.config['extract']['output']['locations'][endpoint]

        dir_exists = os.path.isdir(path)
        format = self.api_map['apiMap'][endpoint]['format']
        print(path)

        if not dir_exists:
            os.mkdir(path)

        if format == 'json':
            with open(path + '/'+ file_name, 'w') as outfile:
                    json.dump(raw_data, outfile)

        elif format == 'csv':
            with open(path + '/' + file_name, 'w', newline='\n') as f:
                for line in raw_data.splitlines():
                    f.writelines(line + '\n')

    def run(self):

        for endpoint in self.endpoints:
            print(endpoint)
            self.make_requests(endpoint,
                               zzz = self.api_map['apiMap'][endpoint]['sleep'])
