import yaml
import time
import requests
import os
import json

class extract_step:

    def __init__(self, config):

        self.config_object = config
        self.config=config.config
        self.endpoints=self.config['include'].keys()
        self.urls=config.urls
        with open('config/api.yaml') as file:
            self.api_map = yaml.load(file, Loader=yaml.FullLoader)

        self.run()

    def make_requests(self, endpoint, zzz=0):

        ##Make requests and save files

        urls =  self.urls[endpoint][0]

        for url in urls:
            print(url)

            response = requests.get(url)
            file_name = url.replace('/','_')

            if response.status_code != 200 & zzz != 0:
                time.sleep(zzz)
                response = requests.get(url)

                if response.status_code != 200:
                    raise Exception(url + ': Request failed' + response.status_code)

            self.write_file(endpoint, response, file_name)
            time.sleep(zzz)

    def write_file(self, endpoint, raw_data, file_name):

        path = self.config['output']['dir'] +  self.config['output']['locations'][endpoint]

        dir_exists = os.path.isdir(path)
        format = self.api_map['apiMap'][endpoint]['format']
        print(path)

        if not dir_exists:
            os.mkdir(path)

        if format == 'json':
            with open(path + '/'+ file_name, 'w') as outfile:
                    json.dump(raw_data.json(), outfile)

        elif format == 'csv':
            decoded_content = raw_data.content.decode('utf-8')
            with open(path + '/' + file_name, 'w', newline='\n') as f:
                for line in decoded_content.splitlines():
                    f.writelines(line + '\n')
        else:
            raise Exception(format + " format not supported")

    def run(self):

        for endpoint in self.endpoints:
            print(endpoint)
            self.make_requests(endpoint,
                               zzz = self.api_map['apiMap'][endpoint]['sleep'])
