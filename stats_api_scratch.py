import requests
import yaml
import itertools
import json
import os

with open(r'stats_api.yaml') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

def _build_url(endpoint):

    ##build url for endpoint
    ##result is a url with {} for later substitution

    endpoint_config = config['mlbapi'][endpoint]

    url = config['mlbapi']['base_url'] +  config['mlbapi'][endpoint]['template']

    return url

def _create_request_params(endpoint, params):

    ##Take a dictionary of dictironaries and generate all combinations of items
    ##Results in list of dictionaries
    ##Each dict is a set of a parameters for a request
    ##Checks to make sure all required parameters are present in each dict

    endpoint_config = config['mlbapi'][endpoint]
    required_params = endpoint_config['params']

    all_params = []
    for combination in itertools.product(*params):
        temp = [a for a in combination]
        if len(set(required_params) - set(new_dict.keys())):
            raise Exception('Missing params: {} are required keys'\
                            .format(set(['season']) - set(new_dict.keys())))
        new_dict = dict(pair for d in temp for pair in d.items())

        all_params.append()

    return all_params


def _get(url, params):
    ##adds params to url
    ##makes request and gets response

    url = url.format(**params)

    response = requests.get(url)


def _output_file(response, output_dir):

    file = response.url.replace('/','_').replace('.','_')

    dir_exists = os.path.isdir(output_dir)

    if not dir_exists:
        os.mkdir(output_dir)

    with open(output_dir+'/'+file, 'w') as outfile:
        json.dump(response.json(), outfile)



def get_all()

    return response




