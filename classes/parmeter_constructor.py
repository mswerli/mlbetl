import yaml
import itertools
import datetime


class parameter_constructor:

    ##Class for generating lists of parameter dictionaries for substituting into request urls
    ##Uses itertools to make all unique combinations of a give number of lists

    def __init__(self, endpoints = 'config/api.yaml'):

        with open(endpoints) as file:
            self.endpoints = yaml.load(file, Loader=yaml.FullLoader)

        self.output_params = []


    def create_request_params(self, params, endpoint):

        ##Take a dictionary of dictironaries and generate all combinations of items
        ##Results in list of dictionaries
        ##Each dict is a set of a parameters for a request
        ##Checks to make sure all required parameters are present in each dict

        api = self.endpoints['apiMap'][endpoint]['api']
        endpoint_config = self.endpoints[api]['endpoints'][endpoint]
        required_params = endpoint_config['params']

        all_params = []
        for combination in itertools.product(*params):
            temp = [a for a in combination]
            new_dict = dict(pair for d in temp for pair in d.items())

            param = '&'.join([k + '=' + str(v)\
                              for k, v in zip(new_dict.keys(), new_dict.values())])

            all_params.append(param)


        return all_params

    def build_date_interval(self, start, end, days=30, delim='-'):

        ##Takes a start date, end date, and an interval and creates date intervals
        ##append a list of dictionaries to the classes params method
        ##This should be called before create_request_params
        ##Results in parameter groups which contain start and end date
        ##When configuring api endpoints and params, use start_date and end_date in {}
        ##All dates in conigs should use -. Use delim argument to replace with appropriate delim for api

        duration = (end - start)

        if duration > datetime.timedelta(days):
            print(duration)

            interval_start = start
            date_pairs=[]
            while interval_start < end:
                start_str =  str(interval_start).replace('-',delim)
                end_str = str(interval_start + datetime.timedelta(days)).replace('-',delim)


                date_pairs.append('start_date=' + start_str +'&end_date='+end_str)
                interval_start = interval_start + datetime.timedelta(days=days+1)

            return date_pairs

    def build_date_range(self, start, end, delim):

        ##Takes a start and an end date and generates string representations of each day inbetween
        ##Returs a list of dictionaries all with date as the key
        ##Should be called before create_request_params
        ##Values appened to params attribute for class

        days = datetime.datetime.strptime(end,  "%Y-%m-%d") - \
               datetime.datetime.strptime(start,  "%Y-%m-%d")

        date_range = [{'date': str(datetime.datetime.strptime(start,  "%Y-%m-%d") +datetime.timedelta(days=n)).replace(' 00:00:00','')}\
                      for n in range(0, days.days)]

        return date_range



