import yaml
import os
import json
import pandas as pd
import numpy as np
from jsonpath_ng import jsonpath, parse

class transform_step:

    def __init__(self, config ,staging_dir = 'staging'):

        self.config_object=config
        self.config=config.config
        self.schemas=config.schemas
        self.endpoints=self.config['include'].keys()

        with open('config/api.yaml') as file:
            self.api_map = yaml.load(file, Loader=yaml.FullLoader)

        self.engine=config.engine

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
        self.files={}

        self.staging_dir = staging_dir

        if not os.path.isdir(self.staging_dir):
            os.mkdir(staging_dir)

        self.staged_files = {}

        self.run()

    def map_sql_to_numpy(self, col):

        type_map = {
            "character varying":self.convert_var_char,
            "bit": self.convert_var_char,
            "text": self.convert_var_char,
            "integer": self.convert_integer,
            "float": self.convert_float,
            "boolean": self.convert_boolean,
            "date": self.convert_date,
            "timestamp": self.convert_timestamp,
            "timestamp without time zone": self.convert_timestamp,
            "double precision": self.convert_float
        }

        return type_map.get(col)

    def convert_var_char(self, df, col):

        #df[col] = df[col].str.encode('latin-1')
        df[col] = df[col].astype(str)

        return df

    def convert_integer(self, df, col):

        try:
            df[col] = df[col].astype(int)
        except:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        #df[col] = df[col].fillna(None)

        return df

    def convert_float(self, df, col):

        df[col] = df[col].replace('', np.nan).astype(float)
        #df[col] = df[col].replace({pd.np.nan: None})

        return df

    def convert_boolean(self, df, col):

        df[col] = df[col].astype(bool)

        return df

    def convert_date(self, df, col):

        df[col] = df[col].astype(np.datetime64)
        df[col] = df[col].replace({pd.np.nan: None})

        return df

    def convert_timestamp(self, df, col):

        df[col] = df[col].replace('', '12:00:00 PM')
        df[col] = df[col].astype(np.datetime64)
        df[col] = df[col].replace({pd.np.nan: None})

        return df

    def convert_all_data_type(self,df, schema):

        for col_name, data_type in zip(schema['column_name'], schema['data_type_long']):
            print(col_name)
            print(data_type)

            dtype = schema.loc[schema['column_name'] == col_name.lower()]['data_type_long'].values[0]

            print(dtype)

            cast_column = self.map_sql_to_numpy(dtype)

            df = cast_column(df, col_name)

        return(df)


    def get_al_files(self):

        for ep in self.endpoints:
            files = os.listdir(self.config['output']['dir'] +\
                               self.config['output']['locations'][ep])

            self.files[ep] = files

    def collapse_files_to_csv(self, endpoint):

        if self.api_map['apiMap'][endpoint]['format'] == 'json':
            jsonpath_expr = parse(self.api_map['apiMap'][endpoint]['json_path'])
            all_data = []
            for file in self.files[endpoint]:
                with open(self.config['output']['dir'] + '/' + endpoint + '/' + file) as f:
                    data = json.load(f)
                    data = [match.value for match in jsonpath_expr.find(data)]
                    all_data.append(data)

            dfs = [pd.json_normalize(item) for item in all_data]
            df = pd.concat(dfs)
            new_cols = {col:col.lower().replace('.','_') for col in df.columns.values}
            df = df.rename(columns = new_cols)
            df = self.convert_all_data_type(df, schema=self.schemas[endpoint])

        else:
            dfs = [pd.read_csv(file) for file in self.files['endpoint']]
            df = pd.concat(dfs)

        file_name = self.staging_dir + '/' + endpoint + '.csv'

        df.to_csv(file_name, sep='\t', index=False, na_rep=None, encoding = 'latin-1')

        self.staged_files[endpoint] = file_name

    def run(self):

        self.get_al_files()

        for ep in self.endpoints:
            print(ep)
            self.collapse_files_to_csv(ep)