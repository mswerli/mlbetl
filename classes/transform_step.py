import yaml
import os
import json
import pandas as pd
import numpy as np
import datetime

class transform_step:

    def __init__(self, config ,staging_dir = 'staging'):

        self.config_object=config
        self.config=config.config
        self.schemas=config.schemas
        self.endpoints=config.endpoints

        self.transform_map = self.config_object.transform_map

        self.transform_types = self.config['transform']['types']

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
            "text":self.convert_var_char,
            "jsonb":self.convert_jsonb,
            "json": self.convert_jsonb,
            "bit": self.convert_var_char,
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

    def convert_jsonb(self, df, col):
        vals = [str(json.dumps(item).replace("\t'",'\t//"')) for item in df[col]]
        vals = [val.replace('NaN','{}') for val in vals]
        #df[col] = str(df[col]).replace("'",'\\"')
        df[col] = vals
        return df


    def convert_integer(self, df, col):

        df[col] = df[col].astype(object)
        df[col] = df[col].replace('.0','')
        return df

    def convert_float(self, df, col):
        df[col] = df[col].replace('', np.nan).astype(float)

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

    def convert_all_data_type(self,df, schema, type):

        for col_name, data_type in zip(schema['column_name'], schema['data_type_long']):
            print(col_name)
            print(data_type)

            if col_name != 'added_timestamp':
                if col_name in df.columns:

                    dtype = schema.loc[schema['column_name'] == col_name.lower()]['data_type_long'].values[0]

                    print(dtype)

                    cast_column = self.map_sql_to_numpy(dtype)

                    df = cast_column(df, col_name)
                else:
                   df[col_name] = None

            else:
                df['added_timestamp'] =  datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        return df

    def filter_columns(self, df, endpoint):
        excess_columns = list(set(df.columns.values) - set(self.schemas[endpoint]['column_name']))
        df.drop(excess_columns, axis=1, inplace=True)
        return df

    def get_all_files(self):
        sources = [self.transform_map['types'][type]['source'] for type in self.transform_types]
        sources = set(sources)

        for ep in sources:
            path = self.config['extract']['output']['dir'] + \
                   self.config['extract']['output']['locations'][ep]
            if os.path.isdir(path):
                files = os.listdir(path)
                self.files[ep] = files

    def collapse_files_to_csv(self, type):
        print(self.files.keys())
        source = self.transform_map['types'][type]['source']
        if len(self.files[source]) > 0:
            if self.transform_map['types'][type]['format'] == 'json':
                all_data = []
                if os.path.isdir(self.config['extract']['output']['dir'] + '/' + source):
                    for file in self.files[source]:
                        with open(self.config['extract']['output']['dir'] + '/' + source + '/' + file) as f:
                            data = json.load(f)
                            if 'ids' in self.transform_map['types'][type]['json_path'].keys():
                                ids = {}
                                for id in self.transform_map['types'][type]['json_path']['ids']:
                                    ids.update({id:data[id]})
                            else:
                                ids = None
                            for a in self.transform_map['types'][type]['json_path']['top_level']:
                                data = data[a]
                            record_path=self.transform_map['types'][type]['json_path']['record_path'] if \
                                self.transform_map['types'][type]['json_path']['record_path'] != 'None' else None
                            meta_param= self.transform_map['types'][type]['json_path']['meta'] if \
                                self.transform_map['types'][type]['json_path']['meta'] != 'None' else None
                            data = pd.json_normalize(data, record_path=record_path, meta=meta_param)
                            if ids is not None:
                                for id in ids:
                                    data[id] = ids[id]
                            all_data.append(data)

                    #dfs = [pd.json_normalize(item) for item in all_data]
                    df = pd.concat(all_data)
                    new_cols = {col:col.lower().replace('.','_') for col in df.columns.values}
                    df = df.rename(columns = new_cols)
                    df = self.convert_all_data_type(df, schema=self.schemas[type], type = type)

        else:
            all_data = []
            if os.path.isdir(self.config['extract']['output']['dir'] + '/' + source):
                print(self.files.keys())
                for file in self.files[source]:
                    df = pd.read_csv(self.config['extract']['output']['dir'] + '/' + source + '/' + file)
                    df =self.convert_all_data_type(df, schema=self.schemas[source], type = type)
                    all_data.append(df)
            df = pd.concat(all_data)

        if os.path.isdir(self.config['extract']['output']['dir'] + '/' + source):
            df = self.filter_columns(df, type)
            file_name = self.staging_dir + '/' + type + '.csv'

            df.to_csv(file_name,
                      sep= '\t',
                      index=False,
                      encoding = 'UTF-8')

            self.staged_files[source] = file_name

    def run(self):
        self.get_all_files()
        for ep in list(self.transform_types):
            print(ep)
            self.collapse_files_to_csv(ep)