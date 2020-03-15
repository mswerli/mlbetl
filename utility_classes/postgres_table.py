import psycopg2 as pg
import sqlalchemy as sql
import yaml
import pandas as pd
import numpy as np
from sqlalchemy import  Integer, String, Unicode, Boolean, DateTime, Float ,Numeric
import datetime
import csv


class postgres_table:

    def __init__(self, table, db_config):

        with open(r'config/' + db_config) as file:
            db_config = yaml.load(file, Loader=yaml.FullLoader)

        with open(r'config/urls.yaml') as file:
            self.urls = yaml.load(file, Loader=yaml.FullLoader)

        with open(r'config/type_conversion.yaml') as file:
            self.types = yaml.load(file, Loader=yaml.FullLoader)

        with open(r'config/columns/teams.yaml') as file:
            self.columns = yaml.load(file, Loader=yaml.FullLoader)

        self.table = table

        self.engine=sql.create_engine("postgresql+psycopg2://{}:{}@{}/{}"\
            .format(db_config['user'],
                    db_config['password'],
                    db_config['host'],
                    db_config['database']))

        self.conn = pg.connect(host=db_config['host'],
                               database=db_config['database'],
                               user=db_config['user'],
                               password = db_config['password'])

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

        self.primary_key_query = """

                            SELECT
                                tc.constraint_name, 
                                tc.table_name,
                                kcu.column_name, 
                                tc.constraint_schema
                            FROM 
                                information_schema.table_constraints AS tc 
                                JOIN information_schema.key_column_usage AS kcu
                                  ON tc.constraint_name = kcu.constraint_name
                                JOIN information_schema.constraint_column_usage AS ccu
                                  ON ccu.constraint_name = tc.constraint_name
                            WHERE constraint_type = 'PRIMARY KEY' 
                                  and tc.table_name = '{}' 
                                  and tc.constraint_schema='{}'
                                    
                        """

        self.id_query = """
        
            SELECT DISTINCT {} FROM {}.{}
        
        
        """
        self.schema = self.get_schema()
        self.p_key = self.get_contraints()

    def convert_data_types(self, col,col_len):

        type_map = {
            "character varying": String(col_len),
            "bit": String(col_len),
            "integer": Integer(),
            "float": Float(),
            "boolean": Boolean(),
            "timestamp": DateTime()

        }

        return type_map[col]

    def map_sql_to_numpy(self, col):

        type_map = {
            "character varying":np.character,
            "bit": np.character,
            "integer": np.integer,
            "float": np.floating,
            "boolean": np.bool_,
            "date": np.datetime64,
            #"time": datetime.time,
            #"time without time zone": datetime.time,
            "timestamp": np.datetime64
        }

        return type_map.get(col)

    def _del_csv_column(self, row, col_indexes):

        col_indexes.sort()
        col_indexes.reverse

        for i in col_indexes:
            del row[i]

        return tuple(row)


    def convert_all_data_type(self,df):

        for a, b in zip(self.schema['column_name'], self.schema['data_type_long']):

            b = self.schema.loc[self.schema['column_name'] == a]['data_type_long'].values[0]

            if b in ['time', 'time without time zone']:
                df[a] = df[a].replace('', '12:00:00 PM')

            try:
                df[a] = df[a].astype(self.map_sql_to_numpy(b))

            except:
                if b == 'integer':
                    df[a] = pd.to_numeric(df[a], errors = 'coerce')
                    #df[a] = df[a].astype(self.map_sql_to_numpy(b))


        return(df)

    def get_schema(self):

        query = self.col_type_query.format(self.urls['tables'][self.table]['schema'],
                                           self.urls['tables'][self.table]['table'])

        schema = self.engine.execute(query)\
            .fetchall()

        schema=pd.DataFrame.from_dict(schema) \
            .rename(columns={0:'column_name',
                     1:'data_type_long',
                     2:'ordinal_position',
                     3:'character_length',
                     4:"numeric_precision",
                     5:"data_type_short",
                     6:"is_nullable"})


        return schema

    def get_contraints(self):

        query = self.primary_key_query.format(self.urls['tables'][self.table]['table'],
                                              self.urls['tables'][self.table]['schema'])

        p_keys = self.engine.execute(query)\
            .fetchall()

        p_keys = pd.DataFrame.from_dict(p_keys) \
            .rename(columns={0: 'constraint_name',
                             1: 'table_name',
                             2: 'column_name',
                             3: 'constraint_schema'})


        return p_keys

    def get_existing_ids(self, column_name=None):

        if column_name is None:
            query = self.id_query.format(self.p_key['column_name'][0],
                                         self.p_key['constraint_schema'][0],
                                         self.p_key['table_name'][0])
        else:

            query = self.id_query.format(column_name,
                                         self.urls['tables'][self.table]['schema'],
                                         self.urls['tables'][self.table]['table']
                                         )

        results = self.engine.execute(query).fetchall()
        ids = [a.items()[0] for a in results]
        ids =  [a[1] for a in ids]

        return ids

    def extract_new_ids(self,df):

        check_ids = set(df[self.p_key['column_name'][0]])
        present_ids = set(self.existing_ids)

        new_ids = check_ids - present_ids

        df = df[df[self.p_key['column_name'] in new_ids]]

        return df

    def truncate_table(self):

        query = "TRUNCATE TABLE {schema}.{table}".format( **self.urls['tables'][self.table])

        self.engine.execute(query)

    def remove_rows_in_date_range(self, date_col, min_date, max_date=None):

        if max_date is None:
            max_date = datetime.datetime.today()

        replace_vals = self.urls['tables'][self.table]\
            .update({'min_date':min_date,'max_date':max_date, 'date_col':date_col})

        query = "DELETE FROM {schema}.{table} WHERE {date_col} between {min_date} and {max+date}".\
            format( **replace_vals)

        self.engine.execute(query)

    def remove_rows_with_id(self, id_col,ids):

        cursor = self.conn.cursor()

        ids = tuple(ids)

        replace_vals =  self.urls['tables'][self.table]\
            .update({'id_col':id_col})

        query = "DELETE FROM {schema}.{table} WHERE {id_col} = %s"\
            .format( **replace_vals)

        cursor.executemany(query, ids)

        self.conn.commit()
        cursor.close

    def write_to_database(self,df, method='append'):

        print(len(df))

        df.to_sql(self.urls['tables'][self.table]['table'],
                  con= self.engine,
                  schema=self.urls['tables'][self.table]['schema'],
                  if_exists='append',
                  index=False)

    def transform_and_load(self, df, load_method='append', load_type ='copy'):

        print(df.columns)
        df = df[self.schema['column_name']]
        df = self.convert_all_data_type(df)
        df = df.drop_duplicates()

        if load_type == 'copy':
            self.copy_df_to_table(df)
        else:
            self.write_to_database(df, load_method)

    def copy_df_to_table(self, df):

        cursor = self.conn.cursor()

        values = df.to_numpy()

        replace_vals = self.urls['tables'][self.table]
        replace_vals.update({'values':','.join(['%s'] * len(df.columns))})

        query =  "INSERT INTO {schema}.{table} VALUES({values})"\
            .format( **replace_vals)

        cursor.executemany(query, values)
        self.conn.commit()

        cursor.close()

    def copy_file_to_table(self, file, col_indexes = None):

        cursor = self.conn.cursor()

        with open(file, 'r') as f:
            reader = csv.reader(f)
            next(reader)
            if not col_indexes is None:
                data = [self._del_csv_column(line, col_indexes)\
                        for line in reader]
            else:
                data = [tuple(line) for line in reader]

        replace_vals = self.urls['tables'][self.table]
        replace_vals.update({'values':','.join(['%s'] * len(data[0]))})

        query =  "INSERT INTO {schema}.{table} VALUES({values})"\
            .format( **replace_vals)

        cursor.executemany(query, data)
        self.conn.commit()
        cursor.close()










