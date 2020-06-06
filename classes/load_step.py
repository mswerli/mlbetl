import psycopg2 as pg
import sqlalchemy as sql
import yaml
import datetime
import codecs

class load_step:

    def __init__(self, config, db_config):

        with open(r'config/' + db_config) as file:
            db_config = yaml.load(file, Loader=yaml.FullLoader)

        self.table_map = config.table_map
        self.config = config.config
        self.files = config.staged_files
        self.endpoints = config.endpoints

        with open('config/api.yaml') as file:
            self.api_map = yaml.load(file, Loader=yaml.FullLoader)



        self.engine = sql.create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}" \
                                        .format(db_config['user'],
                                                db_config['password'],
                                                db_config['host'],
                                                db_config['port'],
                                                db_config['database']))

        self.conn = pg.connect(host=db_config['host'],
                               database=db_config['database'],
                               user=db_config['user'],
                               port=db_config['port'],
                               password=db_config['password'])

        self.run()

    def truncate_table(self, endpoint):

        query = "TRUNCATE TABLE {schema}.{table}".format(**self.table_map['tables'][endpoint])

        self.engine.execute(query)

    def refresh_missing_players(self):

        self.engine.execute("REFRESH MATERIALIZED VIEW rosters.missing_players")

    def remove_rows_in_date_range(self, date_col, min_date, endpoint, max_date=None):

        if max_date is None:
            max_date = datetime.datetime.today()

        replace_vals = self.table_map['tables'][endpoint] \
            .update({'min_date': min_date, 'max_date': max_date, 'date_col': date_col})

        query = "DELETE FROM {schema}.{table} WHERE {date_col} between {min_date} and {max+date}". \
            format(**replace_vals)

        self.engine.execute(query)

    def remove_rows_with_id(self, id_col, ids, endpoint):

        cursor = self.conn.cursor()

        ids = tuple(ids)

        replace_vals = self.table_map['tables'][endpoint] \
            .update({'id_col': id_col})

        query = "DELETE FROM {schema}.{table} WHERE {id_col} = %s" \
            .format(**replace_vals)

        cursor.executemany(query, ids)

        self.conn.commit()
        cursor.close()

    def copy_file_to_table(self, endpoint):
        table = self.table_map['tables'][endpoint]['schema'] + '.' + self.table_map['tables'][endpoint]['table']
        cursor = self.conn.cursor()

        f = open(self.files[endpoint], 'rb')
        next(f)
        if self.table_map['tables'][endpoint]['encoding'] != 'UTF8':
            f = codecs.EncodedFile(f, "UTF8", self.table_map['tables'][endpoint]['encoding'])

        cursor.copy_from(f,
                         table=table, sep='\t',
                         null=self.table_map['tables'][endpoint]['null_value'])

        self.conn.commit()

        cursor.close()

    def run(self):
        for ep in self.endpoints:
            if self.config['load'][ep] == 'truncate':
                self.truncate_table(ep)

            self.copy_file_to_table(ep)

        self.refresh_missing_players()




