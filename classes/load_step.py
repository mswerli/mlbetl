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

        with open('config/global/api.yaml') as file:
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

    def refresh_missing_games(self):

        self.engine.execute("REFRESH MATERIALIZED VIEW league.missing_games")

    def update_control_table(self, table):

        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        values = {"table_name":table, "ts":now}
        query = """
        INSERT INTO league.control_table VALUES ('{table_name}','{ts}')
        """
        self.engine.execute(query.format(**values))

    def run_sql_statements(self):

        sql_files = [self.config['sql_statements']['dir'] + '/' + file for file in self.config['sql_statements']['scripts']]
        cursor = self.conn.cursor()

        for file in sql_files:
            fd = open(file, 'r')
            sqlFile = fd.read()
            fd.close()
            sql_commands = sqlFile.split(';')

            for command in sql_commands:
                print(command)
                cursor.execute(command)

    def remove_rows_in_date_range(self, date_col, min_date, endpoint, max_date=None):

        if max_date is None:
            max_date = datetime.datetime.today()

        replace_vals = self.table_map['tables'][endpoint] \
            .update({'min_date': min_date, 'max_date': max_date, 'date_col': date_col})

        query = "DELETE FROM {schema}.{table} WHERE {date_col} between {min_date} and {max+date}". \
            format(**replace_vals)

        self.engine.execute(query)

    def remove_overlapping_rows(self, endpoint):

        cursor = self.conn.cursor()

        table = self.table_map['tables'][endpoint]['schema'] + '.' + self.table_map['tables'][endpoint]['table']
        replace_vals = {
            "table" :  table ,
            "temp_table" : table + '_temp',
            "id" : self.table_map['tables'][endpoint]['key']
        }

        print(replace_vals)

        query = """
         with real_table as (
            select {id} as id from {table}
            ),
            temp_table as (
            select {id} as id from {temp_table}
            ),
            overlap as (
            select distinct a.id as id from real_table a
            left join temp_table b
            on a.id = b.id
            )
         delete from {table} where {id} in (select id from overlap);
         insert into {table} (select * from {temp_table});
         drop table {temp_table}
        """
        cursor.execute(query.format(**replace_vals))
        self.conn.commit()

    def copy_file_to_table(self, endpoint):

        cursor = self.conn.cursor()
        table = self.table_map['tables'][endpoint]['schema'] + '.' + self.table_map['tables'][endpoint]['table']
        if self.config['load'][endpoint] == 'upsert':
            self.engine.execute("DROP TABLE IF EXISTS {table}_temp; CREATE TABLE {table}_temp (LIKE {table} including all)".format(**{"table":table}))
            table = table + '_temp'

        print(table)

        f = open(self.files[endpoint], 'rb')
        cols = f.readline()
        cols=str(cols).replace('\\t', ',').replace("b'", '').replace('\\n', '').replace("'", "")
        if self.table_map['tables'][endpoint]['encoding'] != 'UTF8':
            f = codecs.EncodedFile(f, "UTF8", self.table_map['tables'][endpoint]['encoding'])
        vals = {"table": table,
                "cols": cols}

        copy_sql = """
                   COPY {table} ({cols}) FROM stdin WITH CSV HEADER
                   DELIMITER as '\t'
                   """.format(**vals)

        print(copy_sql)

        with open(self.files[endpoint], 'r') as f:
            cursor.copy_expert(sql=copy_sql, file=f)

        self.conn.commit()
        cursor.close()

        if self.config['load'][endpoint] == 'upsert':
            self.remove_overlapping_rows(endpoint)

        self.update_control_table(table)

    def run(self):
        for ep in self.config['transform']['types']:
            if self.config['load'][ep] == 'truncate':
                self.truncate_table(ep)

            self.copy_file_to_table(ep)
        if 'sql_statements' in self.config.keys():
            self.run_sql_statements()

        self.refresh_missing_players()
        self.refresh_missing_games()





