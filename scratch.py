from utility_classes.config_class import config_class
from utility_classes.extract_step import extract_step
from utility_classes.transform_step import transform_step
import yaml
import sqlalchemy as sql
import psycopg2 as pg
from jsonpath_ng import jsonpath, parse

with open(r'config/' + 'do_db.yaml') as file:
    db_config = yaml.load(file, Loader=yaml.FullLoader)

engine = sql.create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}" \
                            .format(db_config['user'],
                                    db_config['password'],
                                    db_config['host'],
                                    db_config['port'],
                                    db_config['database']))
conn = pg.connect(host=db_config['host'],
                  database=db_config['database'],
                  user=db_config['user'],
                  port=db_config['port'],
                  password=db_config['password'],
                  )
conn.set_client_encoding('LATIN-1')
cursor = conn.cursor()


config_object = config_class(config_file = 'config/seed_db.yaml',
                             db_config = 'do_db.yaml',
                             tables = 'db_config.yaml')

extract = extract_step(config = config_object.config,
                       urls = config_object.urls)


transform = transform_step(config = config_object)




f = open('staging/player.csv', 'rb')
next(f)
cursor.copy_from(f, table='rosters.players', sep='\t', null='')
conn.commit()


file_name = "/Users/morrisswerlick/baseball_database/temp_data/transaction/http:__lookup-service-prod.mlb.com_json_named.transaction_all.bam?sport_code='mlb'&start_date=20170101&end_date=20170131"

jsonpath_expr = parse('roster_team_alltime.queryResults.row[*]'])
with open(file_name) as f:
    data = json.load(f)
    data = [match.value for match in jsonpath_expr.find(data)]
    all_data.append(data)