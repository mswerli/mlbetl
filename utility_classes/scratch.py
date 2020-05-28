from utility_classes.config_class import config_class
from utility_classes.extract_step import extract_step
from utility_classes.transform_step import transform_step

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
                  password=db_config['password'])

cursor = conn.cursor()


config_object = config_class(config_file = 'config/seed_db.yaml',
                             db_config = 'do_db.yaml')


file = 'temp_data/schedule/http:__statsapi.mlb.com_api_v1_schedule_games_?sportId=1&season=2019&startDate=2019-03-01&endDate=2019-10-05'

jsonpath_expr = parse('dates[*].games[*]')
all_data = []
for file in files:
    with open(file) as f:
        data = json.load(f)
        data = [match.value for match in jsonpath_expr.find(data)]
        all_data.append(data)

dfs = [pd.json_normalize(item) for item in all_data]
df = pd.concat(dfs).set_index(df.columns[0])

df.to_csv(endpoint + '.csv')

f = open('schedule.csv')
next(f)
cursor.copy_from(f, table='league.schedule', sep='\t', null='None')
self.conn.commit()
