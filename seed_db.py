from run_config import run_config

CONFIGS = ['config/teams_and_rosters.yaml', 'config/player_atbat_schedule.yaml']
DB_CONFIG = 'local_db.yaml'
DB_MAP = 'db_config.yaml'


for CONFIG in CONFIGS:
    print('Running ' + CONFIG)
    run_config(CONFIG=CONFIG,
               DB_CONFIG=DB_CONFIG,
               DB_MAP=DB_MAP)



