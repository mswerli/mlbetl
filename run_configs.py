from run_config import run_config
import os
import yaml

CONFIGS = ['config/daily_config_1.yaml', 'config/daily_config_2.yaml']
DB_CONFIG = 'do_db.yaml'
DB_MAP = 'global/db_config.yaml'


for CONFIG in CONFIGS:
    print('Running ' + CONFIG)
    run_config(CONFIG=CONFIG,
               DB_CONFIG=DB_CONFIG,
               DB_MAP=DB_MAP)