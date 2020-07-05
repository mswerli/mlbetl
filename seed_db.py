from run_config import run_config
import argparse
import os
import yaml


parser = argparse.ArgumentParser()

parser.add_argument('--db_config', type = str, help='file name where db config is stored')
parser.add_argument('--db_map', type = str, default= 'db_config.yaml',
                    help='file name where mappings to db tables are')
args = parser.parse_args()


CONFIGS = ['config/seed_db_step_1.yaml','config/seed_db_step_2.yaml']
DB_CONFIG = args.db_config
DB_MAP = args.db_map


for CONFIG in CONFIGS:
    print('Running ' + CONFIG)
    run_config(CONFIG=CONFIG,
               DB_CONFIG=DB_CONFIG,
               DB_MAP=DB_MAP)


    print('Running ' + CONFIG)
    run_config(CONFIG=CONFIG, DB_CONFIG=DB_CONFIG,DB_MAP=DB_MAP)






