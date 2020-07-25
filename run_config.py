from classes.config_class import config_class
from classes.extract_step import extract_step
from classes.transform_step import transform_step
from classes.load_step import load_step
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--config', type = str, help='config file')
parser.add_argument('--db_config', type = str, help='file name where db config is stored')
parser.add_argument('--db_map', type = str, default= 'db_config.yaml',
                    help='file name where mappings to db tables are')
args = parser.parse_args()

def run_config(CONFIG, DB_CONFIG, DB_MAP):

    config_object = config_class(config_file = CONFIG ,db_config = DB_CONFIG,tables = DB_MAP)

    extract = extract_step(config = config_object)
    transform = transform_step(config = config_object)
    config_object.get_staged_files('staging')
    load = load_step(config = config_object,
                     db_config=DB_CONFIG)

run_config(CONFIG =args.config , DB_CONFIG = args.db_config, DB_MAP=args.db_map)
run_config(CONFIG ='config/daily_config_1.yaml' , DB_CONFIG = 'do_db.yaml', DB_MAP='global/db_config.yaml')


