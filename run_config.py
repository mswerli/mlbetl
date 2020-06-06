from classes.config_class import config_class
from classes.extract_step import extract_step
from classes.transform_step import transform_step
from classes.load_step import load_step

CONFIG = 'config/seed_db.yaml'
DB_CONFIG = 'do_db.yaml'
DB_MAP = 'db_config.yaml'

config_object = config_class(config_file = CONFIG ,
                             db_config = DB_CONFIG,
                             tables = DB_MAP)

extract = extract_step(config = config_object)
transform = transform_step(config = config_object)
load = load_step(config = transform,
                 db_config=DB_CONFIG)