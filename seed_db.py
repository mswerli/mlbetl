from classes.config_class import config_class
from classes.extract_step import extract_step
from classes.transform_step import transform_step


config_object = config_class(config_file = 'config/seed_db.yaml',
                             db_config = 'do_db.yaml',
                             tables = 'db_config.yaml')

extract = extract_step(config = config_object)
transform = transform_step(config = extract)
load = load_step(config = config_object)