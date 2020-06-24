from classes.config_class import config_class
from classes.extract_step import extract_step
from classes.transform_step import transform_step
from classes.load_step import load_step


def run_config(CONFIG, DB_CONFIG, DB_MAP):
    config_object = config_class(config_file = CONFIG ,
                                 db_config = DB_CONFIG,
                                 tables = DB_MAP)

    extract = extract_step(config = config_object)
    transform = transform_step(config = config_object)
    config_object.get_staged_files('staging')
    load = load_step(config = config_object,
                     db_config=DB_CONFIG)