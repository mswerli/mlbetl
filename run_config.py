from classes.archive.roster import loader
from classes.archive.atBat import atBat
from classes.archive.player import player
import yaml

CONFIG = '2018.yaml'
DB_CONFIG = 'do_db.yaml'
FILE_STORE = 'local'


def run_config():

    with open(r'config/' + CONFIG) as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    objects = list(config['include'].keys())


    if 'atbat' in objects:
        atBat_loader = atBat(config=CONFIG,
                             table='play_by_play',
                             db_config=DB_CONFIG,
                             file_store=FILE_STORE)
        atBat_loader.execute_steps()

    if 'team' in objects:
        team_loader = loader('team', CONFIG, DB_CONFIG, FILE_STORE)
        team_loader.execute_steps()

    if 'rosters_40' in objects:
        rosters_40_loader = loader('roster_40', CONFIG, DB_CONFIG, FILE_STORE)
        rosters_40_loader.execute_steps()

    if 'rosters_past' in objects:
        rosters_past = loader('roster_past', CONFIG, DB_CONFIG, FILE_STORE)
        rosters_past.execute_steps()

    if 'transactions' in objects:
        transactions = loader('transaction', CONFIG, DB_CONFIG, FILE_STORE)
        transactions.execute_steps()

    if 'players' in objects:
        players = player('player', CONFIG, DB_CONFIG, FILE_STORE)
        players.execute_steps()


run_config()


