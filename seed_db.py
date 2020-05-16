from roster import loader
from atBat import atBat
from player import player

CONFIG = 'seed_db.yaml'
DB_CONFIG = 'do_db.yaml'
FILE_STORE = 'local'


def run(extract = False, load = True):

    atBat_loader = atBat(config=CONFIG,
                         table='play_by_play',
                         db_config=DB_CONFIG,
                         file_store=FILE_STORE)
    team_loader = loader('team', CONFIG, DB_CONFIG, FILE_STORE)
    rosters_40_loader = loader('roster_40', CONFIG, DB_CONFIG, FILE_STORE)
    rosters_past = loader('roster_past', CONFIG, DB_CONFIG, FILE_STORE)
    transactions = loader('transaction', CONFIG, DB_CONFIG, FILE_STORE)
    players = player('player', CONFIG, DB_CONFIG, FILE_STORE)



    if extract:

        team_loader.make_all_requests()
        rosters_40_loader.make_all_requests()
        rosters_past.make_all_requests()
        transactions.make_all_requests()
        players.make_all_requests()
        atBat_loader.make_all_requests()


    if load:

        team_loader.truncate_table()
        rosters_40_loader.truncate_table()
        rosters_past.truncate_table()
        transactions.truncate_table()
        players.truncate_table()
        atBat_loader.truncate_table()

        team_loader.populate_table(load_type='df')
        rosters_40_loader.populate_table(load_type='df')
        rosters_past.populate_table(load_type='df')
        transactions.populate_table(load_type='df')
        players.populate_table(load_type='df')
        atBat_loader.populate_table(load_type='copy')





run()
