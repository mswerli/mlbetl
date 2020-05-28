import yaml
import psycopg2 as pg
import sqlalchemy as sql
from utility_classes.parmeter_constructor  import parameter_constructor
from utility_classes.extract_step import extract_step
import time
import requests
import os
import json
import datetime


class id_manager:

    def __init__(self, endpoints, db_config):

        with open(r'config/' + db_config) as file:
            db_config = yaml.load(file, Loader=yaml.FullLoader)

        self.engine = sql.create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}" \
                                        .format(db_config['user'],
                                                db_config['password'],
                                                db_config['host'],
                                                db_config['port'],
                                                db_config['database']))

    def get_missing_player_ids(self):

        ##Used to get players who are on rosters but not in the players table
        ##Values will be used if All is indicated for player ids in config file

        query = """select player_id from rosters.missing_players"""
        res = self.engine.execute(query)
        player_ids = [a[0] for a in res.fetchall()]

        return player_ids

    def get_team_ids(self, id_type='team_id'):

        ##Used to substitute All values for team_id in config files

        with open(r'config/teams.yaml') as file:
            teams = yaml.load(file, Loader=yaml.FullLoader)

        team_ids = [team[id_type] for team in teams]

        return team_ids

    def get_missing_game_ids(self):


