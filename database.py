import dataset
from collections import defaultdict

DB_ADDRESS = "sqlite:///dunks.db"


class DatabaseManager():
    def __init__(self):
        self.db = dataset.connect(DB_ADDRESS)
        self.error_dict = defaultdict(list)

    def add_dunk(self, dunk):
        self.db.begin()
        try:
            self.db["dunks"].insert_ignore(dunk, ["id"])
            self.db.commit()
        except Exception as err:
            print "Did not save dunk. ", err
            self.db.rollback()
            self.record_failure("dunk", dunk)

    def add_game(self, game):
        self.db.begin()
        try:
            self.db["games"].insert_ignore(game, ["id"])
            self.db.commit()
        except Exception as err:
            print "Did not save game. ", err
            self.db.rollback()
            self.record_failure("game", game)

    def get_game_ids(self, season):
        return [game["id"] for game in self.db["games"].find(season=season)]

    def get_players(self, season):
        return [player for player in self.db["players"].find(season=season)]

    def get_dunks(self, season):
        return [dunk for dunk in self.db["dunks"].find(season=season)]

    def add_player(self, player):
        self.db.begin()
        try:
            self.db["players"].insert_ignore(player, ["id"])
            self.db.commit()
        except Exception as err:
            print "Did not save player", err
            self.db.rollback()
            self.record_failure("player", player)

    def record_failure(self, table, content):
        self.error_dict[table].append(content)
