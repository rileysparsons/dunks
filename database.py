import dataset

DB_ADDRESS = "sqlite:///dunks.db"


class Database():
    def __init__(self):
        self.db = dataset.connect(DB_ADDRESS)
