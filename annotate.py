import os
import pandas as pd


PLAYER_HEADERS = ['id', 'age', 'ast', 'blk', 'drb', 'efg_pct', 'fg', 'fg2', 'fg2_pct',
       'fg2a', 'fg3', 'fg3_pct', 'fg3a', 'fg_pct', 'fga', 'first_name',
       'ft', 'ft_pct', 'fta', 'g', 'gs', 'last_name', 'mp', 'orb', 'pf',
       'pos', 'pts', 'stl', 'teams', 'tov', 'trb', 'season']

DUNK_HEADERS = ['assister_id', 'date', 'dunker_id', 'game_id', 'make', 'quarter',
       'team', 'time', 'id', 'season']

def read(data_type):
    if data_type == "players":
        path = os.path.join("processed", "all_players.csv")
    elif data_type == "dunks":
        path = os.path.join("processed", "all_dunks.csv")
    data = pd.DataFrame.from_csv(path)
    return data

def aggregate_and_annotate(players, dunks):

    def find_player_stats_for_season(row):
        dunker_id = row["dunker_id"]
        season = row["season"]
        stats = players[(players["id"] == dunker_id) & (players["season"] == season)]
        return list(stats.values)


    dunks["miss"] = dunks.make.map(lambda x: x==0)
    player_dunks_by_years = []
    for season in dunks.season.unique():
        df = dunks[dunks.season == season].groupby("dunker_id").sum().ix[:, ["make", "miss"]]
        df["season"] = season
        player_dunks_by_years.append(df)
    df = pd.concat(player_dunks_by_years)
    df.reset_index(inplace=True)
    print pd.DataFrame(*zip(df.apply(find_player_stats_for_season, axis=1))).head()
    # stats.head()
    # print pd.concat(df, stats).head()






if __name__ == "__main__":

    players = read("players")
    dunks = read("dunks")

    dunks = dunks.ix[:, DUNK_HEADERS]
    players = players.ix[:, PLAYER_HEADERS]

    aggregate_and_annotate(players, dunks)
