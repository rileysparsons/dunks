import pandas as pd
import os

def concatenate(data_type):
    year_dirs = [name for name in os.listdir("data")
                if os.path.isdir(os.path.join("data", name))]

    full = []
    for year in year_dirs:
        if data_type == "dunks":
                if os.path.exists(os.path.join("data", year, "datasets", "dunks.csv")):
                    print "found dunk data for season: " + year + "-" + str(int(year)+1)
                    data = pd.read_csv(os.path.join("data", year, "datasets", "dunks.csv"))
                    data["season"] =  year
                    full.append(data)

        elif data_type == "players":
                if os.path.exists(os.path.join("data", year, "player_list.csv")):
                    print "found player data for season: " + year + "-" + str(int(year)+1)
                    data = pd.read_csv(os.path.join("data", year, "player_list.csv"))
                    data["season"] =  year
                    full.append(data)

    df = pd.concat(full, axis = 0)
    df.reset_index(inplace=True)

    try:
        df.to_csv(os.path.join("processed", "all_{}.csv".format(data_type)))
    except IOError:
        os.mkdir("processed")
        df.to_csv(os.path.join("processed", "all_{}.csv".format(data_type)))

if __name__ == "__main__":
    concatenate("dunks")
    concatenate("players")
