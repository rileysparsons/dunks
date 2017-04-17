import bs4, pandas as pd
from datetime import datetime
from collections import defaultdict


class ScheduleParser():
    def __init__(self, season):
        self.season = self.year_to_espn_season_code(season)

    def year_to_espn_season_code(self, year):
        base = (20, 2000)
        diff = year - base[1]
        code_for_year = base[0] + diff
        return code_for_year

    def parse_schedule_page(self, page, start, end):
        soup = bs4.BeautifulSoup(page)
        games = soup.find_all("a", {"name": "&lpos=nba:schedule:score"})
        game_ids = []
        for game in games:
            game_id = game["href"].split("=")[1]
            game_month = int(game_id[2:4])
            game_day = int(game_id[4:6])
            game_year_code = int(game_id[0:2])
            if game_year_code == self.season:
                # Beginning of Season
                if game_month == start.month:
                    if game_day > start.day or game_day == start.day:
                        game_ids.append(game_id)
                elif game_month > start.month:
                    game_ids.append(game_id)
            else:
                # End of Season
                if game_month == end.month:
                    if game_day < end.day or game_day == end.day:
                        game_ids.append(game_id)
                elif game_month < end.month:
                    game_ids.append(game_id)
        print "games found: " + str(len(game_ids))
        return game_ids

class PlayerParser():
    def parse_player_page(self, page):
        table = bs4.BeautifulSoup(page, "lxml").find("div", {"id":"div_totals_stats"}).table
        stat_dict = defaultdict(list)
        for row in table.find_all("tr", {"class":["full_table", "partial_table"]})[1:]:
            if row.find("td")["data-append-csv"] not in stat_dict["player_id"]:

                for col in row.find_all("td"):
                    if col.has_attr("data-stat"):
                        stat_name = col["data-stat"]

                        # Special behavior for managing team data
                        if stat_name == "team_id":
                            if col.text != "TOT":
                                #this player played on just one team this season.
                                stat_dict["teams"].append([col.text])
                            else:
                                #this player played on multiple teams, we'll add them later.
                                stat_dict["teams"].append([])
                        # Special behavior for the player name data
                        elif stat_name == "player":
                            player = col["csk"]
                            stat_dict["first_name"].append(player.split(",")[1])
                            stat_dict["last_name"].append(player.split(",")[0])
                            stat_dict["player_id"].append(col["data-append-csv"])
                        # With everything else just use the stat name
                        else:
                            stat_dict[stat_name].append(col.text)
            else:
                # we've recorded this player's information before, we just need to complete his list of teams.
                index = stat_dict["player_id"].index(row.find("td")["data-append-csv"])
                stat_dict["teams"][index].append(row.find("td", {"data-stat":"team_id"}).a.text)
        df = pd.DataFrame(stat_dict)
        df.set_index("player_id", drop=True, inplace=True)
        return df