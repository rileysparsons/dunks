import bs4, pandas as pd
from datetime import datetime
from collections import defaultdict
import numpy as np
from fuzzywuzzy import process
import re

class ScheduleParser:
    def __init__(self, season):
        self.season = season


    def parse_schedule_page(self, page, start, end):
        soup = bs4.BeautifulSoup(page, 'lxml')
        games = soup.find_all("a", {"name": "&lpos=nba:schedule:score"})
        schedule = soup.find('div', {'id': 'sched-container'}).find_all(['div', 'h2'], recursive=False)
        game_ids = []

        for i, tag in enumerate(schedule):

            if tag.name == 'h2' and tag['class'][0] == 'table-caption':

                if 'February' in tag.text:
                    # hacky way to stop leap year errors with strptime
                    date_string = tag.text + ' 2012'
                    date = datetime.strptime(date_string, "%A, %B %d %Y")
                else:
                    date = datetime.strptime(tag.text, "%A, %B %d")

                if (date.month > 0) and (date.month < 5):
                    date = date.replace(year=self.season+1)
                else:
                    date = date.replace(year = self.season)
                print date

                if (date >= start) and \
                        (date <= end):
                    game_ids.extend([game['href'].split("=")[1] for game in
                                    schedule[i+1].find_all("a", {"name": "&lpos=nba:schedule:score"})])
        return game_ids
        # for game in games:
        #     game_id = game["href"].split("=")[1]
        #     game_month = int(game_id[2:4])
        #     game_day = int(game_id[4:6])
        #     game_year_code = int(game_id[0:2])
        #     if game_year_code == self.season:
        #         # Beginning of Season
        #         if game_month == start.month:
        #             if game_day > start.day or game_day == start.day:
        #                 game_ids.append(game_id)
        #         elif game_month > start.month:
        #             game_ids.append(game_id)
        #     else:
        #         # End of Season
        #         if game_month == end.month:
        #             if game_day < end.day or game_day == end.day:
        #                 game_ids.append(game_id)
        #         elif game_month < end.month:
        #             game_ids.append(game_id)
        # print "games found: " + str(len(game_ids))
        # return game_ids

    def get_first_game(self, page):
        soup = bs4.BeautifulSoup(page, 'lxml')
        game = soup.find_all("a", {"name": "&lpos=nba:schedule:score"})[0]["href"].split("=")[1]
        return game


class PlayerParser:

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
                                stat_dict["teams"].append(col.text)
                            else:
                                #this player played on multiple teams, we'll add them later.
                                stat_dict["teams"].append("")
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
                stat_dict["teams"][index] = stat_dict["teams"][index]+ "," + \
                                            row.find("td", {"data-stat":"team_id"}).a.text
        df = pd.DataFrame(stat_dict)
        df.set_index("player_id", drop=True, inplace=True)
        return df

class PlayByPlayParser():

    def parse_pbp(self, page, players, game_id, season):
        g_soup = bs4.BeautifulSoup(page, "lxml")
        try:
            title = g_soup.title
            print "Checking for dunks: ", " ".join(title.text.split(" - ")[0:3])
        except AttributeError:
            print "Something went wrong. Possibly the backed up file did not save properly."
            return None

        # Add data to dunk record
        try:
            date_from_title = title.text.split(" - ")[2].strip().encode("ascii", "ignore")
            date = datetime.strptime(date_from_title, "%B %d, %Y")
            day = date.day
            month = date.month
            year = date.year

        except IndexError:
            print "Could not get date for game"
            day = np.NaN
            month = np.NaN
            year = np.NaN


        play_by_play = g_soup.find("article", {"class": "play-by-play"})

        try:
            play_by_play.find_all("li", {"class": "accordion-item"})
        except AttributeError:
            print "Something went wrong. This is not a valid game play-by-play."
            return None

        teams = [team.text for team in g_soup.find_all("span", {"class": "abbrev"})]

        dunks = []
        for acc in play_by_play.find_all("li", {"class": "accordion-item"}):
            for div in acc.find_all("div"):
                if div.has_attr('id'):
                    q_id = div["id"]
            for tr in acc.find_all("tr"):
                details = tr.find("td", {"class": "game-details"})
                if details is not None:
                    play = details.string.lower()
                    if "dunk" in play:
                        # This play was a dunk!
                        if ("missed" in play) or ("made" in play):
                            print "old"
                            # Old style
                            if "missed" in play:
                                dunker_name = play.split("miss")[0]
                                make = 0
                                # And he missed it...
                            elif "made" in play:
                                dunker_name = play.split("made")[0]
                                make = 1
                            elif "alley-oop" in play:
                                dunker_name = play.split("alley-oop")[0]
                                make = 1
                            elif "'s" in play:
                                # rare possesive form of play-by-play, "player 1 blocks player 2's slam dunk"
                                # Since the dunk was blocked, we'll just ignore it for now.
                                return
                            else:
                                print "can't find name"
                                return
                        else:
                            print "new"
                            if "miss" in play:
                                # And he missed it...
                                dunker_name = play.split("miss")[0]
                                make = 0

                            elif "make" in play:
                                # And he made it...
                                dunker_name = play.split("make")[0]
                                make = 1
                            elif "alley-oop" in play:
                                dunker_name = play.split("alley-oop")[0]
                                make = 1
                            elif "'s" in play:
                                # rare possesive form of play-by-play, "player 1 blocks player 2's slam dunk"
                                # Since the dunk was blocked, we'll just ignore it for now.
                                return
                            else:
                                print "can't find name"
                                return

                        # Try getting a last name for player, some people don't have these
                        if len(dunker_name.split(" ", 1)) == 2:
                            last_name = dunker_name.split(" ", 1)[1].strip()
                        else:
                            last_name = ""

                        # Fetch the playerId from the passed in player dataframe.
                        first_name = dunker_name.split(" ", 1)[0].strip()

                        if len(first_name + last_name) != 0:
                            player_id = self.get_player_id(last_name, first_name, players)
                        else:
                            player_id = np.NaN

                        # Add quarter to record
                        quarter = q_id[-1]

                        # Add in-game time to dunk record
                        time_stamp = tr.find("td", {"class": "time-stamp"}).text
                        time = datetime.strptime(time_stamp, "%M:%S").time()

                        minute = time.minute
                        second = time.second

                        ptrn = re.compile("\([\w\s]*\)")
                        # Add assist to dunk record
                        if re.findall(ptrn, play):
                            assist = re.findall(ptrn, play)[0]
                            full_name = assist.strip("(").strip(")").split("assists")[0].strip()
                            first_name = full_name.split(" ")[0].strip()
                            if len(full_name.split(" ")) == 2:
                                last_name = full_name.split(" ")[1].strip()
                            else:
                                last_name = ""

                            if len(first_name + last_name) != 0:
                                a_player_id = self.get_player_id(last_name, first_name, players)
                            else:
                                a_player_id = ""
                        elif "assisted" in play:
                            full_name = play.lower().split("assisted by")[1].strip()
                            first_name = full_name.split(" ")[0].strip()
                            if len(full_name.split(" ")) == 2:
                                last_name = full_name.split(" ")[1].strip()
                            else:
                                last_name = ""

                            if len(first_name + last_name) != 0:
                                a_player_id = self.get_player_id(last_name, first_name, players)
                            else:
                                a_player_id = ""
                        else:
                            a_player_id = ""

                        try:
                            team_id = tr.find("img")["src"].split("/500/")[1].split(".png")[0].upper()
                        except IndexError:
                            print "couldn't find logo for team"
                            team_id = self.predict_team(player_id, a_player_id, players, teams)

                        dunk_id = str(str(game_id) + str(quarter) + str(minute) + str(second))

                        dunks.append(dict(day=day,
                                        month=month,
                                        year=year,
                                        minute=minute,
                                        second=second,
                                        game_id=game_id,
                                        quarter=quarter,
                                        make=make,
                                        player_id=player_id,
                                        assister_id=a_player_id,
                                        team=team_id,
                                        id=dunk_id,
                                        season=season
                                        ))
        return dunks

    def predict_team(self, player_id, a_player_id, players, teams):
        df = pd.DataFrame.from_dict(players)
        dunker_teams = df[df['player_id'] == player_id]["teams"].values[0]
        if type(dunker_teams) == list:
            dunker_teams = ",".join(dunker_teams)

        if a_player_id:

            assister_teams = df[df['player_id'] == a_player_id]["teams"].values[0]
            if type(assister_teams) == list:
                assister_teams = ",".join(assister_teams)
            print 'team list:', teams, 'dunker t', dunker_teams, 'assist t', assister_teams
            for t in teams:
                if t in dunker_teams and t in assister_teams:
                    return t
        else:
            print 'team list:', teams, 'dunker t', dunker_teams
            for t in teams:
                if t in dunker_teams:
                    return t
        print 'failed to find team...'
        return np.NaN

    @staticmethod
    def get_player_id(last_name, first_name, players):
        df = pd.DataFrame.from_dict(players)
        basic_search = df[(df["last_name"] == last_name) & (df["first_name"] == first_name)]
        if not basic_search.empty:
            # print "found: " + str(basic_search.index[0])
            print basic_search['player_id']
            return basic_search.index[0]
        else:
            full_names = pd.Series(df["first_name"] + " " + df["last_name"]).values
            name_match = process.extract(first_name + last_name, full_names, limit=1)[0]
            index = np.where(full_names == name_match[0])
            print type(df.ix[index[0][0], 'player_id'])
            return df.ix[index[0][0], 'player_id'].encode('utf-8')
