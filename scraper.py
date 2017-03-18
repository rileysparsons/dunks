import pandas as pd
import numpy as np
import urllib2
import collections
import json
import bs4
import datetime
import re
import time
import wptools
import os
import scipy
import scipy.stats
from fuzzywuzzy import process
import matplotlib.pyplot as plt
import argparse


def scrape_all(year):

    def players_for_year(year, force_query=False):
        # scrapes basketball-reference.com

        def request_players_from_web(year):
            player_stat_page = "http://www.basketball-reference.com/leagues/NBA_{0}_totals.html".format(str(year+1))
            r = urllib2.urlopen(player_stat_page)
            return r

        def backup_players_to_disk(year, df):
            if not os.path.exists("data/"+str(year)):
                os.makedirs("data/"+str(year))

            with open(os.path.join("data",str(year),"player_list.csv"), "w") as f:
                df.to_csv(f)

        def parse_player_page(page):
            table = bs4.BeautifulSoup(page, "lxml").find("div", {"id":"div_totals_stats"}).table
            stat_dict = collections.defaultdict(list)
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
            return df


        if force_query:
            # We want to get the remote copy, regardless if we have it already.
            page = request_players_from_web(year)
            df = parse_player_page(page)
            backup_players_to_disk(year, df)

        else:
            try:
                # Just try to get the local copy, if it exists
                with open(os.path.join(str(year), "player_list.csv"), "r") as f:
                    return pd.DataFrame.from_csv(f)
            except IOError:
                # We don't have a local copy
                page = request_players_from_web(year)
                df = parse_player_page(page)
                backup_players_to_disk(year, df)

        return df


    def scrape_pbp(year, players):
        # scrapes ESPN and wikipedia

        def getPlayerId(last_name, first_name, players):
            print "searching for: " + first_name + " " + last_name

            basic_search = players[(players["last_name"] == last_name) & (players["first_name"] == first_name)]
            if not basic_search.empty:
                print "found: " + str(basic_search.index[0])
                return basic_search.index[0]
            else:
                full_names = pd.Series(player_list["first_name"]+" "+player_list["last_name"]).values
                name_match = process.extract(first_name+last_name, full_names, limit=1)[0]
                index = np.where(full_names == name_match[0])
                print "fuzzywuzzy found: " + str(players.index[index[0][0]])
                return players.index[index[0][0]]

        def parse_pbp(page, dunk_dict, players, game_id):
            g_soup = bs4.BeautifulSoup(page, "lxml")
            title = g_soup.title
            play_by_play =  g_soup.find("article", {"class":"play-by-play"})

            teams = [team.text for team in g_soup.find_all("span", {"class":"abbrev"})]

            print title.text.split(" - ")
            try:
                for acc in play_by_play.find_all("li", {"class":"accordion-item"}):
                    q_id =""
                    for div in acc.find_all("div"):
                        if div.has_attr('id'):
                            q_id = div["id"]
                    for tr in acc.find_all("tr"):
                        details = tr.find("td", {"class":"game-details"})
                        if details != None:
                            play = details.string
                            # This play was a dunk!
                            if "dunk" in play:
                                print play
                                # And he made it...
                                if "miss" in play:
                                    dunker_name = play.split("miss")[0]
                                    dunk_dict["make"].append(0)
                                # And he missed it...
                                elif "make" in play:
                                    dunker_name = play.split("make")[0]
                                    dunk_dict["make"].append(1)
                                elif "alley-oop" in play:
                                    dunker_name = play.split("alley-oop")[0]
                                    dunk_dict["make"].append(1)
                                elif "'s" in play:
                                    # rare possesive form of play-by-play, "player 1 blocks player 2's slam dunk"
                                    # Since the dunk was blocked, we'll just ignore it for now.
                                    continue
                                else:
                                    print "can't find name"
                                    dunker_name = ""
                                    dunk_dict["make"].append(np.NaN)

                                # Try getting a last name for player, some people don't have these
                                if len(dunker_name.split(" ", 1)) == 2:
                                    last_name = dunker_name.split(" ", 1)[1].strip()
                                else:
                                    last_name = ""

                                # Fetch the playerId from the passed in player dataframe.
                                first_name = dunker_name.split(" ", 1)[0].strip()

                                if len(first_name+last_name) != 0:
                                    player_id = getPlayerId(last_name, first_name, players)
                                    dunk_dict["player_id"].append(player_id)
                                else:
                                    dunk_dict["player_id"].append(np.NaN)

                                # Add quarter to record
                                dunk_dict["quarter"].append(q_id[-1])


                                # Add data to dunk record
                                try:
                                    date_from_title = title.text.split(" - ")[2].strip().encode("ascii", "ignore")
                                    dunk_dict["date"].append(datetime.datetime.strptime(date_from_title, "%B %d, %Y"))
                                except IndexError:
                                    print "Could not get date for game"
                                    dunk_dict["date"].append(np.NaN)


                                # Add in-game time to dunk record
                                time_stamp = tr.find("td", {"class":"time-stamp"}).text
                                dunk_dict["time"].append(datetime.datetime.strptime(time_stamp, "%M:%S").time())
                                dunk_dict["game_id"].append(game_id)

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

                                    if len(first_name+last_name) != 0:
                                        a_player_id = getPlayerId(last_name, first_name, players)
                                        dunk_dict["assister_id"].append(a_player_id)
                                    else:
                                        a_player_id = ""
                                        dunk_dict["assister_id"].append(np.NaN)
                                else:
                                    a_player_id = ""
                                    dunk_dict["assister_id"].append(np.NaN)


                                try:
                                    team_id = tr.find("img")["src"].split("/500/")[1].split(".png")[0].upper()
                                    dunk_dict["team"].append(team_id)
                                except IndexError:
                                    print "couldn't find logo for team"
                                    dunk_dict["team"].append(predict_team(player_id, a_player_id, players, teams))

            except AttributeError, e:
                print e.message
                raise
            except Exception as e:
                print "unexpected error: " + e.message
                raise

            return dunk_dict

        def season_duration(year):

            print "looking for season for year: " + str(year)
            page_title = "{0}%E2%80%93{1}_NBA_season".format(str(year), str(year+1)[2:])


            r = wptools.page(page_title).get_parse().infobox["duration"]

            start_date = 0
            end_date = 0

            dates_string = r.encode('ascii','ignore')
            p = re.compile("[A-Za-z]+\s[0-9]+,\s[0-9]{4}")
            dates = p.findall(dates_string)
            start_date = datetime.datetime.strptime(dates[0], "%B %d, %Y")
            end_date = datetime.datetime.strptime(dates[1], "%B %d, %Y")

            return start_date, end_date

        def predict_team(dunker, assister, player_list, teams):
            if assister != "":
                dunker_teams = player_list.ix[dunker, "teams"]
                if type(dunker_teams) == list:
                    dunker_teams = ",".join(dunker_teams)
                assister_teams = player_list.ix[assister, "teams"]
                if type(assister_teams) == list:
                    assister_teams = ",".join(assister_teams)
                print teams, dunker_teams, assister_teams

                for t in teams:
                    print t + " looking for team"
                    if t in dunker_teams and t in assister_teams:
                        print "found team: " + t
                        return t
                print "team not found"
            else:
                dunker_teams = player_list.ix[dunker, "teams"]
                if type(dunker_teams) == list:
                    dunker_teams = ",".join(dunker_teams)
                print teams, dunker_teams
                for t in teams:
                    print t + " looking for team"
                    if t in dunker_teams:
                        print "found team: " + t
                        return t
                print "team not found"
            return np.NaN



        def parse_schedule_page(page, start_date, end_date):

            def year_to_espn_season_code(year):
                base = (20, 2000)
                diff = year - base[1]
                code_for_year = base[0] + diff
                return code_for_year

            soup = bs4.BeautifulSoup(page)
            games = soup.find_all("a", {"name":"&lpos=nba:schedule:score"})
            links = []
            for game in games:
                game_id = game["href"].split("=")[1]
                game_month = int(game_id[2:4])
                game_day = int(game_id[4:6])
                game_year_code = int(game_id[0:2])
                print game_id, game_month, game_day, game_year_code, year_to_espn_season_code(start_date.year)
                if game_year_code == year_to_espn_season_code(start_date.year):
                    # Beginning of Season
                    if game_month == start_date.month:
                        if game_day > start_date.day or game_day == start_date.day:
                            links.append(game_id)
                    elif game_month > start_date.month:
                        links.append(game_id)
                else:
                    # End of Season
                    if game_month == end_date.month:
                        if game_day < end_date.day or game_day == end_date.day:
                            links.append(game_id)
                    elif game_month < end_date.month:
                        links.append(game_id)
            print "games found: " + str(len(links))
            return links

        def request_schedule_page(date):
            date_param = date.strftime("%Y%m%d")

            ROOT_URL = "http://www.espn.com/nba/schedule/_/date/{date}"
            GAME_ROOT_URL = "http://www.espn.com/nba/playbyplay?gameId={0}"

            r = urllib2.urlopen(ROOT_URL.format(date=date_param)).read()

            return r

        GAME_ROOT_URL = "http://www.espn.com/nba/playbyplay?gameId={0}"
        start_date, end_date = season_duration(year)

        # ESPN starts using sequential game_ids starting Oct 2012, making it very easy to iterate through all games.
        if start_date > datetime.datetime.strptime("October, 2012", "%B, %Y"):
            print "after 2012"
            r = request_schedule_page(start_date)

            soup = bs4.BeautifulSoup(r, "lxml")

            indices = []
            pbp_path = "data/"+str(year)+"/pbp_reg"

            if not os.path.exists(pbp_path):
                os.makedirs(pbp_path)

            dataset_path = "data/"+str(year)+"/datasets"
            if not os.path.exists(dataset_path):
                os.makedirs(dataset_path)

            first_game_id = ""
            for tr in soup.find("div", {"id":"sched-container"}).find_all("tr"):
                if tr.find("a", {"name":"&lpos=nba:schedule:score"}) != None:
                    first_game_id = tr.find("a", {"name":"&lpos=nba:schedule:score"})["href"].split("=")[1]
                    break

            if year == 2011:
                reg_season_games = (66*30)/2
            elif year == 1998:
                reg_season_games = (50*29)/2
            else:
                reg_season_games = (82*30)/2

            games_not_found = []

            dunk_dict = collections.defaultdict(list)

            for game_id in np.arange(int(first_game_id), int(first_game_id)+reg_season_games):
                try:
                    with open(os.path.join(pbp_path, "pbp_"+str(game_id)+".txt"), "r") as f:
                        g = f.read()
                        g_soup = bs4.BeautifulSoup(g, "lxml")
                    print "We've got this on disk"
                except IOError:
                    print "Don't have this play-by-play yet"
                    time.sleep(np.random.randint(2,7))
                    try:
                        g = urllib2.urlopen(GAME_ROOT_URL.format(game_id)).read()
                        g_soup = bs4.BeautifulSoup(g, "lxml")
                        with open(os.path.join(pbp_path, "pbp_"+str(game_id)+".txt"), "wb") as f:
                            f.write(str(g_soup))
                    except urllib2.URLError:
                        print "could not get page"
                        games_not_found.append(game_id)
                        continue

                try:
                    dunk_dict = parse_pbp(g, dunk_dict, players, game_id)
                except AttributeError:
                    games_not_found.append(game_id)
        # ESPN doesn't use a sequential numbering scheme for game_ids before October 2012
        else:
            print "before 2012"
            duration = end_date - start_date

            weeks_in_season = duration.days//7

            pbp_path = "data/"+str(year)+"/pbp_reg"

            if not os.path.exists(pbp_path):
                os.makedirs(pbp_path)

            dataset_path = "data/"+str(year)+"/datasets"
            if not os.path.exists(dataset_path):
                os.makedirs(dataset_path)

            games_not_found = []

            for week in np.arange(0, weeks_in_season+1):
                r = request_schedule_page(start_date+datetime.timedelta(days=week*7))
                game_ids = parse_schedule_page(r, start_date, end_date)
                for game_id in game_ids:
                    try:
                        with open(os.path.join(pbp_path, "pbp_"+str(game_id)+".txt"), "r") as f:
                            g = f.read()
                            g_soup = bs4.BeautifulSoup(g, "lxml")
                        print "We've got this on disk"
                    except IOError:
                        print "Don't have this play-by-play yet"
                        time.sleep(np.random.randint(2,7))
                        try:
                            g = urllib2.urlopen(GAME_ROOT_URL.format(game_id)).read()
                            g_soup = bs4.BeautifulSoup(g, "lxml")
                            with open(os.path.join(pbp_path, "pbp_"+str(game_id)+".txt"), "wb") as f:
                                f.write(str(g_soup))
                        except urllib2.URLError:
                            print "could not get page"
                            games_not_found.append(game_id)
                            continue

                    try:
                        print "trying to parse pbp"
                        dunk_dict = parse_pbp(g, dunk_dict, players, game_id)
                    except AttributeError, error:
                        print "game not found "+ game_id + "error: " + error.message
                        games_not_found.append(game_id)
                    except Exception, error:
                        print "unexpected error: " + error.message


        for k,v in dunk_dict.iteritems():
            print k, len(v)

        df = pd.DataFrame(dunk_dict)
        df["id"] = df.apply((lambda x: str(str(x["game_id"])+str(x["quarter"])+str(x["time"]).replace(":", ""))), axis=1)

        with open(os.path.join(dataset_path, "dunks.csv"), "wb") as f:
            df.to_csv(f)

    player_list = players_for_year(year)
    scrape_pbp(year, player_list)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get dunks for any given NBA season')
    parser.add_argument('season', type=int,
                        help='The year to scrape (the year passed represents the beginning of the season)')

    args = parser.parse_args()
    scrape_all(args.season)
