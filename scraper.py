
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
import dataset



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
            df.set_index("player_id", drop=True, inplace=True)
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

        ###### Helper Methods #######

        def getPlayerId(last_name, first_name, players):
            # print "searching for: " + first_name + " " + last_name

            basic_search = players[(players["last_name"] == last_name) & (players["first_name"] == first_name)]
            if not basic_search.empty:
                # print "found: " + str(basic_search.index[0])
                return basic_search.index[0]
            else:
                full_names = pd.Series(player_list["first_name"]+" "+player_list["last_name"]).values
                name_match = process.extract(first_name+last_name, full_names, limit=1)[0]
                index = np.where(full_names == name_match[0])
                # print "fuzzywuzzy found: " + str(players.index[index[0][0]])
                return players.index[index[0][0]]

        def parse_dunk(dunk, players, game_id, title, teams):
            if "missed" or "made" in dunk:

                # Old style
                if "missed" in dunk:
                    dunker_name = dunk.split("miss")[0]
                    make = 0
                    # And he missed it...
                elif "made" in dunk:
                    dunker_name = dunk.split("made")[0]
                    make = 0
                elif "alley-oop" in dunk:
                    dunker_name = dunk.split("alley-oop")[0]
                    make = 1
                elif "'s" in dunk:
                    # rare possesive form of play-by-play, "player 1 blocks player 2's slam dunk"
                    # Since the dunk was blocked, we'll just ignore it for now.
                    return
                else:
                    print "can't find name"
                    return
            else:
                if "miss" in dunk:
                    # And he missed it...
                    dunker_name = dunk.split("miss")[0]
                    make = 0

                elif "make" in dunk:
                    # And he made it...
                    dunker_name = dunk.split("make")[0]
                    make = 1
                elif "alley-oop" in dunk:
                    dunker_name = dunk.split("alley-oop")[0]
                    make = 1
                elif "'s" in dunk:
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

            if len(first_name+last_name) != 0:
                player_id = getPlayerId(last_name, first_name, players)
            else:
                player_id = np.NaN

            # Add quarter to record
            quarter = q_id[-1]

            # Add data to dunk record
            try:
                date_from_title = title.text.split(" - ")[2].strip().encode("ascii", "ignore")
                date = datetime.datetime.strptime(date_from_title, "%B %d, %Y")

                day = date.day
                month = date.month
                year = date.year

            except IndexError:
                print "Could not get date for game"

                day = np.NaN
                month = np.NaN
                year = np.NaN

            # Add in-game time to dunk record
            time_stamp = tr.find("td", {"class":"time-stamp"}).text
            time = datetime.datetime.strptime(time_stamp, "%M:%S").time()

            minute = time.minute
            second = time.second

            ptrn = re.compile("\([\w\s]*\)")
            # Add assist to dunk record
            if re.findall(ptrn, dunk):
                assist = re.findall(ptrn, dunk)[0]
                full_name = assist.strip("(").strip(")").split("assists")[0].strip()
                first_name = full_name.split(" ")[0].strip()
                if len(full_name.split(" ")) == 2:
                    last_name = full_name.split(" ")[1].strip()
                else:
                    last_name = ""

                if len(first_name+last_name) != 0:
                    a_player_id = getPlayerId(last_name, first_name, players)
                else:
                    a_player_id = np.NaN
            elif "assisted" in dunk:
                full_name = dunk.lower().split("assisted by")[1].strip()
                first_name = full_name.split(" ")[0].strip()
                if len(full_name.split(" ")) == 2:
                    last_name = full_name.split(" ")[1].strip()
                else:
                    last_name = ""

                if len(first_name+last_name) != 0:
                    a_player_id = getPlayerId(last_name, first_name, players)
                else:
                    a_player_id = np.NaN
            else:
                a_player_id = np.NaN


            try:
                team_id = tr.find("img")["src"].split("/500/")[1].split(".png")[0].upper()
            except IndexError:
                print "couldn't find logo for team"
                team_id = predict_team(player_id, a_player_id, players, teams)
                
            dunk_id = str(str(game_id)+str(quarter)+str(minute)+str(second))

            return dict(day=day,
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
            id=dunk_id
            )

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
            
            dunker_teams = player_list.ix[dunker, "teams"]
            if type(dunker_teams) == list:
                dunker_teams = ",".join(dunker_teams)
            
            if not pd.isnull(assister):
                
                assister_teams = player_list.ix[assister, "teams"]
                if type(assister_teams) == list:
                    assister_teams = ",".join(assister_teams)
                
                for t in teams:
                    if t in dunker_teams and t in assister_teams:
                        return t
            else:
                for t in teams:
                    if t in dunker_teams:
                        return t
                    
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

        ###### END OF HELPER METHODS ######

        db = dataset.connect("sqlite:///dunks.db")
        table = db["dunks"]

        results = list(table.find(season=2005, order_by=["-year", "-month", "-day"]))
        
        start_date, end_date = season_duration(year)
        
        if len(results) > 0:
            month = results[0]["month"]
            day = results[0]["day"]
            year = results[0]["year"]
            start_date = datetime.datetime(year=year, month=month, day=day)
        
        GAME_ROOT_URL = "http://www.espn.com/nba/playbyplay?gameId={0}"    

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
                    except urllib2.URLError:
                        print "could not get page"
                        games_not_found.append(game_id)
                        continue
                        
                        
                g_soup = bs4.BeautifulSoup(g, "lxml")
                with open(os.path.join(pbp_path, "pbp_"+str(game_id)+".txt"), "wb") as f:
                    f.write(str(g_soup)) 

                title = g_soup.title
                print "Checking for dunks: ", " ".join(title.text.split(" - ")[0:3])
                
                
                play_by_play =  g_soup.find("article", {"class":"play-by-play"})

                teams = [team.text for team in g_soup.find_all("span", {"class":"abbrev"})]

                for acc in play_by_play.find_all("li", {"class":"accordion-item"}):
                    for div in acc.find_all("div"):
                        if div.has_attr('id'):
                            q_id = div["id"]
                    for tr in acc.find_all("tr"):
                        details = tr.find("td", {"class":"game-details"})
                        if details != None:
                            play = details.string.lower()
                            if "dunk" in play:
                                # This play was a dunk!
                                parsed_dict = parse_dunk(play, players, game_id, title, teams)
                                if parsed_dict != None:
                                    parsed_dict["season"] = year
                                    try:
                                        table.insert_ignore(parsed_dict, ["id"])
                                        db.commit()
                                    except:
                                        db.rollback()
                                else:
                                    continue

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
                        try:
                            time.sleep(np.random.randint(2,7))
                            g = urllib2.urlopen(GAME_ROOT_URL.format(game_id)).read()
                        except urllib2.URLError:
                            print "could not get page"
                            games_not_found.append(game_id)
                            continue
                        
                    g_soup = bs4.BeautifulSoup(g, "lxml")
                    with open(os.path.join(pbp_path, "pbp_"+str(game_id)+".txt"), "wb") as f:
                        f.write(str(g_soup)) 

                    title = g_soup.title
                    print "Checking for dunks: ", " ".join(title.text.split(" - ")[0:3])
                    
                    play_by_play =  g_soup.find("article", {"class":"play-by-play"})
                    
                    teams = [team.text for team in g_soup.find_all("span", {"class":"abbrev"})]

                    for acc in play_by_play.find_all("li", {"class":"accordion-item"}):
                        for div in acc.find_all("div"):
                            if div.has_attr('id'):
                                q_id = div["id"]
                        for tr in acc.find_all("tr"):
                            details = tr.find("td", {"class":"game-details"})
                            if details != None:
                                play = details.string.lower()
                                if "dunk" in play:
                                    # This play was a dunk!
                                    parsed_dict = parse_dunk(play, players, game_id, title, teams)
                                    if parsed_dict != None:
                                        parsed_dict["season"] = year
                                        try:
                                            table.insert_ignore(parsed_dict, ["id"])
                                            db.commit()
                                        except:
                                            db.rollback()
                                else:
                                    continue
        #
        # for k,v in dunk_dict.iteritems():
        #     print k, len(v)
        #
        #
        # df = pd.DataFrame(dunk_dict)
        # print df


        # with open(os.path.join(dataset_path, "dunks.csv"), "wb") as f:
        #     df.to_csv(f)

    player_list = players_for_year(year)
    scrape_pbp(year, player_list)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get dunks for any given NBA season')
    parser.add_argument('season', type=int,
                        help='The year to scrape (the year passed represents the beginning of the season)')

    args = parser.parse_args()
    scrape_all(args.season)