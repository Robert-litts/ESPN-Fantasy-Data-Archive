from app.services.asyncLeagueData import fetch_league_data
from app.services.espn_service import *
import asyncio
import pickle
from app.services.cache import save_league_to_shelf, load_league_from_shelf, fetch_league_with_cache
from espn_api.football import League
from dotenv import load_dotenv
import os
load_dotenv()
START_YEAR=os.getenv("START_YEAR")
END_YEAR=os.getenv("END_YEAR")
LEAGUE_ID = os.getenv("LEAGUE_ID")
ESPN_S2 = os.getenv("ESPN_S2")
SWID = os.getenv("SWID")
years = range(START_YEAR,END_YEAR)
# fetch league data from ESPN API
leagues = fetch_league_with_cache(years, LEAGUE_ID, ESPN_S2, SWID)

#Populate DB with league data
fetch_and_populate_leagues_from_leagues(leagues)
fetch_and_populate_players_from_leagues(leagues)
fetch_and_populate_settings_from_leagues(leagues)
fetch_and_populate_teams_from_leagues(leagues)
fetch_and_populate_draft_from_leagues(leagues)
fetch_and_populate_matchups_from_leagues(leagues)
fetch_and_populate_roster_from_leagues(leagues)

