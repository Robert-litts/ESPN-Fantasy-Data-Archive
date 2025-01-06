
from app.services.espn_service import fetch_and_populate_draft, fetch_and_populate_league, fetch_and_populate_teams, fetch_and_populate_players, fetch_and_populate_settings, fetch_and_populate_draft, fetch_and_populate_players_concurrent, fetch_and_populate_draft_concurrent

# Call the function to fetch and populate draft data for the specified years
#fetch_and_populate_draft(2014, 2017)
#fetch_and_populate_league(2013, 2021)
#fetch_and_populate_teams(2013,2021)
#fetch_and_populate_players(2013,2021)
#fetch_and_populate_settings(2013,2021)
fetch_and_populate_draft(2013,2021)
#fetch_and_populate_players_concurrent(2013, 2021)
#fetch_and_populate_draft_concurrent(2013,2021)
#func_to_pd(2013)
