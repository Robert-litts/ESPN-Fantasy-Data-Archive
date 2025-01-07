from app.services.asyncLeagueData import fetch_league_data
from app.services.espn_service import fetch_and_populate_draft, fetch_and_populate_league, fetch_and_populate_teams, fetch_and_populate_players, fetch_and_populate_settings, fetch_and_populate_draft, fetch_and_populate_players_concurrent, fetch_and_populate_draft_concurrent, fetch_and_populate_draft_from_leagues
import asyncio
import pickle
from app.services.cache import save_league_to_shelf, load_league_from_shelf, fetch_league_with_cache
from espn_api.football import League
years = range(2013,2022)
LEAGUE_ID = '747376'
ESPN_S2 = 'AEAJA%2Fy8g05SnaVZ9KbodrBqefAHzG3RvXbFtbZuNcrMPOIwN%2F5fqKHSbwuZeAh6vVgrLOxmYT50SVKJ3wtd6LdYTOaIpe%2FG8ilQ6ijU5mZjdLfQzEW0DE1JYZTrSii%2Fi%2BcjgJcNeMFALOyeccy4ljD8tZwEbzJ5Pzp7KPk%2BLVS7H8C8RCwBnuuQUlMnlZSmS6sZz1W8F0qz06gxz5zzji4BbHp4%2BvUc%2FCzBGhDoIPXFz%2BYDteBxocWWvp3c3sYmqS6dQHeq3oQftD0C2B2yxIbGSarDN2DMj%2FLs18pKbYs6nQ%3D%3D'
SWID = '{A88F0FF7-2664-46A3-8F0F-F7266476A3C3}'
# Call the function to fetch and populate draft data for the specified years
#fetch_and_populate_draft(2014, 2017)
#fetch_and_populate_league(2013, 2021)
#fetch_and_populate_teams(2013,2021)
#fetch_and_populate_players(2013,2021)
#fetch_and_populate_settings(2013,2021)
#fetch_and_populate_draft(2013,2021)
#fetch_and_populate_players_concurrent(2013, 2021)
#fetch_and_populate_draft_concurrent(2013,2021)
#func_to_pd(2013)

# leagues = asyncio.run(fetch_league_data(years, LEAGUE_ID, ESPN_S2, SWID))
# if leagues is not None and len(leagues) > 0:
#     print('LEAGUES')
#     print(leagues)
#     for league in leagues:
#         print('year: ', league.year)
#         save_league_to_shelf(league)
#         print(league.teams)
#fetch_and_populate_draft_from_leagues(leagues)
# fetch_and_populate_draft_from_leagues(leagues)
leagues = fetch_league_with_cache(years, LEAGUE_ID, ESPN_S2, SWID)
for league in leagues:
        print('year: ', league.year)
        print(league.standings())
        print('TOP SCORER: ', league.top_scorer())

