from espn_api.football import League
from app.db.models import FFleague, Team, Draft, Player, Settings, Matchup, BoxScore
#from app.db.session import SessionLocal  # Assuming you have a SessionLocal() function to get a database session
from app.db.session import get_db
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from contextlib import contextmanager

# Replace these with your actual values
LEAGUE_ID = '747376'
ESPN_S2 = 'AEAJA%2Fy8g05SnaVZ9KbodrBqefAHzG3RvXbFtbZuNcrMPOIwN%2F5fqKHSbwuZeAh6vVgrLOxmYT50SVKJ3wtd6LdYTOaIpe%2FG8ilQ6ijU5mZjdLfQzEW0DE1JYZTrSii%2Fi%2BcjgJcNeMFALOyeccy4ljD8tZwEbzJ5Pzp7KPk%2BLVS7H8C8RCwBnuuQUlMnlZSmS6sZz1W8F0qz06gxz5zzji4BbHp4%2BvUc%2FCzBGhDoIPXFz%2BYDteBxocWWvp3c3sYmqS6dQHeq3oQftD0C2B2yxIbGSarDN2DMj%2FLs18pKbYs6nQ%3D%3D'
SWID = '{A88F0FF7-2664-46A3-8F0F-F7266476A3C3}'


def fetch_and_populate_draft(start_year, end_year):
    db = next(get_db())

    try:
        for year in range(start_year, end_year + 1):
            # Initialize the league object with your league ID and year
            league = League(LEAGUE_ID, year=year, swid=SWID, espn_s2=ESPN_S2)
            teams = league.teams
            # Iterate through the teams and insert them into the database
            for team in teams:

                new_team = Team (

                    league_id = LEAGUE_ID,
                    team_id = team.team_id,
                    team_abbrev = team.team_abbrev,
                    team_name = team.team_name,
                    division_id = team.division_id,
                    division_name = team.division_name,
                    wins = team.wins,
                    losses = team.losses,
                    ties = team.ties,
                    points_for = team.points_for,
                    points_against = team.points_against,
                    waiver_rank = team.waiver_rank,
                    acquisitions = team.acquisitions,
                    acquisition_budget_spent = team.acquisition_budget_spent,
                    drops = team.drops,
                    trades = team.trades,
                    # owners = team.owners,
                    # stats = team.stats,
                    streak_type = team.streak_type,
                    streak_length = team.streak_length,
                    standing = team.standing,
                    final_standing = team.final_standing,
                    draft_projected_rank = team.draft_projected_rank,
                    playoff_pct = team.playoff_pct,
                    logo_url = team.logo_url,
                    # schedule = team.schedule,
                    # scores = team.scores,
                    # outcomes = team.outcomes
                )
             # Insert the team into the database
                db.add(new_team)
                db.commit()
                print(f"Inserted team: ID={team.team_id}, Name={team.team_name}")
            # # Fetch the draft picks for the league and year
            # picks = league.draft
            # # Iterate throug the draft picks and insert them into the database
            # for pick in picks:
            #     player_id = pick.playerId
            #     team_id = pick.team.team_id
            #     round_num = pick.round_num
            #     round_pick = pick.round_pick
            #     player_id = pick.playerId
            #     # Insert the draft pick into the database
            #     draft_picks = Draft(team_id=team_id, player_id=player_id, round_num=round_num, round_pick=round_pick, year=year)
            #     db.add(draft_picks)
            #     db.commit()
            #     print(f"Inserted draft pick: Team ID={team_id}, Player ID={player_id}, Round={round_num}, Pick={round_pick}, Year={year}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        db.close()



@contextmanager
def get_db_session():
    """Context manager for database sessions to ensure proper cleanup"""
    db = next(get_db())
    try:
        yield db
    finally:
        db.close()

def fetch_and_populate_league(start_year, end_year):
    """
    Fetch and populate league data with upsert functionality.
    Uses batch processing for better performance.
    """
    batch_size = 50  # Adjust based on your needs
    leagues_to_upsert = []
    
    try:
        for year in range(start_year, end_year + 1):
            try:
                league = League(LEAGUE_ID, year=year, swid=SWID, espn_s2=ESPN_S2)
                scoreboard = league.scoreboard(1) # Test to check scoreboard functionality.
                roster = league.teams[0].roster
                #"Matchup" available 2019 and later

                league_data = {
                    'leagueId': LEAGUE_ID,
                    'teamCount': len(league.teams),
                    'year': league.year,
                    'currentWeek': league.current_week,
                    'nflWeek': league.nfl_week
                }
                leagues_to_upsert.append(league_data)
                
                # Process in batches
                if len(leagues_to_upsert) >= batch_size:
                    _bulk_upsert_leagues(leagues_to_upsert)
                    leagues_to_upsert = []
                    
            except Exception as e:
                print(f"Error processing year {year}: {e}")
                continue
        
        # Process any remaining leagues
        if leagues_to_upsert:
            _bulk_upsert_leagues(leagues_to_upsert)
            
    except Exception as e:
        print(f"A critical error occurred: {e}")
        raise

def _bulk_upsert_leagues(leagues_data):
    """
    Perform bulk upsert operation for leagues.
    """
    with get_db_session() as db:
        stmt = insert(FFleague).values(leagues_data)
        
        # On conflict with unique year, update other fields
        stmt = stmt.on_conflict_do_update(
            index_elements=['year'],
            set_={
                'team_count': stmt.excluded.team_count,
                'current_week': stmt.excluded.current_week,
                'nfl_week': stmt.excluded.nfl_week
            }
        )
        
        try:
            db.execute(stmt)
            db.commit()
            print(f"Successfully upserted {len(leagues_data)} leagues")
        except Exception as e:
            db.rollback()
            print(f"Error during bulk upsert: {e}")
            raise
