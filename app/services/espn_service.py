from espn_api.football import League
from app.db.models import FFleague, Team, Draft, Player, Settings, Matchup, Roster
#from app.db.session import SessionLocal  # Assuming you have a SessionLocal() function to get a database session
from app.db.session import get_db
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from contextlib import contextmanager
import pandas as pd

# Replace these with your actual values
LEAGUE_ID = '747376'
ESPN_S2 = 'AEAJA%2Fy8g05SnaVZ9KbodrBqefAHzG3RvXbFtbZuNcrMPOIwN%2F5fqKHSbwuZeAh6vVgrLOxmYT50SVKJ3wtd6LdYTOaIpe%2FG8ilQ6ijU5mZjdLfQzEW0DE1JYZTrSii%2Fi%2BcjgJcNeMFALOyeccy4ljD8tZwEbzJ5Pzp7KPk%2BLVS7H8C8RCwBnuuQUlMnlZSmS6sZz1W8F0qz06gxz5zzji4BbHp4%2BvUc%2FCzBGhDoIPXFz%2BYDteBxocWWvp3c3sYmqS6dQHeq3oQftD0C2B2yxIbGSarDN2DMj%2FLs18pKbYs6nQ%3D%3D'
SWID = '{A88F0FF7-2664-46A3-8F0F-F7266476A3C3}'

@contextmanager
def get_db_session():
    """Context manager for database sessions to ensure proper cleanup"""
    db = next(get_db())
    try:
        yield db
    finally:
        db.close()


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

def bulk_upsert_leagues(leagues_data):
    """
    Perform bulk upsert operation for leagues using the unique constraint.
    """
    with get_db_session() as db:
        stmt = insert(FFleague).values(leagues_data)
        stmt = stmt.on_conflict_do_update(
            constraint='uix_league_year',  # Use the unique constraint instead
            set_={
                'teamCount': stmt.excluded.teamCount,
                'currentWeek': stmt.excluded.currentWeek,
                'nflWeek': stmt.excluded.nflWeek
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

def bulk_upsert_players(player_data):
    """
    Perform bulk upsert operation for players using the unique constraint.
    """
    with get_db_session() as db:
        stmt = insert(Player).values(player_data)
        stmt = stmt.on_conflict_do_update(
            index_elements=['espnId'],
            set_={
                'position': stmt.excluded.position,
                'name': stmt.excluded.name,
            }
        )
        try:
            db.execute(stmt)
            db.commit()
            print(f"Successfully upserted {len(player_data)} playerss")
        except Exception as e:
            db.rollback()
            print(f"Error during bulk upsert: {e}")
            raise


def fetch_and_populate_league(start_year, end_year):
    """
    Fetch and populate league data with upsert functionality.
    Uses batch processing for better performance.
    """
    batch_size = 2000
    leagues_to_upsert = []
    
    try:
        for year in range(start_year, end_year + 1):
            try:
                league = League(LEAGUE_ID, year=year, swid=SWID, espn_s2=ESPN_S2)
                scoreboard = league.scoreboard(1)  # Test to check scoreboard functionality
                player = league.player_map[0]
                roster = league.teams[0].roster
                
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
                    bulk_upsert_leagues(leagues_to_upsert)
                    leagues_to_upsert = []
                    
            except Exception as e:
                print(f"Error processing year {year}: {e}")
                continue
                
        # Process any remaining leagues
        if leagues_to_upsert:
            bulk_upsert_leagues(leagues_to_upsert)
            
    except Exception as e:
        print(f"A critical error occurred: {e}")
        raise

def fetch_and_populate_players(start_year, end_year):
    """
    Fetch and populate player data.
    """
    batch_size = 50
    players_to_upsert = []
    
    try:
        for year in range(start_year, end_year + 1):
            try:
                league = League(LEAGUE_ID, year=year, swid=SWID, espn_s2=ESPN_S2)
                player_map = league.player_map
                
                for espnId, name in player_map.items():
                    if not isinstance(espnId, int):
                        continue

                    player_data = {
                        'espnId': espnId,
                        'name': name,
                    }
                    players_to_upsert.append(player_data)
                
                # Process in batches
                if len(players_to_upsert) >= batch_size:
                    bulk_upsert_players(players_to_upsert)
                    players_to_upsert = []
                    
            except Exception as e:
                print(f"Error processing year {year}: {e}")
                continue
                
        # Process any remaining leagues
        if players_to_upsert:
            bulk_upsert_players(players_to_upsert)
            
    except Exception as e:
        print(f"A critical error occurred: {e}")
        raise

def fetch_and_populate_teams(start_year, end_year):
    """
    Fetch and populate team data with upsert functionality.
    Uses batch processing for better performance.
    """
    batch_size = 100
    teams_to_upsert = []
    
    try:
        with get_db_session() as db:
            for year in range(start_year, end_year + 1):
                try:
                    # First, get the league_id from the leagues table
                    league_record = db.query(FFleague).filter(
                        FFleague.leagueId == LEAGUE_ID,
                        FFleague.year == year
                    ).first()
                    
                    if not league_record:
                        print(f"League record not found for year {year}")
                        continue
                        
                    league = League(LEAGUE_ID, year=year, swid=SWID, espn_s2=ESPN_S2)
                    scoreboard = league.scoreboard(1)
                    roster = league.teams[0].roster
                    
                    for team in league.teams:
                        owners = []
                        for owner in team.owners:
                            first_name = owner.get('firstName', 'Unknown')
                            last_name = owner.get('lastName', 'Unknown')
                            owners.append(first_name + " " + last_name)
                       # Prepare the data for upserting into the teams table
                        team_data = {
                            'teamId': team.team_id,
                            'league_id': league_record.id,  # Use the actual league.id here
                            'year': year,
                            'teamAbbrv': team.team_id,
                            'teamName': team.team_name,
                            'owners': ', '.join(owners),
                            'divisionId': team.division_id,
                            'divisionName': team.division_name,
                            'wins': team.wins,
                            'losses': team.losses,
                            'ties': team.ties,
                            'pointsFor': team.points_for,
                            'pointsAgainst': team.points_against,
                            'waiverRank': team.waiver_rank,
                            'acquisitions': team.acquisitions,
                            'acquisitionBudgetSpent': team.acquisition_budget_spent,
                            'drops': team.drops,
                            'trades': team.trades,
                            'streakType': team.streak_type,
                            'streakLength': team.streak_length,
                            'standing': team.standing,
                            'finalStanding': team.final_standing,
                            'draftProjRank': team.draft_projected_rank,
                            'playoffPct': team.playoff_pct,
                            'logoUrl': team.logo_url
                        }
                        teams_to_upsert.append(team_data)
                        
                        # Process in batches
                        if len(teams_to_upsert) >= batch_size:
                            bulk_upsert_teams(teams_to_upsert)
                            teams_to_upsert = []
                            
                except Exception as e:
                    print(f"Error processing year {year}: {e}")
                    continue
                    
            # Process any remaining teams
            if teams_to_upsert:
                bulk_upsert_teams(teams_to_upsert)
                
    except Exception as e:
        print(f"A critical error occurred: {e}")
        raise

def bulk_upsert_teams(teams_data):
    """
    Perform bulk upsert operation for teams using the unique constraint.
    """
    with get_db_session() as db:
        stmt = insert(Team).values(teams_data)
        stmt = stmt.on_conflict_do_update(
            constraint='uix_team_year',
            set_={
                'league_id': stmt.excluded.league_id,
                'teamAbbrv': stmt.excluded.teamAbbrv,
                'teamName': stmt.excluded.teamName,
                'divisionId': stmt.excluded.divisionId,
                'divisionName': stmt.excluded.divisionName,
                'owners': stmt.excluded.owners,
                'wins': stmt.excluded.wins,
                'losses': stmt.excluded.losses,
                'ties': stmt.excluded.ties,
                'pointsFor': stmt.excluded.pointsFor,
                'pointsAgainst': stmt.excluded.pointsAgainst,
                'waiverRank': stmt.excluded.waiverRank,
                'acquisitions': stmt.excluded.acquisitions,
                'acquisitionBudgetSpent': stmt.excluded.acquisitionBudgetSpent,
                'drops': stmt.excluded.drops,
                'trades': stmt.excluded.trades,
                'streakType': stmt.excluded.streakType,
                'streakLength': stmt.excluded.streakLength,
                'standing': stmt.excluded.standing,
                'finalStanding': stmt.excluded.finalStanding,
                'draftProjRank': stmt.excluded.draftProjRank,
                'playoffPct': stmt.excluded.playoffPct,
                'logoUrl': stmt.excluded.logoUrl
            }
        )
        try:
            db.execute(stmt)
            db.commit()
            print(f"Successfully upserted {len(teams_data)} teams")
        except Exception as e:
            db.rollback()
            print(f"Error during bulk upsert: {e}")
            raise