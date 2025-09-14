from espn_api.football import League
from app.db.models import FFleague, Team, Draft, Player, Settings, Matchup, Roster
from app.db.session import get_db
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
import asyncio
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()
LEAGUE_ID = os.getenv("LEAGUE_ID")
ESPN_S2 = os.getenv("ESPN_S2")
SWID = os.getenv("SWID")

@contextmanager
def get_db_session():
    """Context manager for database sessions to ensure proper cleanup"""
    db = next(get_db())
    try:
        yield db
    finally:
        db.close()

    
def fetch_and_populate_draft(start_year, end_year):
    batch_size = 1000
    picks_to_upsert = []

    try:
        with get_db_session() as db:
            for year in range(start_year, end_year + 1):
                try:
                    league = League(LEAGUE_ID, year=year, swid=SWID, espn_s2=ESPN_S2)
                    draft = league.draft
                    for pick_index, pick in enumerate(draft, 1):

                        #Query the database to get the player ID from 'players' table and team id

                        #Get the playerId from players table
                        player_id = db.query(Player).filter(Player.espnId == pick.playerId).first().id
                        #Get the team id from teams table
                        team_id = db.query(Team).filter(Team.teamId == pick.team.team_id, Team.year == year).first().id
                        # Create a dictionary to store pick information
                        pick_info = {
                                'team_id': team_id,
                                'overallPick': pick_index,
                                'player_id': player_id,           # Integer ID of the player
                                'roundNum': pick.round_num,          # Integer round number
                                'roundPick': pick.round_pick,        # Integer pick number within the round
                                'bidAmount': pick.bid_amount,        # Integer bid amount (for auction drafts)
                                'keeperStatus': pick.keeper_status,  # Boolean keeper status
                                #'nominating_team_name': None
                            }
                        picks_to_upsert.append(pick_info)

                            # Process in batches
                        if len(picks_to_upsert) >= batch_size:
                            bulk_upsert_picks(picks_to_upsert)
                            picks_to_upsert = []
                            
                except Exception as e:
                    print(f"Error processing year {year}: {e}")
                    continue
                    
            # Process any remaining leagues
            if picks_to_upsert:
                bulk_upsert_picks(picks_to_upsert)
                
    except Exception as e:
            print(f"A critical error occurred: {e}")
            raise

def fetch_and_populate_draft_from_leagues(leagues: list[League]):
    batch_size = 2000
    picks_to_upsert = []

    try:
        with get_db_session() as db:
            for league in leagues:
                year=league.year
                try:
                    #league = League(LEAGUE_ID, year=year, swid=SWID, espn_s2=ESPN_S2)
                    draft = league.draft
                    for pick_index, pick in enumerate(draft, 1):

                        #Query the database to get the player ID from 'players' table and team id

                        #Get the playerId from players table
                        player_id = db.query(Player).filter(Player.espnId == pick.playerId).first().id
                        #Get the team id from teams table
                        team_id = db.query(Team).filter(Team.teamId == pick.team.team_id, Team.year == year).first().id
                        # Create a dictionary to store pick information
                        pick_info = {
                                'team_id': team_id,
                                'overallPick': pick_index,
                                'player_id': player_id,           # Integer ID of the player
                                'roundNum': pick.round_num,          # Integer round number
                                'roundPick': pick.round_pick,        # Integer pick number within the round
                                'bidAmount': pick.bid_amount,        # Integer bid amount (for auction drafts)
                                'keeperStatus': pick.keeper_status,  # Boolean keeper status
                                #'nominating_team_name': None
                            }
                        picks_to_upsert.append(pick_info)

                            # Process in batches
                        if len(picks_to_upsert) >= batch_size:
                            bulk_upsert_picks(picks_to_upsert)
                            picks_to_upsert = []
                            
                except Exception as e:
                    print(f"Error processing year {year}: {e}")
                    continue
                    
            # Process any remaining leagues
            if picks_to_upsert:
                bulk_upsert_picks(picks_to_upsert)
                
    except Exception as e:
            print(f"A critical error occurred: {e}")
            raise

def fetch_and_populate_leagues_from_leagues(leagues: list[League]):
    batch_size = 100
    leagues_to_upsert = []
    for league in leagues:
        try:
            league_data = {
                        'leagueId': league.league_id,
                        'teamCount': len(league.teams),
                        'year': league.year,
                        'currentWeek': league.current_week,
                        'nflWeek': league.nfl_week
                    }
            leagues_to_upsert.append(league_data)

            # Process in batches
            if len(leagues_to_upsert) >= batch_size:
                bulk_upsert_league(leagues_to_upsert)
                leagues_to_upsert = []
        except Exception as e:
            print(f"A critical error occurred {league.year}: {e}")
            raise

    # Process any remaining leagues
    if leagues_to_upsert:
        bulk_upsert_leagues(leagues_to_upsert)
                

def fetch_and_populate_roster_from_leagues(leagues: list[League]):
    batch_size = 100
    roster_to_upsert = []

    try:
        with get_db_session() as db:
            for league in leagues:
                year=league.year
                try:
                    for team in league.teams:
                        team_id = db.query(Team).filter(Team.teamId == team.team_id, Team.year == year).first().id


                        for player in team.roster:

                        #Query the database to get the player ID from 'players' table and team id

                            #Get the playerId from players table
                            player_id = db.query(Player).filter(Player.espnId == player.playerId).first().id
                            # Create a dictionary to store pick information
                            roster_info = {
                                    'team_id': team_id,
                                    'player_id': player_id,
                                    'rosterSlot': player.lineupSlot,
                                }
                            roster_to_upsert.append(roster_info)

                                # Process in batches
                            if len(roster_to_upsert) >= batch_size:
                                bulk_upsert_roster(roster_to_upsert)
                                roster_to_upsert = []
                            
                except Exception as e:
                    print(f"Error processing year {year}: {e}")
                    continue
                    
            # Process any remaining leagues
            if roster_to_upsert:
                bulk_upsert_roster(roster_to_upsert)
                
    except Exception as e:
            print(f"A critical error occurred: {e}")
            raise

def fetch_and_populate_matchups_from_leagues(leagues: list[League]):
    batch_size = 1000
    matchups_to_upsert = []
    try:
        with get_db_session() as db:
            for league in leagues:
                try:
                    year = league.year
                    # Retrieve all team IDs upfront - single query
                    teams_lookup = {team.teamId: team.id for team in 
                                    db.query(Team).filter(Team.year == year).all()}
                    
                    weeks = range(league.firstScoringPeriod, league.finalScoringPeriod+1)
                    for week in weeks:
                        scoreboard = league.scoreboard(week)
                        for matchup in scoreboard:
                            home_team_id = teams_lookup.get(matchup._home_team_id) if matchup._home_team_id != 0 else None
                            away_team_id = teams_lookup.get(matchup._away_team_id) if matchup._away_team_id != 0 else None
                            
                            matchup_info = {
                                'week': week,
                                'home_team_id': home_team_id,
                                'away_team_id': away_team_id,
                                'homeScore': matchup.home_score,
                                'awayScore': matchup.away_score,
                                'isPlayoff': matchup.is_playoff,
                                'matchupType': matchup.matchup_type
                            }
                            matchups_to_upsert.append(matchup_info)
                            
                            # Process in batches
                            if len(matchups_to_upsert) >= batch_size:
                                bulk_upsert_matchups(matchups_to_upsert)
                                matchups_to_upsert = []
                
                except Exception as e:
                    print(f"Error processing year {year}: {e}")
                    continue
            
            # Process any remaining leagues
            if matchups_to_upsert:
                bulk_upsert_matchups(matchups_to_upsert)
    
    except Exception as e:
        print(f"A critical error occurred: {e}")
        raise

def fetch_and_populate_settings_from_leagues(leagues: list[League]):
    """
    Fetch and populate league settings with upsert functionality.
    
    Uses batch processing for better performance.
    """
    batch_size = 100
    settings_to_upsert = []

    try:
        with get_db_session() as db:
            for league in leagues:
                try:
                    # Access the league's year
                    year = league.year
                    league_id = league.league_id

                    # Query the leagues table to get the league record for the current year
                    league_record = db.query(FFleague).filter(
                        FFleague.leagueId == league_id,
                        FFleague.year == year
                    ).first()

                    if not league_record:
                        print(f"League record not found for year {year}")
                        continue

                    # Fetch settings from the League object (assuming it's part of the League object)
                    settings = league.settings

                    # Prepare the dictionary for the league settings
                    league_settings = {
                        'league_id': league_record.id,
                        'regularSeasonCount': settings.reg_season_count,
                        'vetoVotesRequired': settings.veto_votes_required,
                        'teamCount': settings.team_count,
                        'playoffTeamCount': settings.playoff_team_count,
                        'keeperCount': settings.keeper_count,
                        'tradeDeadline': settings.trade_deadline,
                        'name': settings.name,
                        'tieRule': settings.tie_rule,
                        'playoffTieRule': settings.playoff_tie_rule,
                        'playoffSeedTieRule': settings.playoff_seed_tie_rule,
                        'playoffMatchupPeriodLength': settings.playoff_matchup_period_length,
                        'faab': settings.faab,
                    }

                    # Add the settings to the list of settings to upsert
                    settings_to_upsert.append(league_settings)

                    # Process in batches
                    if len(settings_to_upsert) >= batch_size:
                        bulk_upsert_settings(settings_to_upsert)
                        settings_to_upsert = []

                except Exception as e:
                    print(f"Error processing league {league.leagueId} for year {year}: {e}")
                    continue

            # Process any remaining settings
            if settings_to_upsert:
                bulk_upsert_settings(settings_to_upsert)

    except Exception as e:
        print(f"A critical error occurred: {e}")
        raise

def fetch_and_populate_players_from_leagues(leagues: list[League]):
    """
    Fetch and populate player data from a list of leagues.
    
    Uses batch processing for better performance.
    """
    batch_size = 1000
    players_to_upsert = []
    
    try:
        for league in leagues:
            try:
                year = league.year
                player_map = league.player_map  # Assuming player_map is a dict with ESPN ID and name
                
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
                print(f"Error processing league {league.leagueId} for year {league.year}: {e}")
                continue
        
        # Process any remaining players
        if players_to_upsert:
            bulk_upsert_players(players_to_upsert)
            
    except Exception as e:
        print(f"A critical error occurred: {e}")
        raise

def fetch_and_populate_teams_from_leagues(leagues: list[League]):
    """
    Fetch and populate team data from a list of leagues with upsert functionality.
    
    Uses batch processing for better performance.
    """
    batch_size = 100
    teams_to_upsert = []
    
    try:
        with get_db_session() as db:
            for league in leagues:
                try:
                    year = league.year
                    league_id = league.league_id
                    
                    # Query the leagues table to get the league record for the current year
                    league_record = db.query(FFleague).filter(
                        FFleague.leagueId == league_id,
                        FFleague.year == year
                    ).first()
                    
                    if not league_record:
                        print(f"League record not found for year {year}")
                        continue
                    
                    # Process each team in the league
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
                    print(f"Error processing league {league.leagueId} for year {league.year}: {e}")
                    continue
        
            # Process any remaining teams
            if teams_to_upsert:
                bulk_upsert_teams(teams_to_upsert)
                
    except Exception as e:
        print(f"A critical error occurred: {e}")
        raise

def fetch_and_populate_draft_from_leagues(leagues: list[League]):
    """
    Fetch and populate draft pick data from a list of leagues.
    
    Uses batch processing for better performance.
    """
    batch_size = 1000
    picks_to_upsert = []

    try:
        with get_db_session() as db:
            for league in leagues:
                try:
                    year = league.year
                    draft = league.draft  # Assuming this gives the draft information for the league

                    for pick_index, pick in enumerate(draft, 1):
                        # Query the database to get the player ID from the 'players' table and the team ID

                        # Get the player ID from the players table
                        player_id = db.query(Player).filter(Player.espnId == pick.playerId).first().id
                        # Get the team ID from the teams table
                        team_id = db.query(Team).filter(Team.teamId == pick.team.team_id, Team.year == year).first().id
                        if pick.nominatingTeam == None:
                            nominating_team_id = None
                        else:
                            nominating_team = db.query(Team).filter(Team.teamId == pick.nominatingTeam.team_id, Team.year == year).first()
                            nominating_team_id = nominating_team.id if nominating_team else None

                        # Create a dictionary to store pick information
                        pick_info = {
                            'team_id': team_id,
                            'overallPick': pick_index,
                            'player_id': player_id,           # Integer ID of the player
                            'roundNum': pick.round_num,       # Integer round number
                            'roundPick': pick.round_pick,     # Integer pick number within the round
                            'bidAmount': pick.bid_amount,     # Integer bid amount (for auction drafts)
                            'keeperStatus': pick.keeper_status, # Boolean keeper status
                            'nominating_team_id': nominating_team_id 
                        }
                        picks_to_upsert.append(pick_info)

                        # Process in batches
                        if len(picks_to_upsert) >= batch_size:
                            bulk_upsert_picks(picks_to_upsert)
                            picks_to_upsert = []

                except Exception as e:
                    print(f"Error processing league {league.league_id} for year {league.year}: {e}")
                    continue

            # Process any remaining draft picks
            if picks_to_upsert:
                bulk_upsert_picks(picks_to_upsert)

    except Exception as e:
        print(f"A critical error occurred: {e}")
        raise



def bulk_upsert_matchups(matchup_data):
    """
    Perform bulk upsert operation for picks using the unique constraint.
    """
    with get_db_session() as db:
        stmt = insert(Matchup).values(matchup_data)
        stmt = stmt.on_conflict_do_update(
            constraint='uix_matchup',  # Use the unique constraint instead
            set_={
                'homeScore': stmt.excluded.homeScore,
                'awayScore': stmt.excluded.awayScore,
                'isPlayoff': stmt.excluded.isPlayoff,
                'matchupType': stmt.excluded.matchupType
            }
        )
        try:
            db.execute(stmt)
            db.commit()
            print(f"Successfully upserted {len(matchup_data)} matchups")
        except Exception as e:
            db.rollback()
            print(f"Error during bulk upsert: {e}")
            raise


def bulk_upsert_picks(pick_data):
    """
    Perform bulk upsert operation for picks using the unique constraint.
    """
    with get_db_session() as db:
        stmt = insert(Draft).values(pick_data)
        stmt = stmt.on_conflict_do_update(
            constraint='uix_draft_pick',  # Use the unique constraint instead
            set_={
                'overallPick': stmt.excluded.overallPick,
                'roundNum': stmt.excluded.roundNum,          # Integer round number
                'roundPick': stmt.excluded.roundPick,        # Integer pick number within the round
                'bidAmount': stmt.excluded.bidAmount,        # Integer bid amount (for auction drafts)
                'keeperStatus': stmt.excluded.keeperStatus,  # Boolean keeper status
                'nominating_team_id': stmt.excluded.nominating_team_id
            }
        )
        try:
            db.execute(stmt)
            db.commit()
            print(f"Successfully upserted {len(pick_data)} draft picks")
        except Exception as e:
            db.rollback()
            print(f"Error during bulk upsert: {e}")
            raise

def bulk_upsert_roster(roster_data):
    """
    Perform bulk upsert operation for picks using the unique constraint.
    """
    with get_db_session() as db:
        stmt = insert(Roster).values(roster_data)
        stmt = stmt.on_conflict_do_update(
            constraint='uix_roster_team_player',  # Use the unique constraint instead
            set_={
                'rosterSlot': stmt.excluded.rosterSlot,
            }
        )
        try:
            db.execute(stmt)
            db.commit()
            print(f"Successfully upserted {len(roster_data)} roster entries")
        except Exception as e:
            db.rollback()
            print(f"Error during bulk upsert: {e}")
            raise

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

def bulk_upsert_settings(setting_data):
    """
    Perform bulk upsert operation for leagues using the unique constraint.
    """
    with get_db_session() as db:
        stmt = insert(Settings).values(setting_data)
        stmt = stmt.on_conflict_do_update(
            index_elements=['league_id'],  # Use the unique constraint instead
            set_={
                'regularSeasonCount': stmt.excluded.regularSeasonCount,
                'vetoVotesRequired': stmt.excluded.vetoVotesRequired,
                'teamCount': stmt.excluded.teamCount,
                'playoffTeamCount': stmt.excluded.playoffTeamCount,
                'keeperCount': stmt.excluded.keeperCount,
                'tradeDeadline': stmt.excluded.tradeDeadline,
                'name': stmt.excluded.name,
                'tieRule': stmt.excluded.tieRule,
                'playoffTieRule': stmt.excluded.playoffTieRule,
                'playoffSeedTieRule': stmt.excluded.playoffSeedTieRule,
                'playoffMatchupPeriodLength': stmt.excluded.playoffMatchupPeriodLength,
                'faab': stmt.excluded.faab,
            }
        )
        try:
            db.execute(stmt)
            db.commit()
            print(f"Successfully upserted {len(setting_data)} league settings")
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
            print(f"Successfully upserted {len(player_data)} players")
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

def fetch_and_populate_settings(start_year, end_year):
    """
    Fetch and populate league settings with upsert functionality.
    Uses batch processing for better performance.
    """
    batch_size = 100
    settings_to_upsert = []
    
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
                    settings = league.settings
                    
                    league_settings = {
                    'league_id': league_record.id,
                    'regularSeasonCount': settings.reg_season_count,
                    'vetoVotesRequired': settings.veto_votes_required,
                    'teamCount': settings.team_count,
                    'playoffTeamCount': settings.playoff_team_count,
                    'keeperCount': settings.keeper_count,
                    'tradeDeadline': settings.trade_deadline,
                    'name': settings.name,
                    'tieRule': settings.tie_rule,
                    'playoffTieRule': settings.playoff_tie_rule,
                    'playoffSeedTieRule': settings.playoff_seed_tie_rule,
                    'playoffMatchupPeriodLength': settings.playoff_matchup_period_length,
                    'faab': settings.faab,
                    }
                    settings_to_upsert.append(league_settings)
                    
                    # Process in batches
                    if len(settings_to_upsert) >= batch_size:
                        bulk_upsert_settings(settings_to_upsert)
                        settings_to_upsert = []
                        
                except Exception as e:
                    print(f"Error processing year {year}: {e}")
                    continue
                    
            # Process any remaining leagues
            if settings_to_upsert:
                bulk_upsert_settings(settings_to_upsert)
            
    except Exception as e:
        print(f"A critical error occurred: {e}")
        raise

def fetch_and_populate_players(start_year, end_year):
    """
    Fetch and populate player data.
    """
    batch_size = 1000
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

def fetch_draft_for_year(year):
    """
    Fetch draft data for a single year and return it.
    This function will be run in parallel for each year.
    """
    picks_to_upsert = []

    try:
        with get_db_session() as db:
            league = League(LEAGUE_ID, year=year, swid=SWID, espn_s2=ESPN_S2)
            draft = league.draft
            for pick_index, pick in enumerate(draft, 1):

                #Query the database to get the player ID from 'players' table and team id

                #Get the playerId from players table
                player_id = db.query(Player).filter(Player.espnId == pick.playerId).first().id
                #Get the team id from teams table
                team_id = db.query(Team).filter(Team.teamId == pick.team.team_id, Team.year == year).first().id
                # Create a dictionary to store pick information
                pick_info = {
                        'team_id': team_id,
                        'overallPick': pick_index,
                        'player_id': player_id,           # Integer ID of the player
                        'roundNum': pick.round_num,          # Integer round number
                        'roundPick': pick.round_pick,        # Integer pick number within the round
                        'bidAmount': pick.bid_amount,        # Integer bid amount (for auction drafts)
                        'keeperStatus': pick.keeper_status,  # Boolean keeper status
                        #'nominating_team_name': None
                    }
                picks_to_upsert.append(pick_info)

    except Exception as e:
        print(f"Error processing year {year}: {e}")
    
    return picks_to_upsert

def fetch_players_for_year(year):
    """
    Fetch player data for a single year and return it.
    This function will be run in parallel for each year.
    """
    players_to_upsert = []
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
        
    except Exception as e:
        print(f"Error processing year {year}: {e}")
    
    return players_to_upsert

def fetch_and_populate_players_concurrent(start_year, end_year):
    """
    Fetch and populate player data using threading for improved performance.
    """
    batch_size = 1000
    players_to_upsert = deque()  # Use deque for thread-safe appending and popping
    results = []
    
    # Initialize ThreadPoolExecutor with a suitable number of workers
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit tasks for each year
        futures = {executor.submit(fetch_players_for_year, year): year for year in range(start_year, end_year + 1)}

        for future in as_completed(futures):
            year = futures[future]
            try:
                year_players = future.result()
                players_to_upsert.extend(year_players)

                # Process in batches
                while len(players_to_upsert) >= batch_size:
                    batch = [players_to_upsert.popleft() for _ in range(batch_size)]
                    bulk_upsert_players(batch)

            except Exception as e:
                print(f"Error fetching players for year {year}: {e}")
        
        # Process any remaining players
        while players_to_upsert:
            batch = [players_to_upsert.popleft() for _ in range(min(batch_size, len(players_to_upsert)))]
            bulk_upsert_players(batch)

def fetch_and_populate_draft_concurrent(start_year, end_year):
    """
    Fetch and populate draft data using threading for improved performance.
    """
    batch_size = 1000
    picks_to_upsert = deque()  # Use deque for thread-safe appending and popping
    results = []
    
    # Initialize ThreadPoolExecutor with a suitable number of workers
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit tasks for each year
        futures = {executor.submit(fetch_draft_for_year, year): year for year in range(start_year, end_year + 1)}

        for future in as_completed(futures):
            year = futures[future]
            try:
                year_draft = future.result()
                picks_to_upsert.extend(year_draft)

                # Process in batches
                while len(picks_to_upsert) >= batch_size:
                    batch = [picks_to_upsert.popleft() for _ in range(batch_size)]
                    bulk_upsert_picks(batch)

            except Exception as e:
                print(f"Error fetching players for year {year}: {e}")
        
        # Process any remaining players
        while picks_to_upsert:
            batch = [picks_to_upsert.popleft() for _ in range(min(batch_size, len(picks_to_upsert)))]
            bulk_upsert_picks(batch)
            