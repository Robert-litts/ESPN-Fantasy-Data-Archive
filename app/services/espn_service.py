from espn_api.football import League
from app.db.models import FFleague, Team, Draft, Player, Settings, Matchup, Roster
#from app.db.session import SessionLocal  # Assuming you have a SessionLocal() function to get a database session
from app.db.session import get_db
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
import asyncio

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

def fetch_and_populate_matchups_from_leagues(leagues: list[League]):
    batch_size = 2000
    picks_to_upsert = []

    try:
        with get_db_session() as db:
            for league in leagues:
                year=league.year
                weeks = len(league.teams[0].schedule)
                for week in weeks:
                try:
                    #league = League(LEAGUE_ID, year=year, swid=SWID, espn_s2=ESPN_S2)
                    activity = league.activity
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
            print(f"Successfully upserted {len(setting_data)} leagues")
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
            