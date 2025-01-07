import os
import shelve
from datetime import datetime
from typing import Optional
from app.services.asyncLeagueData import normalize_years, fetch_league_data
from espn_api.football import League
import asyncio

CACHE_DIR = "./shelf_cache"

def get_shelf_file(year: int, league_id: str) -> str:
    """Generate a file path for the shelf cache."""
    # Ensure the cache directory exists
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)
    return os.path.join(CACHE_DIR, f"league_cache")

def save_league_to_shelf(league) -> None:
    """Save a League object directly to a shelf file."""
    shelf_file = get_shelf_file(league.year, league.league_id)
    league_key = f"{league.year}_{league.league_id}"
    
    with shelve.open(shelf_file) as shelf:
        shelf[league_key] = {
            'league': league,
            'cached_at': datetime.now()
        }
    print(f"League for year {league.year} cached to {shelf_file}")

def load_league_from_shelf(year: int, league_id: str, max_age_days: float = 30.0) -> Optional[object]:
    """Load a League object from the shelf file if it exists and isn't expired."""
    shelf_file = get_shelf_file(year, league_id)
    league_key = f"{year}_{league_id}"
    
    if os.path.exists(shelf_file): 
        with shelve.open(shelf_file) as shelf:
            if league_key in shelf:
                cached_data = shelf[league_key]
                age = datetime.now() - cached_data['cached_at']
                
                if age.total_seconds() < max_age_days * 86400:  # 86400 seconds in a day
                    print(f"Loading league from cache: {shelf_file}")
                    return cached_data['league']
                else:
                    print(f"Cache expired (age: {age.total_seconds()/3600:.1f} hours)")
    return None

def fetch_league_with_cache(years: int, LEAGUE_ID: str, ESPN_S2: str, SWID: str, max_age_days: float = 30.0) -> list[League]:
    """Fetch a league with shelf-based caching."""

    leagues = []
    years_to_fetch = normalize_years(years)

    non_cached_years = []
    year_to_league_map = {}

    for year in years_to_fetch:
        cached_league = load_league_from_shelf(year, LEAGUE_ID, max_age_days)
        if cached_league == None:
            non_cached_years.append(year)
        else:
            print(f"Cache hit for league {LEAGUE_ID} in year {year}")
            year_to_league_map[year] = cached_league
    if non_cached_years: 
        print(f"Cache miss for years: {non_cached_years}")
        fetched_leagues = asyncio.run(fetch_league_data(non_cached_years, LEAGUE_ID, ESPN_S2, SWID))
        
        for year, league in zip(non_cached_years, fetched_leagues):
            print(f"Saving league {LEAGUE_ID} in year {year} to shelf")
            save_league_to_shelf(league)
            year_to_league_map[year] = league

        # Return leagues in the order of the requested years
    for year in years_to_fetch:
        leagues.append(year_to_league_map[year])

    return leagues