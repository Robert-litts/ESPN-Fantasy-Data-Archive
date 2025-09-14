import os
import shelve
from datetime import datetime
from typing import Optional, List
from app.services.asyncLeagueData import normalize_years, fetch_league_data
from espn_api.football import League
import asyncio
import glob

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

def load_league_from_shelf(year: int, league_id: str, max_age_days: float = 600.0) -> Optional[object]:
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

def clear_cache_for_league(league_id: str, years: Optional[List[int]] = None) -> None:
    """
    Clear cached data for a specific league and optionally specific years.
    
    Args:
        league_id: League ID to clear cache for
        years: Optional list of specific years to clear. If None, clears all years.
    """
    try:
        # Pattern to find cache files
        cache_files = glob.glob(os.path.join(CACHE_DIR, "league_cache*"))
        
        for cache_file in cache_files:
            try:
                with shelve.open(cache_file) as shelf:
                    keys_to_delete = []
                    
                    for key in shelf.keys():
                        # Key format is "year_league_id"
                        if f"_{league_id}" in key:
                            if years is None:
                                # Clear all years for this league
                                keys_to_delete.append(key)
                            else:
                                # Only clear specific years
                                key_year = int(key.split('_')[0])
                                if key_year in years:
                                    keys_to_delete.append(key)
                    
                    # Delete the keys
                    for key in keys_to_delete:
                        del shelf[key]
                        year = key.split('_')[0]
                        print(f"Cleared cache for league {league_id}, year {year}")
                        
            except Exception as e:
                print(f"Error clearing cache file {cache_file}: {e}")
                
    except Exception as e:
        print(f"Error during cache clearing: {e}")

def check_cache_exists(year: int, league_id: str, max_age_days: float = 600.0) -> bool:
    """
    Check if valid cached data exists for a specific year and league.
    
    Args:
        year: Year to check
        league_id: League ID to check
        max_age_days: Maximum age of cache in days
        
    Returns:
        True if valid cache exists, False otherwise
    """
    cached_league = load_league_from_shelf(year, league_id, max_age_days)
    return cached_league is not None

def get_cache_status(years: List[int], league_id: str, max_age_days: float = 600.0) -> dict:
    """
    Get cache status for multiple years.
    
    Args:
        years: List of years to check
        league_id: League ID to check
        max_age_days: Maximum age of cache in days
        
    Returns:
        Dictionary with year as key and cache status (bool) as value
    """
    status = {}
    for year in years:
        status[year] = check_cache_exists(year, league_id, max_age_days)
    return status

def fetch_league_with_cache(
    years: int, 
    LEAGUE_ID: str, 
    ESPN_S2: str, 
    SWID: str, 
    max_age_days: float = 10,
    use_cache: bool = True,
    force_refresh: bool = False
) -> List[League]:
    """
    Fetch a league with shelf-based caching and cache control options.
    
    Args:
        years: Years to fetch data for
        LEAGUE_ID: ESPN League ID
        ESPN_S2: ESPN S2 cookie
        SWID: ESPN SWID cookie
        max_age_days: Maximum age of cached data in days
        use_cache: Whether to use cached data (default: True)
        force_refresh: Whether to force refresh cache (default: False)
        
    Returns:
        List of League objects
    """
    leagues = []
    years_to_fetch = normalize_years(years)
    
    # Handle force refresh - clear cache first
    if force_refresh:
        print(f"Force refresh enabled - clearing cache for league {LEAGUE_ID}")
        clear_cache_for_league(LEAGUE_ID, years_to_fetch)
        # Continue with normal flow to fetch and cache
    
    # Handle no cache mode
    if not use_cache:
        print(f"Cache disabled - fetching fresh data for years {years_to_fetch}")
        # Fetch directly from API without checking or saving to cache
        fetched_leagues = asyncio.run(fetch_league_data(years_to_fetch, LEAGUE_ID, ESPN_S2, SWID))
        return fetched_leagues
    
    # Normal cache behavior (use_cache=True)
    non_cached_years = []
    year_to_league_map = {}
    
    # Check cache for each year
    for year in years_to_fetch:
        cached_league = load_league_from_shelf(year, LEAGUE_ID, max_age_days)
        if cached_league is None:
            non_cached_years.append(year)
        else:
            print(f"Cache hit for league {LEAGUE_ID} in year {year}")
            year_to_league_map[year] = cached_league
    
    # Fetch missing years from API
    if non_cached_years:
        print(f"Cache miss for years: {non_cached_years}")
        fetched_leagues = asyncio.run(fetch_league_data(non_cached_years, LEAGUE_ID, ESPN_S2, SWID))
        
        # Save to cache and map
        for year, league in zip(non_cached_years, fetched_leagues):
            print(f"Saving league {LEAGUE_ID} in year {year} to shelf")
            save_league_to_shelf(league)
            year_to_league_map[year] = league
    
    # Return leagues in the order of the requested years
    print(f"Final year to league mapping: {list(year_to_league_map.keys())}")
    for year in years_to_fetch:
        leagues.append(year_to_league_map[year])
    
    return leagues

# Convenience function for backwards compatibility
def fetch_league_with_cache_simple(years: int, LEAGUE_ID: str, ESPN_S2: str, SWID: str, max_age_days: float = 10) -> List[League]:
    """
    Simple wrapper for backwards compatibility with existing code.
    Uses default cache behavior (use_cache=True, force_refresh=False).
    """
    return fetch_league_with_cache(years, LEAGUE_ID, ESPN_S2, SWID, max_age_days)

def fetch_league_no_cache(years: int, LEAGUE_ID: str, ESPN_S2: str, SWID: str) -> List[League]:
    """
    Convenience function to fetch league data without using cache.
    
    Args:
        years: Years to fetch data for
        LEAGUE_ID: ESPN League ID  
        ESPN_S2: ESPN S2 cookie
        SWID: ESPN SWID cookie
        
    Returns:
        List of League objects
    """
    return fetch_league_with_cache(years, LEAGUE_ID, ESPN_S2, SWID, use_cache=False)

def fetch_league_force_refresh(years: int, LEAGUE_ID: str, ESPN_S2: str, SWID: str, max_age_days: float = 365) -> List[League]:
    """
    Convenience function to force refresh cache and fetch fresh data.
    
    Args:
        years: Years to fetch data for
        LEAGUE_ID: ESPN League ID
        ESPN_S2: ESPN S2 cookie  
        SWID: ESPN SWID cookie
        max_age_days: Maximum age for future cache
        
    Returns:
        List of League objects
    """
    return fetch_league_with_cache(years, LEAGUE_ID, ESPN_S2, SWID, max_age_days, force_refresh=True)