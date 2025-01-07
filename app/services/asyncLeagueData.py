from datetime import datetime
import asyncio, time
from espn_api.football import League
async def fetch_league_data(years, LEAGUE_ID, ESPN_S2, SWID):
    try:
        years_to_fetch = normalize_years(years)
        validate_years(years_to_fetch)

        tasks = [fetch_league_year(year, LEAGUE_ID, ESPN_S2, SWID) for year in years_to_fetch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        leagues = []
        for year, result in zip(years_to_fetch, results):
            if isinstance(result, Exception):
                print(f"Failed to fetch data for year {year}: {result}")
            else:
                leagues.append(result)
                print(f"Successfully fetched data for year {year}")
        return leagues
    except ValueError as ve:
        print(f"Validation Error: {ve}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise
        

async def fetch_league_year(year, LEAGUE_ID, ESPN_S2, SWID) -> League:
    """Fetches leagues data from ESPN"""
    #use async to get one league data from ESPN"""

    return League(LEAGUE_ID, year=year, swid=SWID, espn_s2=ESPN_S2)



def normalize_years(year_input):
    """Normalize year input to a list of years."""
    if isinstance(year_input, int):  # Single year
        return [year_input]
    elif isinstance(year_input, range):  # Range of years
        return list(year_input)
    elif isinstance(year_input, list):  # List of years
        return year_input
    else:
        raise ValueError("Year input must be an int, range, or list of integers.")

    validate_years(years_to_fetch)
    return years_to_fetch

def validate_years(years):
    """Ensure all years are valid integers and within an acceptable range."""
    current_year = datetime.now().year
    for year in years:
        if not isinstance(year, int):
            raise ValueError(f"Year {year} is not an integer.")
        if not (1900 <= year <= current_year): 
            raise ValueError(f"Year {year} is out of the valid range.")