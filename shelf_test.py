import shelve

# Open the shelf file (change 'my_shelf.db' to your actual filename, without the .db extension)
with shelve.open('shelf_cache/league_cache') as shelf:
    for key, data in shelf.items():
        league = data['league']
        cached_at = data['cached_at']
        print(f"\n{key}:")
        print(f"  Cached at: {cached_at}")
        print(f"  League ID: {league.league_id}")
        print(f"  Season:    {league.year}")
        print(f"  Teams: {[team.team_name for team in league.teams]}")
        print(f"  Team Count: {len(league.teams)}")