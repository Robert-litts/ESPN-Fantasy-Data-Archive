from espn_api.football import League
LEAGUE_ID = '747376'
#LEAGUE_ID = '368876'
ESPN_S2 = 'AEAJA%2Fy8g05SnaVZ9KbodrBqefAHzG3RvXbFtbZuNcrMPOIwN%2F5fqKHSbwuZeAh6vVgrLOxmYT50SVKJ3wtd6LdYTOaIpe%2FG8ilQ6ijU5mZjdLfQzEW0DE1JYZTrSii%2Fi%2BcjgJcNeMFALOyeccy4ljD8tZwEbzJ5Pzp7KPk%2BLVS7H8C8RCwBnuuQUlMnlZSmS6sZz1W8F0qz06gxz5zzji4BbHp4%2BvUc%2FCzBGhDoIPXFz%2BYDteBxocWWvp3c3sYmqS6dQHeq3oQftD0C2B2yxIbGSarDN2DMj%2FLs18pKbYs6nQ%3D%3D'
SWID = '{A88F0FF7-2664-46A3-8F0F-F7266476A3C3}'
league = League(LEAGUE_ID, year=2019, swid=SWID, espn_s2=ESPN_S2)
#league = League(LEAGUE_ID, year=2018)

draft = league.draft
processed_picks = []
for pick_index, pick in enumerate(draft, 1):
    # Create a dictionary to store pick information
    pick_info = {
            'overall_pick': pick_index,
            'player_id': pick.playerId,           # Integer ID of the player
            'player_name': pick.playerName,       # String name of the player
            'round_num': pick.round_num,          # Integer round number
            'round_pick': pick.round_pick,        # Integer pick number within the round
            'bid_amount': pick.bid_amount,        # Integer bid amount (for auction drafts)
            'keeper_status': pick.keeper_status,  # Boolean keeper status
            
            # For team objects, we need to extract the team name
            # The team attribute is a Team class instance
            'team_name': pick.team.team_name if pick.team else None,
            
            # The nominatingTeam is only present in auction drafts
            # We'll extract the team name if it exists
            'nominating_team_name': (pick.nominatingTeam.team_name 
                                   if hasattr(pick, 'nominatingTeam') and pick.nominatingTeam 
                                   else None)
        }
    processed_picks.append(pick_info)

#print(processed_picks)


#########___________HANDLE TEAMS________#####################3

# for year in range (2013, 2021+1):
#         league = League(LEAGUE_ID, year=year)
#         print(year)
#         print(league.teams)
#         for team in league.teams:
#                 for owner in team.owners:
#                         first_name = owner.get('firstName', 'Unknown')
#                         last_name = owner.get('lastName', 'Unknown')
#                         print(team.team_name, first_name, last_name, 'final standing: ', team.final_standing)

#                 for player in team.roster:
#                         name = player.name
#                         playerId = player.playerId
#                         print('player ID: ', playerId, 'Name: ', name)

league = League(LEAGUE_ID, year=2013)
player_map = league.player_map
players_to_upsert = []
for key, value in player_map.items():
    print(key, value)
    players_to_upsert.append(value)






       