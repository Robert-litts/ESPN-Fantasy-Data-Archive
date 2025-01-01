from sqlalchemy import Column, Integer, String, ForeignKey, Text, Float, JSON, Boolean, UniqueConstraint, Index
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class FFleague(Base):
    __tablename__ = 'leagues'
    leagueId = Column(Integer, primary_key=True)
    teamCount = Column(Integer)
    year = Column(Integer, unique=True, nullable=False)  # Added to store the league year
    currentWeek = Column(Integer)  # Added for current fantasy football week
    nflWeek = Column(Integer)  # Added for current NFL week

     # Relationships
    settings = relationship("Settings", back_populates="league")
    teams = relationship("Team", back_populates="league")
    players = relationship("Player", back_populates="league")
    
    __table_args__ = (
        UniqueConstraint('year', name='uix_league_year'),
        Index('idx_league_year', 'year')
    )


class Settings(Base):
    __tablename__ = 'settings'

    settingsId = Column(Integer, primary_key=True)
    leagueId = Column(Integer, ForeignKey('leagues.leagueId'), nullable=False)  # Link to the League
    year = Column(Integer, ForeignKey('leagues.year'), nullable=False)  # Year of the settings
    regularSeasonCount = Column(Integer)  # Regular season count (integer)
    vetoVotesRequired = Column(Integer)  # Veto votes required (integer)
    teamCount = Column(Integer)  # Total number of teams (integer)
    playoffTeamCount = Column(Integer)  # Playoff team count (integer)
    keeperCount = Column(Integer)  # Keeper count (integer)
    tradeDeadline = Column(Integer)  # Trade deadline (epoch timestamp)
    name = Column(String(255))  # League name (string)
    tieRule = Column(Integer)  # Tie rule (integer)
    playoffTieRule = Column(Integer)  # Playoff tie rule (integer)
    playoffSeedTieRule = Column(Integer)  # Playoff seed tie rule (integer)
    playoffMatchupPeriodLength = Column(Integer)  # Weeks per playoff matchup (integer)
    faab = Column(Boolean)  # Whether the league uses FAAB (boolean)
    
    # Relationships
    league = relationship("FFLeague", back_populates="settings")
    
    __table_args__ = (
        UniqueConstraint('leagueId', 'year', name='uix_settings_league_year'),
        Index('idx_settings_league_year', 'leagueId', 'year')
    )

class Team(Base):
    __tablename__ = 'teams'
    teamId = Column(Integer, primary_key=True)  # Team internal database ID
    leagueId = Column(Integer, ForeignKey('leagues.leagueId'), nullable=False)  # Link to the League
    year = Column(Integer, ForeignKey('leagues.year'), nullable=False)  # Year of the settings
    teamAbbrv = Column(String(10), nullable=False)  # Team abbreviation
    teamName = Column(String(255), nullable=False)  # Full team name
    divisionId = Column(String(255))  # Division ID (string format)
    divisionName = Column(String(255))  # Division name
    wins = Column(Integer, default=0)  # Number of wins
    losses = Column(Integer, default=0)  # Number of losses
    ties = Column(Integer, default=0)  # Number of ties
    pointsFor = Column(Integer, default=0)  # Points scored throughout the season
    pointsAgainst = Column(Integer, default=0)  # Points scored against the team
    waiverRank = Column(Integer)  # Waiver rank
    acquisitions = Column(Integer, default=0)  # Number of acquisitions made
    acquisitionBudgetSpent = Column(Integer, default=0)  # Budget spent on acquisitions
    drops = Column(Integer, default=0)  # Number of drops made
    trades = Column(Integer, default=0)  # Number of trades made
    streakType = Column(String(50))  # WIN or LOSS (string)
    streakLength = Column(Integer)  # Streak length
    standing = Column(Integer)  # Team's standing before playoffs
    finalStanding = Column(Integer)  # Final standing at the end of the season
    draftProjRank= Column(Integer)  # Projected draft rank
    playoffPct = Column(Integer)  # Projected playoff percentage
    logoUrl = Column(String(255))  # URL for the team logo
   
   # Relationships
    league = relationship("FFLeague", back_populates="teams")
    rosters = relationship("Roster", back_populates="team")
    home_matchups = relationship("Matchup", foreign_keys="[Matchup.homeTeamId]", back_populates="home_team")
    away_matchups = relationship("Matchup", foreign_keys="[Matchup.awayTeamId]", back_populates="away_team")
    activities = relationship("Activity", back_populates="team")
    draft_picks = relationship("Draft", back_populates="team", foreign_keys="[Draft.teamId]")
    draft_nominations = relationship("Draft", back_populates="nominating_team", foreign_keys="[Draft.nominatingTeamId]")

    __table_args__ = (
        UniqueConstraint('teamId', 'year', name='uix_team_year'),
        Index('idx_team_league_year', 'leagueId', 'year')
    )


class Player(Base):
    __tablename__ = 'players'
    playerId = Column(Integer, primary_key=True)
    leagueId = Column(Integer, ForeignKey('leagues.leagueId'), nullable=False)  # Link to the League
    year = Column(Integer, ForeignKey('leagues.year'), nullable=False)  # Year of the settings
    name = Column(String(255), nullable=False)
    position = Column(String(50))
    proTeam = Column(String(50))
    posRank = Column(Integer)
    acquisitionType = Column(String(50))
    points = Column(Float, default=0)
    projected_points = Column(Float, default=0)

      # Relationships
    league = relationship("FFLeague", back_populates="players")
    rosters = relationship("Roster", back_populates="player")
    activities = relationship("Activity", back_populates="player")
    drafts = relationship("Draft", back_populates="player")

    __table_args__ = (
        Index('idx_player_league_year', 'leagueId', 'year'),
        Index('idx_player_name', 'name'),
        Index('idx_player_position', 'position')
    )

class Draft(Base):
    __tablename__ = 'drafts'
    
    draftId = Column(Integer, primary_key=True)
    leagueId = Column(Integer, ForeignKey('leagues.leagueId'), nullable=False)  # Link to the League
    year = Column(Integer, ForeignKey('leagues.year'), nullable=False)  # Year of the settings
    teamId = Column(Integer, ForeignKey('teams.teamId'), nullable=False)  # The team making the pick
    playerId = Column(Integer, ForeignKey('players.playerId'), nullable=False)  # The player being picked
    roundNum = Column(Integer, nullable=False)  # Round number in which the pick is made
    roundPick = Column(Integer, nullable=False)  # The pick position within the round
    keeperStatus = Column(Boolean, default=False)  # Whether the player is a keeper (default is False)
    bidAmount = Column(Integer, nullable=True, default=None)    
    nominatingTeamId = Column(Integer, ForeignKey('teams.teamId'), nullable=True, default=None)    

    
    # Relationships
    team = relationship("Team", back_populates="draft_picks", foreign_keys=[teamId])
    player = relationship("Player", back_populates="drafts")
    nominating_team = relationship("Team", back_populates="draft_nominations", foreign_keys=[nominatingTeamId])

    __table_args__ = (
        UniqueConstraint('teamId', 'playerId', 'year', name='uix_draft_pick'),
        Index('idx_draft_league_year', 'leagueId', 'year'),
        Index('idx_draft_team', 'teamId')
    )

class Matchup(Base):
    __tablename__ = 'matchups'

    matchupId = Column(Integer, primary_key=True)
    leagueId = Column(Integer, ForeignKey('leagues.leagueId'), nullable=False)  # Link to the League
    year = Column(Integer, ForeignKey('leagues.year'), nullable=False)  # Year of the settings
    week = Column(Integer, nullable=False)  # The week in which the matchup occurs
    homeTeamId = Column(Integer, ForeignKey('teams.teamId'), nullable=False)  # Foreign key to home team
    awayTeamId = Column(Integer, ForeignKey('teams.teamId'), nullable=False)  # Foreign key to away team
    homeScore = Column(Float, nullable=True)  # Score for home team
    awayScore = Column(Float, nullable=True)  # Score for away team
    isPlayoff = Column(Boolean, default=False)  # Whether the matchup is part of the playoffs
    matchupType = Column(String(50), default='NONE')  # Type of matchup (e.g., 'WINNERS_BRACKET')
    winnerTeamId = Column(Integer, ForeignKey('teams.teamId'), nullable=True)  # ID of the winning team

    # Relationships
    home_team = relationship("Team", foreign_keys=[homeTeamId], back_populates="home_matchups")
    away_team = relationship("Team", foreign_keys=[awayTeamId], back_populates="away_matchups")
    winner = relationship("Team", foreign_keys=[winnerTeamId])

    __table_args__ = (
        Index('idx_matchup_league_year', 'leagueId', 'year'),
        Index('idx_matchup_week', 'week')
    )

class Activity(Base):
    __tablename__ = 'activities'
    activityId = Column(Integer, primary_key=True)
    leagueId = Column(Integer, ForeignKey('leagues.leagueId'), nullable=False)  # Link to the League
    year = Column(Integer, ForeignKey('leagues.year'), nullable=False)  # Year of the settings
    teamId = Column(Integer, ForeignKey('teams.teamId'))
    playerId = Column(Integer, ForeignKey('players.playerId'), nullable=False)  # The player being picked
    bidAmount = Column(Float)
    action = Column(String(50))

     # Relationships
    team = relationship("Team", back_populates="activities")
    player = relationship("Player", back_populates="activities")

    __table_args__ = (
        Index('idx_activity_league_year', 'leagueId', 'year'),
        Index('idx_activity_team', 'teamId')
    )

class Roster(Base):
    __tablename__ = 'rosters'
    rosterId = Column(Integer, primary_key=True)
    leagueId = Column(Integer, ForeignKey('leagues.leagueId'), nullable=False)  # Link to the League
    year = Column(Integer, ForeignKey('leagues.year'), nullable=False)  # Year of the settings
    teamId = Column(Integer, ForeignKey('teams.teamId'), nullable=False)  # Link to the Team
    playerId = Column(Integer, ForeignKey('players.playerId'), nullable=False)  # The player on the roster
    rosterSlot = Column(String(50))

      # Relationships
    team = relationship("Team", back_populates="rosters")
    player = relationship("Player", back_populates="rosters")

    __table_args__ = (
        UniqueConstraint('teamId', 'playerId', 'year', name='uix_roster_team_player_year'),
        Index('idx_roster_league_year', 'leagueId', 'year'),
        Index('idx_roster_team', 'teamId')
    )