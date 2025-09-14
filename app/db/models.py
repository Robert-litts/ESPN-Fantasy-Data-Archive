from sqlalchemy import Column, Integer, String, ForeignKey, Text, Float, JSON, Boolean, UniqueConstraint, Index, BigInteger
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class FFleague(Base):
    __tablename__ = 'leagues'
    id = Column(Integer, primary_key=True)
    leagueId = Column(Integer, nullable=False)  # ESPN's league ID
    year = Column(Integer, nullable=False)  # League year
    teamCount = Column(Integer, nullable=False)
    currentWeek = Column(Integer, nullable=False, default=0)
    nflWeek = Column(Integer, nullable=False, default=0)

    # Relationships
    settings = relationship("Settings", back_populates="league")
    teams = relationship("Team", back_populates="league")
    
    __table_args__ = (
        UniqueConstraint('leagueId', 'year', name='uix_league_year'),
        Index('idx_league_composite', 'leagueId', 'year')
    )

class Settings(Base):
    __tablename__ = 'settings'
    id = Column(Integer, primary_key=True, autoincrement=True)
    league_id = Column(Integer, ForeignKey('leagues.id'), nullable=False, unique=True)
    regularSeasonCount = Column(Integer)
    vetoVotesRequired = Column(Integer)
    teamCount = Column(Integer)
    playoffTeamCount = Column(Integer)
    keeperCount = Column(Integer)
    tradeDeadline = Column(BigInteger)
    name = Column(String(255))
    tieRule = Column(String(50))
    playoffTieRule = Column(String(50))
    playoffSeedTieRule = Column(String(50))
    playoffMatchupPeriodLength = Column(Integer)
    faab = Column(Boolean)
    
    # Relationships
    league = relationship("FFleague", back_populates="settings")
    
    __table_args__ = (
        Index('idx_settings_league', 'league_id'),
    )

class Team(Base):
    __tablename__ = 'teams'
    id = Column(Integer, primary_key=True, autoincrement=True)
    league_id = Column(Integer, ForeignKey('leagues.id'), nullable=False)
    teamId = Column(Integer, nullable=False)  # ESPN's team ID
    year = Column(Integer, nullable=False)
    teamAbbrv = Column(String(10), nullable=False)
    teamName = Column(String(255), nullable=False)
    owners = Column(String(50))
    divisionId = Column(String(255))
    divisionName = Column(String(255))
    wins = Column(Integer, default=0)
    losses = Column(Integer, default=0)
    ties = Column(Integer, default=0)
    pointsFor = Column(Integer, default=0)
    pointsAgainst = Column(Integer, default=0)
    waiverRank = Column(Integer)
    acquisitions = Column(Integer, default=0)
    acquisitionBudgetSpent = Column(Integer, default=0)
    drops = Column(Integer, default=0)
    trades = Column(Integer, default=0)
    streakType = Column(String(50))
    streakLength = Column(Integer)
    standing = Column(Integer)
    finalStanding = Column(Integer)
    draftProjRank = Column(Integer)
    playoffPct = Column(Integer)
    logoUrl = Column(String(255))
   
    # Relationships
    league = relationship("FFleague", back_populates="teams")
    rosters = relationship("Roster", back_populates="team")
    home_matchups = relationship("Matchup", foreign_keys="[Matchup.home_team_id]", back_populates="home_team")
    away_matchups = relationship("Matchup", foreign_keys="[Matchup.away_team_id]", back_populates="away_team")
    activities = relationship("Activity", back_populates="team")
    draft_picks = relationship("Draft", back_populates="team", foreign_keys="[Draft.team_id]")
    draft_nominations = relationship("Draft", back_populates="nominating_team", foreign_keys="[Draft.nominating_team_id]")

    __table_args__ = (
        UniqueConstraint('teamId', 'year', name='uix_team_year'),
        Index('idx_team_league_year', 'league_id', 'year'),
        Index('idx_team_year', 'teamId', 'year')
    )

class Player(Base):
    __tablename__ = 'players'
    id = Column(Integer, primary_key=True)
    espnId = Column(Integer, unique=True, nullable=False)
    name = Column(String(255), nullable=False)
    position = Column(String(50), nullable=True)

    # Relationships
    rosters = relationship("Roster", back_populates="player")
    activities = relationship("Activity", back_populates="player")
    drafts = relationship("Draft", back_populates="player")

    __table_args__ = (
        Index('idx_player_name', 'name'),
        Index('idx_player_position', 'position')
    )

class Draft(Base):
    __tablename__ = 'drafts'
    id = Column(Integer, primary_key=True, autoincrement=True)
    team_id = Column(Integer, ForeignKey('teams.id'), nullable=False)
    player_id = Column(Integer, ForeignKey('players.id'), nullable=False)
    overallPick = roundNum = Column(Integer, nullable=False)
    roundNum = Column(Integer, nullable=False)
    roundPick = Column(Integer, nullable=False)
    keeperStatus = Column(Boolean, default=False, nullable=False)
    bidAmount = Column(Integer, default=-1, nullable=False)
    nominating_team_id = Column(Integer, ForeignKey('teams.id'))

    # Relationships
    team = relationship("Team", back_populates="draft_picks", foreign_keys=[team_id])
    player = relationship("Player", back_populates="drafts")
    nominating_team = relationship("Team", back_populates="draft_nominations", foreign_keys=[nominating_team_id])

    __table_args__ = (
        UniqueConstraint('team_id', 'player_id', name='uix_draft_pick'),
        Index('idx_draft_team_player_year', 'team_id', 'player_id')
    )

class Matchup(Base):
    __tablename__ = 'matchups'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    week = Column(Integer, nullable=False)
    home_team_id = Column(Integer, ForeignKey('teams.id'), nullable=True)
    away_team_id = Column(Integer, ForeignKey('teams.id'), nullable=True)
    homeScore = Column(Float, nullable=False, default=0.0)
    awayScore = Column(Float, nullable=False, default=0.0)
    isPlayoff = Column(Boolean, default=False, nullable=False)
    matchupType = Column(String(50), nullable=False, default='NONE')
    
    # Relationships
    home_team = relationship("Team", foreign_keys=[home_team_id], back_populates="home_matchups")
    away_team = relationship("Team", foreign_keys=[away_team_id], back_populates="away_matchups")
    
    __table_args__ = (
        UniqueConstraint('week', 'home_team_id', 'away_team_id', name='uix_matchup'),
        Index('idx_matchup_team_week', 'home_team_id', 'away_team_id', 'week'),
        Index('idx_matchup_week', 'week')
    )

class Activity(Base):
    __tablename__ = 'activities'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(BigInteger)
    team_id = Column(Integer, ForeignKey('teams.id'))
    player_id = Column(Integer, ForeignKey('players.id'), nullable=False)
    bidAmount = Column(Float)
    action = Column(String(50))

    # Relationships
    team = relationship("Team", back_populates="activities")
    player = relationship("Player", back_populates="activities")

    __table_args__ = (
        Index('idx_activity_team_player', 'team_id', 'player_id'),
        Index('idx_activity_team', 'team_id')
    )

class Roster(Base):
    __tablename__ = 'rosters'
    id = Column(Integer, primary_key=True, autoincrement=True)
    team_id = Column(Integer, ForeignKey('teams.id'), nullable=False)
    player_id = Column(Integer, ForeignKey('players.id'), nullable=False)
    rosterSlot = Column(String(50))

    # Relationships
    team = relationship("Team", back_populates="rosters")
    player = relationship("Player", back_populates="rosters")

    __table_args__ = (
        UniqueConstraint('team_id', 'player_id', name='uix_roster_team_player'),
        Index('idx_roster_team_player', 'team_id', 'player_id'),
        Index('idx_roster_team', 'team_id')
    )