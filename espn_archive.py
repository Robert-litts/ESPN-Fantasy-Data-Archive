#!/usr/bin/env python3
"""
Export ESPN Fantasy Football Data to Postgres
Handles database migrations and data fetching with proper error handling and logging.
"""

import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import List, Optional

from alembic import command
from alembic.config import Config
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from app.services.asyncLeagueData import fetch_league_data
from app.services.espn_service import (
    fetch_and_populate_leagues_from_leagues,
    fetch_and_populate_players_from_leagues,
    fetch_and_populate_settings_from_leagues,
    fetch_and_populate_teams_from_leagues,
    fetch_and_populate_draft_from_leagues,
    fetch_and_populate_matchups_from_leagues,
    fetch_and_populate_roster_from_leagues,
)
from app.services.cache import fetch_league_with_cache, get_cache_status

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('espn_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ESPNDataPipeline:
    """Modern ESPN Fantasy Football data pipeline with migration support."""
    
    def __init__(self, use_cache: bool = True, force_refresh: bool = False):
        """
        Initialize the pipeline with environment variables.
        
        Args:
            use_cache: Whether to use cached data (default: True)
            force_refresh: Whether to force refresh of cached data (default: False)
        """
        load_dotenv()
        
        # Environment variables
        self.start_year = int(os.getenv("START_YEAR", "2020"))
        self.end_year = int(os.getenv("END_YEAR", "2024"))
        self.league_id = os.getenv("LEAGUE_ID")
        self.espn_s2 = os.getenv("ESPN_S2")
        self.swid = os.getenv("SWID")
        self.database_url = os.getenv("DATABASE_URL")
        
        # Cache configuration
        self.use_cache = use_cache
        self.force_refresh = force_refresh
        self.cache_max_age_days = int(os.getenv("CACHE_MAX_AGE_DAYS", "365"))
        
        # Validate required environment variables
        self._validate_environment()
        
        # Initialize database engine
        self.engine = create_engine(self.database_url)
        
        # Years to process
        self.years = range(self.start_year, self.end_year + 1)
        
        cache_status = "enabled" if use_cache else "disabled"
        if use_cache and force_refresh:
            cache_status += " (force refresh)"
            
        logger.info(f"Initialized pipeline for years {self.start_year}-{self.end_year}, cache {cache_status}, max age {self.cache_max_age_days} days")

    def _validate_environment(self) -> None:
        """Validate that all required environment variables are set."""
        required_vars = ["LEAGUE_ID", "ESPN_S2", "SWID", "DATABASE_URL"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    def check_database_connection(self) -> bool:
        """Check if database connection is working."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                logger.info("Database connection successful")
                return True
        except SQLAlchemyError as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def run_migrations(self, alembic_cfg_path: str = "alembic.ini") -> bool:
        """
        Run Alembic migrations to upgrade database to latest version.
        
        Args:
            alembic_cfg_path: Path to alembic.ini configuration file
            
        Returns:
            bool: True if migrations succeeded, False otherwise
        """
        try:
            # Check if alembic.ini exists
            if not Path(alembic_cfg_path).exists():
                logger.error(f"Alembic configuration file not found: {alembic_cfg_path}")
                return False
            
            logger.info("Starting database migrations...")
            
            # Create Alembic configuration
            alembic_cfg = Config(alembic_cfg_path)
            
            # Set database URL in environment (Alembic env.py will read this)
            os.environ["DATABASE_URL"] = self.database_url
            
            # Also set it in the alembic config as fallback
            alembic_cfg.set_main_option("sqlalchemy.url", self.database_url)
            
            # Run migrations
            command.upgrade(alembic_cfg, "head")
            logger.info("Database migrations completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            return False
    
    def fetch_league_data(self) -> Optional[List]:
        """Fetch league data from ESPN API with caching support."""
        try:
            # Show cache status before fetching
            cache_status = get_cache_status(list(self.years), self.league_id)
            cached_years = [year for year, cached in cache_status.items() if cached]
            
            if cached_years and self.use_cache and not self.force_refresh:
                logger.info(f"Cache available for years: {cached_years}")
            elif not self.use_cache:
                logger.info("Cache disabled - will fetch fresh data from API")
            elif self.force_refresh:
                logger.info("Force refresh enabled - will clear cache and fetch fresh data")
            else:
                logger.info("No cached data found - will fetch from API")
            
            # Use the updated cache function with control parameters
            leagues = fetch_league_with_cache(
                self.years,
                self.league_id,
                self.espn_s2,
                self.swid,
                max_age_days=self.cache_max_age_days,  # You can make this configurable if needed
                use_cache=self.use_cache,
                force_refresh=self.force_refresh
            )
            
            logger.info(f"Successfully fetched data for {len(leagues)} leagues")
            return leagues
            
        except Exception as e:
            logger.error(f"Failed to fetch league data: {e}")
            return None
    
    def populate_database(self, leagues: List) -> bool:
        """
        Populate database with league data.
        
        Args:
            leagues: List of league objects from ESPN API
            
        Returns:
            bool: True if all operations succeeded, False otherwise
        """
        operations = [
            ("leagues", fetch_and_populate_leagues_from_leagues),
            ("players", fetch_and_populate_players_from_leagues),
            ("settings", fetch_and_populate_settings_from_leagues),
            ("teams", fetch_and_populate_teams_from_leagues),
            ("draft", fetch_and_populate_draft_from_leagues),
            ("matchups", fetch_and_populate_matchups_from_leagues),
            ("rosters", fetch_and_populate_roster_from_leagues),
        ]
        
        success = True
        
        for operation_name, operation_func in operations:
            try:
                logger.info(f"Populating {operation_name}...")
                operation_func(leagues)
                logger.info(f"Successfully populated {operation_name}")
                
            except Exception as e:
                logger.error(f"Failed to populate {operation_name}: {e}")
                success = False
        
        return success
    
    def run_full_pipeline(self, skip_migrations: bool = False) -> bool:
        """
        Run the complete data pipeline.
        
        Args:
            skip_migrations: If True, skip database migrations
            
        Returns:
            bool: True if pipeline completed successfully
        """
        cache_info = f"cache {'enabled' if self.use_cache else 'disabled'}"
        if self.use_cache and self.force_refresh:
            cache_info += " (force refresh)"
            
        logger.info(f"Starting ESPN data pipeline with {cache_info}")
        
        # Step 1: Check database connection
        if not self.check_database_connection():
            logger.error("Cannot proceed without database connection")
            return False
        
        # Step 2: Run migrations (unless skipped)
        if not skip_migrations:
            if not self.run_migrations():
                logger.error("Migration failed, aborting pipeline")
                return False
        else:
            logger.info("Skipping migrations as requested")
        
        # Step 3: Fetch league data
        leagues = self.fetch_league_data()
        if leagues is None:
            logger.error("Failed to fetch league data, aborting pipeline")
            return False
        
        # Step 4: Populate database
        if not self.populate_database(leagues):
            logger.error("Database population had errors")
            return False
        
        logger.info("ESPN data pipeline completed successfully")
        return True


def main():
    """Main entry point for the pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(description="ESPN Fantasy Football Data Pipeline")
    parser.add_argument(
        "--skip-migrations", 
        action="store_true",
        help="Skip database migrations"
    )
    parser.add_argument(
        "--migrations-only",
        action="store_true", 
        help="Only run migrations, skip data fetching"
    )
    parser.add_argument(
        "--alembic-config",
        default="alembic.ini",
        help="Path to alembic configuration file"
    )
    
    # Cache control arguments
    cache_group = parser.add_mutually_exclusive_group()
    cache_group.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable cache and fetch fresh data from ESPN API"
    )
    cache_group.add_argument(
        "--force-refresh",
        action="store_true",
        help="Force refresh cached data (fetch from API and update cache)"
    )
    
    args = parser.parse_args()
    
    # Determine cache settings
    use_cache = not args.no_cache
    force_refresh = args.force_refresh
    
    try:
        pipeline = ESPNDataPipeline(use_cache=use_cache, force_refresh=force_refresh)
        
        if args.migrations_only:
            logger.info("Running migrations only")
            success = pipeline.run_migrations(args.alembic_config)
        else:
            success = pipeline.run_full_pipeline(skip_migrations=args.skip_migrations)
        
        if success:
            logger.info("Pipeline completed successfully")
            sys.exit(0)
        else:
            logger.error("Pipeline failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()