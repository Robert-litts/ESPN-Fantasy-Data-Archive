# ESPN Fantasy Data Archive

 🏈 A Python application that extracts ESPN Fantasy Football league data and preserves it locally in case ESPN "misplaces" it in the future! Data is archived in both a local [Shelf](https://docs.python.org/3/library/shelve.html) file as well as in a PostgreSQL database. Archived data includes drafts, scores, rosters, transactions, and league settings!


## Features

- Extract ESPN Fantasy league data using [espn-api](https://github.com/cwendt94/espn-api)
- Smart caching system using local [shelf storage](https://docs.python.org/3/library/shelve.html) with customizable refresh intervals
- PostgreSQL database storage with Alembic migrations for schema management
- Configurable data extraction for multiple years and leagues

## Prerequisites

- Docker and Docker Compose
- PostgreSQL database (local or hosted)
- ESPN Fantasy Football account with LeagueID, SWID, and S2. [See here for how to access those cookies](https://github.com/cwendt94/espn-api/discussions/150)

## Quick Start

### Docker with External Database (Recommended)

1. **Clone and configure:**
   ```bash
   curl -O https://github.com/Robert-litts/ESPN-Fantasy-Data-Archive/docker-compose.yml
   curl -o .env https://github.com/Robert-litts/ESPN-Fantasy-Data-Archive/.env.example
   ```
2. **Update the `.env` file with your settings:**
   ```sh
    #ESPN Credentials
    LEAGUE_ID = '123456'
    ESPN_S2 = 'YOUR_S2_Here'
    SWID = '{LONG_SWID_STRING}'
    DATABASE_URL = "postgresql://user:pass@X.X.X.X:5432/postgres?sslmode=disable"

    #Years to archive data
    START_YEAR=2013
    END_YEAR=2021

    #Optional: Set a max cache age for Shelf, defaults to 365 days if not provided
    CACHE_MAX_AGE_DAYS=20
   ```
3. **Run:**
   ```bash
   docker-compose run --rm espn-archive
   ```

4. **Done! Your data is now stored in your DB and in your local shelf file!** Backup your PostgresDB and Shelf file to a safe place!

### Advanced Docker Commands

```bash
# Run normal extract (recommended for regular use)
docker-compose run --rm espn-archive

# Run with cache control
docker-compose run --rm espn-archive python espn_archive.py --force-refresh  # Ignore cache
docker-compose run --rm espn-archive python espn_archive.py --no-cache       # Disable cache

# Run only database migrations (useful for setup)
docker-compose run --rm espn-archive python espn_archive.py --migrations-only

# Skip migrations (faster for regular runs)
docker-compose run --rm espn-archive python espn_archive.py --skip-migrations

# Debug mode
docker-compose run --rm espn-archive bash
```

### Getting ESPN Credentials

To access private ESPN leagues, you'll need your ESPN cookies, [see here](https://github.com/cwendt94/espn-api/discussions/150):

1. **Log into ESPN Fantasy Sports** in your browser
2. **Open Developer Tools** (F12)
3. **Go to Application/Storage > Cookies**
4. **Find and copy these values:**
   - `espn_s2` - Long authentication string
   - `SWID` - Your unique ESPN identifier (includes curly braces)

### Database Options

#### Option 1: Hosted Database (Recommended)
The free-tier of a hosted PostgreSQL service works great for this task. No need to worry about backups! I've succesfully tested with:
- **Xatalite** - [Free tier](https://lite.xata.io/)
- **AWS RDS** - [AWS RDS](https://aws.amazon.com/free/database/)

#### Option 2: Local PostgreSQL
If you'd prefer to keep it local, this is a great option. Here's a Postgres 17 Docker Container that will get you started as a archive target.

```bash
 db:
    image: postgres:17
    container_name: postgres-db
    restart: always
    env_file:
      - ".env"
    volumes:
      - ./postgres/data:/var/lib/postgresql/data
      - ./postgres/init/:/docker-entrypoint-initdb.d/
    ports:
      - "5432:5432"
```

### Data Backup

Export your data regularly using pg_dump:

```bash
# Create a backup
pg_dump -h your_host -U your_user -d espn_fantasy_data > backup_$(date +%Y%m%d).sql

# Restore from backup
psql -h your_host -U your_user -d espn_fantasy_data < backup_20240101.sql
```

## Database Schema

```mermaid
erDiagram
    FFleague ||--o{ Settings : "has"
    FFleague ||--o{ Team : "has"
    Team ||--o{ Roster : "has"
    Team ||--o{ Matchup : "home/away"
    Team ||--o{ Activity : "makes"
    Team ||--o{ Draft : "makes pick"
    Team ||--o{ Draft : "makes nomination"
    Player ||--o{ Roster : "in"
    Player ||--o{ Activity : "involved"
    Player ||--o{ Draft : "drafted"
    Matchup }o--|| Team : "home_team"
    Matchup }o--|| Team : "away_team"

    FFleague {
        int id PK
        int leagueId
        int year
        int teamCount
        int currentWeek
        int nflWeek
    }

    Settings {
        int id PK
        int league_id FK
        int regularSeasonCount
        int vetoVotesRequired
        int teamCount
        int playoffTeamCount
        int keeperCount
        bigint tradeDeadline
        string name
        string tieRule
        string playoffTieRule
        string playoffSeedTieRule
        int playoffMatchupPeriodLength
        bool faab
    }

    Team {
        int id PK
        int league_id FK
        int teamId
        int year
        string teamAbbrv
        string teamName
        string owners
        string divisionId
        string divisionName
        int wins
        int losses
        int ties
        int pointsFor
        int pointsAgainst
        int waiverRank
        int acquisitions
        int acquisitionBudgetSpent
        int drops
        int trades
        string streakType
        int streakLength
        int standing
        int finalStanding
        int draftProjRank
        int playoffPct
        string logoUrl
    }

    Player {
        int id PK
        int espnId
        string name
        string position
    }

    Draft {
        int id PK
        int team_id FK
        int player_id FK
        int overallPick
        int roundNum
        int roundPick
        bool keeperStatus
        int bidAmount
        int nominating_team_id FK
    }

    Matchup {
        int id PK
        int week
        int home_team_id FK
        int away_team_id FK
        float homeScore
        float awayScore
        bool isPlayoff
        string matchupType
    }

    Activity {
        int id PK
        bigint date
        int team_id FK
        int player_id FK
        float bidAmount
        string action
    }

    Roster {
        int id PK
        int team_id FK
        int player_id FK
        string rosterSlot
    }
```

## Caching Strategy

The application uses Python's `shelf` module for intelligent caching:

- **Customizable refresh intervals** prevent excessive API calls
- **Selective updates** only fetch changed data
- **Persistent storage** survives application restarts
- **Override capabilities** for manual data refresh

## Troubleshooting

### Common Issues

**Authentication Errors:**
- Verify your ESPN cookies are current and correctly formatted
- Ensure you have access to the specified league
- 😞 Your data may have already been deleted 

**Database Connection:**
- Check your database credentials and network access
- Verify the database exists and migrations are current

**Missing Data:**
- Some historical data may not be available through ESPN's API
- Private league data requires proper authentication

### Core Libraries
- `espn-api` - ESPN Fantasy Sports API wrapper by cdwendt
- `sqlalchemy` - Database ORM and toolkit
- `alembic` - Database migration management
- `psycopg2` - PostgreSQL adapter
- `python-dotenv` - Environment variable management

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Disclaimer

This tool is for personal use and data preservation. Ensure you comply with ESPN's Terms of Service and only access leagues you have permission to view. I am not responsible for any misuse of this software.

## Acknowledgments

- This wouldn't be possible without [cdwendt's](https://github.com/cwendt94) excellent [espn-api](https://github.com/cwendt94/espn-api) Python library
---