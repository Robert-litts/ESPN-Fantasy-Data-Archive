## Update Environment Variables


## Alembic Migrations
```python
alembic init alembic
alembic revision --autogenerate -m "Initial migration"
alembic upgrade head
```

Populate the database in the following order:

- FFleague
- Player
- Team
- Settings
- Draft
- Roster
- Matchup
- Activity