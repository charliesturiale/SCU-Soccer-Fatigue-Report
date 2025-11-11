# Match Reports Backend

Python backend for the Match Report Toolkit application.

## Setup

1. **Create virtual environment** (if not already done):
   ```bash
   cd server
   python -m venv .venv
   ```

2. **Activate virtual environment**:
   - Windows: `.venv\Scripts\activate`
   - Linux/Mac: `source .venv/bin/activate`

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Initialize database**:
   ```bash
   python init_db.py
   ```

## Database Management

### SQLAlchemy Setup
- **Database**: SQLite at `../data/project.db`
- **ORM**: SQLAlchemy 2.0
- **Models**: Defined in `models.py`
  - `ValdTest`: Stores VALD performance testing data
  - `CatapultSession`: Stores Catapult GPS/tracking data

### Migrations with Alembic

**Create a new migration** (after modifying models.py):
```bash
cd server
alembic revision --autogenerate -m "Description of changes"
```

**Apply migrations**:
```bash
alembic upgrade head
```

**View migration history**:
```bash
alembic history
```

**Rollback last migration**:
```bash
alembic downgrade -1
```

## Scripts

### `generate.py`
Main report generation script with two commands:

**Build player profiles**:
```bash
python generate.py build-profiles --window-days 42
```

**Generate match report**:
```bash
python generate.py generate --match-date 2025-10-24
```

### `init_db.py`
Initialize database schema (creates all tables):
```bash
python init_db.py
```

### ETL Scripts

**Ingest VALD data**:
```bash
python etl/ingest_vald.py MSOC
```

**Ingest Catapult data**:
```bash
python etl/ingest_catapult.py MSOC
```

## Configuration

The application uses environment variables and JSON config files:

- `DATABASE_URL`: Database connection string (default: `sqlite:///../data/project.db`)
- `CONFIG_JSON`: Path to configuration JSON file
- `SECRETS_JSON`: Path to secrets JSON file

These are automatically exported from the frontend when you click "Prep data pipeline".

## Development Workflow

1. Modify database models in `models.py`
2. Create migration: `alembic revision --autogenerate -m "Add new field"`
3. Review the generated migration in `alembic/versions/`
4. Apply migration: `alembic upgrade head`
5. Test your changes

## Project Structure

```
server/
├── alembic/              # Database migrations
│   ├── versions/         # Migration files
│   ├── env.py           # Alembic environment config
│   └── script.py.mako   # Migration template
├── etl/                 # ETL scripts
│   ├── ingest_vald.py
│   └── ingest_catapult.py
├── config.py            # Configuration loading
├── db.py                # Database connection
├── models.py            # SQLAlchemy models
├── generate.py          # Main report generation
├── init_db.py           # Database initialization
├── alembic.ini          # Alembic configuration
└── requirements.txt     # Python dependencies
```

## Frontend Integration

The Tauri frontend calls Python scripts via the shell plugin:
- "Prep data pipeline" → runs ETL scripts
- "Build Player Profiles" → runs `generate.py build-profiles`
- "Generate Report PDF" → runs `generate.py generate`

All output is streamed to the frontend console in real-time.
