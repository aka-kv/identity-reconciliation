# Identity Reconciliation API

Customer identity linking and reconciliation service for Bitespeed.

## Project Structure

```
identity_reconcilation/
├── models/           # Database models (SQLAlchemy)
├── schemas/          # Request/response schemas (Pydantic)  
├── services/         # Business logic services
├── config.py         # Configuration management
├── database.py       # Database connection setup
├── main.py           # FastAPI application entry point
└── requirements.txt  # Python dependencies
```

## Setup Instructions

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Environment Configuration**
   Create a `.env` file with your database configuration:
   ```
   DEBUG=true
   ENVIRONMENT=development
   DATABASE_URL=postgresql://user:password@localhost:5432/identity_db
   ```

3. **Database Setup**
   Create the database tables:
   ```bash
   python create_tables.py
   ```

4. **Run the Application**
   ```bash
   python main.py
   ```

4. **Access the API**
   - API: http://localhost:8000
   - Health Check: http://localhost:8000/health

## Development

This project follows a task-by-task implementation approach. Current status:

- [x] Task 1: Basic project structure setup
- [x] Task 2: Database models implementation  
- [x] Task 3: Request/response schemas
- [ ] Task 4: Core business logic
- [ ] Task 5: API endpoints
- [ ] Task 6: AWS Lambda deployment

## Requirements

- Python 3.7+
- PostgreSQL database
- FastAPI and dependencies (see requirements.txt) 