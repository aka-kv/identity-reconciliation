# Identity Reconciliation System

A serverless backend service for tracking and linking customer identities across multiple purchases. Built with FastAPI and deployed on AWS using Lambda, RDS, and API Gateway.

## 🌐 Live Demo

**Test the API**: [https://bitespeed-task-kv.netlify.app/](https://bitespeed-task-kv.netlify.app/)

Interactive web interface to test the identity reconciliation API with various scenarios including new customers, validation errors, and edge cases.

## AWS Architecture

```
API Gateway → AWS Lambda → RDS Proxy → PostgreSQL RDS
```

### AWS Services Used
- **AWS Lambda**: Serverless compute for the FastAPI application
- **Amazon RDS (PostgreSQL)**: Managed database for contact storage
- **RDS Proxy**: Connection pooling and management for Lambda-RDS communication
- **API Gateway**: HTTP API endpoint for external access
- **Mangum**: ASGI adapter for FastAPI-Lambda integration

## Business Logic

**Identity Linking**: Links customer contacts based on email or phone number matches, maintaining the oldest contact as primary and newer matches as secondary contacts.

## API Endpoint

**POST /identify**

```json
{
  "email": "customer@example.com",
  "phoneNumber": "1234567890"
}
```

**Response:**
```json
{
  "contact": {
    "primaryContactId": 1,
    "emails": ["customer@example.com"],
    "phoneNumbers": ["1234567890"],
    "secondaryContactIds": []
  }
}
```

## Project Structure

```
Bitespeed/
├── identity_reconcilation/    # Main application package
│   ├── models/               # SQLAlchemy database models
│   ├── services/             # Business logic services
│   ├── schemas/              # Pydantic request/response schemas
│   ├── config.py            # Configuration management
│   └── database.py          # Database connection setup
├── lambda_handler.py        # AWS Lambda entry point
├── main.py                  # FastAPI application
├── requirements.txt         # Python dependencies
└── package/                 # Lambda deployment package
```

## Features

- ✅ **Serverless Architecture**: Fully deployed on AWS Lambda
- ✅ **Identity Linking**: Smart contact consolidation logic
- ✅ **Primary/Secondary Logic**: Maintains contact hierarchy
- ✅ **Database Integration**: PostgreSQL with RDS Proxy
- ✅ **Production Ready**: Deployed with API Gateway

## Local Development

1. **Setup Environment**:
   ```bash
   python -m venv venv
   venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Configure Database**:
   ```bash
   export DATABASE_URL="postgresql://user:password@localhost:5432/identity_db"
   ```

3. **Run Locally**:
   ```bash
   python main.py
   ```

## Deployment

The application is deployed using:
- **Lambda Function**: Contains the packaged FastAPI application
- **RDS Instance**: PostgreSQL database with automated backups
- **RDS Proxy**: Manages database connections efficiently
- **API Gateway**: Provides public HTTPS endpoint

## Configuration

Environment variables used in production:
- `DATABASE_URL`: RDS connection string via RDS Proxy
- `ENVIRONMENT`: production
- `AWS_REGION`: AWS deployment region

## API Documentation

Once deployed, access:
- **API Endpoint**: `https://pegxaa572h.execute-api.us-east-1.amazonaws.com/identify`

---

*This project demonstrates a complete serverless identity reconciliation system using modern AWS services and FastAPI.* 