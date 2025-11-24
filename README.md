# PROCG Enterprise Backend

The backend repository for the PROCG web application.

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![Flask](https://img.shields.io/badge/Flask-2.0%2B-green)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13%2B-blue)
![Redis](https://img.shields.io/badge/Redis-6%2B-red)
![Celery](https://img.shields.io/badge/Celery-5%2B-green)

## Features

- **Core Backend**: Flask-based API with modular Blueprint architecture.
- **Security**: JWT authentication and Role-Based Access Control (RBAC).
- **Async Processing**: Celery and Redis for background and scheduled tasks.
- **Data Management**: PostgreSQL with SQLAlchemy.

## Tech Stack

- **Framework**: Flask
- **Database**: PostgreSQL, SQLAlchemy
- **Message Broker / Cache**: Redis
- **Task Queue**: Celery
- **Scheduler**: Celery Redbeat
- **Authentication**: Flask-JWT-Extended

## Prerequisites

Ensure you have the following installed:
- Python 3.8+
- Redis Server
- PostgreSQL

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd procg_blueprint
    ```

2.  **Create and activate a virtual environment:**
    ```bash
    python -m venv procg_venv
    # Windows
    procg_venv\Scripts\activate
    # Linux/Mac
    source procg_venv/bin/activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Configuration

Create a `.env` file in the root directory with the following variables:

```env
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/dbname

# Redis / Celery
MESSAGE_BROKER=redis://localhost:6379/0
FLOWER_URL=http://localhost:5555

# Security
JWT_SECRET_ACCESS_TOKEN=your_jwt_secret
CRYPTO_SECRET_KEY=your_crypto_key

# Email
MAIL_SERVER=smtp.example.com
MAIL_PORT=587
EMAIL_USER=your_email@example.com
EMAIL_PASS=your_email_password
```

> **Note:** For the full `.env` file, please contact the PROCG Backend team.
>
> **Environment Loading:**
> - For **local testing**, ensure `load_dotenv()` is used to load variables from the `.env` file.
> - For **production**, use `env_path` or system environment variables directly.
> - **Files to modify:** `config.py` and `executors/__init__.py`.

## Running the Application

### 1. Start the Flask Server
```bash
flask run
# OR
python -m flask run
```
The API will be available at `http://localhost:5000`.

### 2. Start the Celery Worker
To process background tasks:
```bash
celery -A executors.celery_app worker --loglevel=info
```

### 3. Start the Celery Beat (Scheduler)
To run scheduled tasks:
```bash
celery -A executors.celery_app beat --loglevel=info
```

## Project Structure

- `api/`: Contains all Flask Blueprints (routes and logic).
- `executors/`: Application factory, extensions, and task execution logic.
- `redbeat_s/`: Redbeat scheduled task functions.
- `utils/`: Utility functions.
- `config.py`: Application configuration.
- `app.py`: Application entry point.
