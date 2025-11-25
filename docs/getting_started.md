# Getting Started

This guide will help you set up the **PROCG Blueprint** project on your local machine for development and testing.

## Prerequisites

Before you begin, ensure you have the following installed:

- **Python 3.8+**
- **PostgreSQL** (for the database)
- **Redis** (for message brokering and caching)
- **Git**

## Installation

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/HasanatSiam/procg_blueprint.git
    cd procg_blueprint
    ```

2.  **Create a virtual environment:**

    ```bash
    python -m venv procg_venv
    source procg_venv/bin/activate  # On Windows: procg_venv\Scripts\activate
    ```

3.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

## Configuration

1.  **Environment Variables:**

    Create a `.env` file in the root directory. You can use the following template:

    ```ini
    # Database
    DATABASE_URL=postgresql://user:password@localhost:5432/procg_db

    # Redis (Celery & Caching)
    MESSAGE_BROKER=redis://localhost:6379/0

    # Security
    JWT_SECRET_ACCESS_TOKEN=your-super-secret-jwt-key
    CRYPTO_SECRET_KEY=your-encryption-key

    # Token Expiry
    ACCESS_TOKEN_EXPIRED_TIME=15m
    REFRESH_TOKEN_EXPIRED_TIME=30d
    INV_EXPIRE_TIME=7  # Days

    # Email (Flask-Mail)
    MAIL_SERVER=smtp.example.com
    MAIL_PORT=587
    MAIL_USE_TLS=True
    EMAIL_USER=your-email@example.com
    EMAIL_PASS=your-email-password

    # Monitoring
    FLOWER_URL=http://localhost:5555
    ```

2.  **Database Setup:**

    Ensure your PostgreSQL server is running and the database specified in `DATABASE_URL` exists.

## Running the Application

1.  **Start the Flask Server:**

    ```bash
    python main.py
    # OR
    flask run
    ```

    The server will start at `http://localhost:5000`.

2.  **Start the Celery Worker:**

    Open a new terminal, activate the virtual environment, and run:

    ```bash
    celery -A executors.celery worker --loglevel=info
    ```

3.  **Start the Celery Beat (Scheduler):**

    Open a new terminal, activate the virtual environment, and run:

    ```bash
    celery -A executors.celery beat --loglevel=info
    ```
