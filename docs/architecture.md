# Architecture Overview

The **ProcG Blueprint** is a modular web application built with **Flask**, designed for scalability and maintainability. It leverages a micro-service-like architecture within a monolith using Flask Blueprints.

## Core Technologies

-   **Flask**: The core web framework.
-   **PostgreSQL**: Relational database for persistent storage.
-   **Redis**: In-memory data structure store, used as a message broker for Celery and for caching.
-   **Celery**: Distributed task queue for handling asynchronous background jobs.
-   **Celery Redbeat**: A Celery Beat Scheduler that stores the schedule in Redis, allowing for dynamic task scheduling.
-   **Flask-JWT-Extended**: Handles JSON Web Token (JWT) authentication.

## Project Structure

The project follows a modular structure where each feature is encapsulated in its own directory within `api/`.

```
x:\procg_blueprint\
├── api/                # Contains all API modules (Blueprints)
│   ├── users/          # User management
│   ├── rbac/           # Role-Based Access Control
│   ├── aggregation/    # Data aggregation
│   └── ...
├── executors/          # Celery task definitions and extensions
├── utils/              # Helper functions
├── app.py              # Application factory and configuration
├── config.py           # Configuration settings
└── main.py             # Entry point
```

## Key Components

### 1. Blueprints
Each module in `api/` (e.g., `users`, `rbac`) is a Flask Blueprint. This allows for separation of concerns, making the codebase easier to navigate and maintain. Each blueprint registers its own routes and views.

### 2. Asynchronous Tasks
Long-running or scheduled tasks are offloaded to **Celery**.
-   **Workers**: Execute the tasks.
-   **Beat**: Schedules periodic tasks.
-   **Redbeat**: Allows dynamic scheduling (e.g., users scheduling a report generation) by storing schedules in Redis.

### 3. Authentication
Security is handled via **JWT**.
-   **Access Tokens**: Short-lived tokens for API access.
-   **Refresh Tokens**: Long-lived tokens used to obtain new access tokens.
-   **RBAC**: Role-Based Access Control ensures users can only access resources they are authorized for.
