# API Overview

The **ProcG Blueprint** API is a RESTful API that uses JSON for data exchange.

## Base URL

All API endpoints are relative to the base URL of the server.
Development: `http://localhost:5000`

## Authentication

Most endpoints require authentication using **JSON Web Tokens (JWT)**.

To access protected endpoints, you must include the `Authorization` header in your request:

```http
Authorization: Bearer <your_access_token>
```

### Obtaining Tokens
-   **Login**: POST `/users/login` to receive an access token and a refresh token.
-   **Refresh**: POST `/users/refresh` to obtain a new access token using a valid refresh token.

## Response Format

### Success
Successful requests typically return a `200 OK` or `201 Created` status code with a JSON body containing the requested data.

```json
{
  "msg": "Operation successful",
  "data": { ... }
}
```

### Errors
Error responses return a `4xx` or `5xx` status code with a JSON body describing the error.

```json
{
  "msg": "Error description",
  "error": "Detailed error message (optional)"
}
```

## Common Status Codes

-   `200 OK`: Request succeeded.
-   `201 Created`: Resource created successfully.
-   `400 Bad Request`: Invalid input or request format.
-   `401 Unauthorized`: Missing or invalid authentication token.
-   `403 Forbidden`: Authenticated user does not have permission.
-   `404 Not Found`: Resource not found.
-   `500 Internal Server Error`: Server encountered an error.
