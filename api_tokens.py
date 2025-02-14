from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from firestore import auth


security = HTTPBearer()


# Generate a random API key (32 characters long)
""" 
api_key = secrets.token_hex(32)
api_key """


def verify_token(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> bool:
    """Verify the bearer token against environment variable."""
    # expected_token = os.getenv("API_BEARER_TOKEN")

    if not credentials:
        raise HTTPException(status_code=401, detail="Authorization header missing")

    token = credentials.credentials
    # if not expected_token:
    #     raise HTTPException(
    #         status_code=500,
    #         detail="API_BEARER_TOKEN environment variable not set"
    #     )
    try:
        decoded_token = auth.verify_id_token(token)
        return decoded_token
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid authentication token")
