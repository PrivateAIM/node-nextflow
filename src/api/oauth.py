import os
from typing import Annotated

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import OAuth2AuthorizationCodeBearer

REALM_URL = f"{os.getenv('KEYCLOAK_URL', '')}/realms/flame/protocol/openid-connect"

oauth2_scheme = OAuth2AuthorizationCodeBearer(tokenUrl=f"{REALM_URL}/token",
                                              authorizationUrl=f"{REALM_URL}/auth",
                                              refreshUrl=f"{REALM_URL}/token")


async def valid_access_token(token: Annotated[str, Depends(oauth2_scheme)]) -> dict:
    try:
        sig_key = jwt.PyJWKClient(f"{REALM_URL}/certs").get_signing_key_from_jwt(token)
        return jwt.decode(token, key=sig_key, options={"verify_signature": True, "verify_aud": False, "exp": True})
    except jwt.exceptions.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Not authenticated")
