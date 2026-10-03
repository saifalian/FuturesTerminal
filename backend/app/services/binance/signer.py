from __future__ import annotations

import hashlib
import hmac



def sign_query(secret: str, query: str) -> str:
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
