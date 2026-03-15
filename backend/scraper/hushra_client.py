import requests
import json
import logging
import random
import time
from typing import Optional, Dict, List
import string
from curl_cffi import requests as curl_requests

logger = logging.getLogger(__name__)

# Varied Chrome/Firefox user agents — rotated per session
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.2365.92",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]

# Varied Sec-Ch-Ua headers matching user agents
SEC_CH_UA_VARIANTS = [
    '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    '"Microsoft Edge";v="122", "Chromium";v="122", "Not(A:Brand";v="24"',
    '"Google Chrome";v="121", "Not:A-Brand";v="8", "Chromium";v="121"',
]

ACCEPT_LANGUAGES = [
    "en-US,en;q=0.9",
    "en-US,en;q=0.8",
    "en-GB,en;q=0.9,en-US;q=0.8",
    "en-US,en;q=0.9,es;q=0.8",
]


class HushraAPIClient:
    """Client for interacting with the Hushra API (Login and SSN Lookup).
    Supports proxy injection and randomized browser fingerprinting headers.
    """

    BASE_URL = "https://api.hushra.me/api/v1"

    def __init__(self, proxy: str = None):
        self.session = requests.Session()
        self._ua = random.choice(USER_AGENTS)
        self._sec_ch_ua = random.choice(SEC_CH_UA_VARIANTS)
        self._accept_lang = random.choice(ACCEPT_LANGUAGES)
        self._proxy = proxy

        if proxy:
            self.session.proxies.update({
                "http": proxy,
                "https": proxy,
            })

        self.session.headers.update(self._build_headers())
        self.token = None

    def _build_headers(self):
        return {
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://hushra.me",
            "Referer": "https://hushra.me/",
            "User-Agent": self._ua,
            "Accept-Language": self._accept_lang,
            "Accept-Encoding": "gzip, deflate",  # Removed 'br' as it was causing binary decode issues
            "Sec-Ch-Ua": self._sec_ch_ua,
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "Connection": "keep-alive",
        }

    def set_token(self, token: str):
        """Inject a pre-cached token directly (bypass login)."""
        self.token = token
        self.session.headers.update({"Authorization": f"Bearer {token}"})

    def login(self, uuid: str) -> str:
        """
        Authenticate with the given UUID and store the Bearer token in the session.
        Returns a result code: 'SUCCESS', 'AUTH_FAILED', 'PARSE_ERROR', 'RATE_LIMITED', 'NETWORK_ERROR'
        """
        payload = {"uuid": uuid, "token": None}
        url = f"{self.BASE_URL}/auth/login"

        try:
            response = self.session.post(url, json=payload, timeout=10)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if "token" in data:
                        self.token = data["token"]
                        self.session.headers.update({"Authorization": f"Bearer {self.token}"})
                        return "SUCCESS"
                    else:
                        logger.error(f"Login response 200 but no token found: {data}")
                        return "PARSE_ERROR"
                except json.JSONDecodeError:
                    # Log the start of the response to see if it's HTML or binary
                    raw = response.text[:500]
                    logger.error(f"Failed to parse login JSON. Raw response start: {raw}")
                    return "PARSE_ERROR"
            
            elif response.status_code == 429:
                return "RATE_LIMITED"
            elif response.status_code in [401, 403]:
                return "AUTH_FAILED"
            else:
                logger.error(f"Failed to login with UUID {uuid[:8]}... Status: {response.status_code}. Response: {response.text[:200]}")
                return "NETWORK_ERROR"

        except requests.exceptions.RequestException as e:
            logger.error(f"Network error during Hushra login: {str(e)}")
            return "NETWORK_ERROR"
        except Exception as e:
            logger.error(f"Unexpected exception during Hushra login: {str(e)}")
            return "NETWORK_ERROR"

    def lookup(self, firstname="", lastname="", state="", city="", dob="",
               address="", zip_code="", phone="", ssn=""):
        """
        Perform the SSN lookup against the datahub endpoint.
        Requires login() or set_token() to be called first.
        Accepts all API filterable fields for multi-dimensional sweeps.
        Returns the data array on success, or raises an exception.
        """
        if not self.token:
            raise ValueError("Must login before performing a lookup.")

        payload = {
            "firstname": firstname,
            "lastname": lastname,
            "dob": dob,
            "address": address,
            "city": city,
            "st": state,
            "zip": zip_code,
            "phone": phone,
            "ssn": ssn,
        }
        url = f"{self.BASE_URL}/datahub"

        # Micro-jitter within the session to simulate natural pacing
        time.sleep(random.uniform(0.3, 1.2))

        response = self.session.post(url, json=payload, timeout=15)

        if response.status_code == 429:
            raise requests.exceptions.HTTPError("429 Too Many Requests", response=response)

        response.raise_for_status()

        data = response.json()
        if data.get("success"):
            return data.get("data", [])

        logger.warning(f"Hushra lookup unsuccessful for '{firstname} {lastname}' city='{city}' state='{state}'. Response: {data}")
        return []

    def register_account(self, username: str, proxy: Optional[str] = None) -> Optional[dict]:
        """
        Register a new account on hushra.me to obtain a fresh UUID.
        Uses curl_cffi to impersonate a browser and bypass stealth protections.
        Returns the response JSON (containing 'uuid') or None.
        """
        url = "https://api.hushra.me/api/v1/auth/registration"
        payload = {"username": username}
        
        # Build headers similar to generator.py
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": self._accept_lang,
            "Content-Type": "application/json",
            "Origin": "https://hushra.me",
            "Referer": "https://hushra.me/login",
            "User-Agent": self._ua,
            "Sec-Ch-Ua": self._sec_ch_ua,
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
        }

        proxies = None
        if proxy:
            proxies = {"http": proxy, "https": proxy}
        elif self._proxy:
            proxies = {"http": self._proxy, "https": self._proxy}

        try:
            # impersonate="chrome110" helps bypass Cloudflare/TLS fingerprinting
            response = curl_requests.post(
                url, 
                json=payload, 
                headers=headers, 
                impersonate="chrome110",
                proxies=proxies,
                timeout=20
            )
            
            if response.status_code in [200, 201]:
                return response.json()
            else:
                logger.error(f"Registration failed for {username}: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error during account registration for {username}: {str(e)}")
            return None
