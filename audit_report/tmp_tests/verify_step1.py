import sys
import os
import asyncio
import time
from unittest.mock import MagicMock
from fastapi import HTTPException

sys.path.insert(0, os.path.abspath("backend"))

from config import AppConfig
from llm.api_manager import APIKeyManager
import api.dependencies as deps

def test_api_key_manager_status():
    print("--- 1. Testing APIKeyManager.get_status() ---")
    km1 = APIKeyManager(["test-key-1", "test-key-2"])
    st1 = km1.get_status()
    assert isinstance(st1["all_exhausted"], bool), f"all_exhausted is not bool: {type(st1['all_exhausted'])}"
    assert st1["all_exhausted"] is False, "Expected all_exhausted=False for non-empty pool"
    assert st1["has_available_keys"] is True, "Expected has_available_keys=True"

    # Empty pool
    km_empty = APIKeyManager([])
    st_empty = km_empty.get_status()
    assert isinstance(st_empty["all_exhausted"], bool), f"all_exhausted is not bool: {type(st_empty['all_exhausted'])}"
    assert st_empty["all_exhausted"] is True, "Expected all_exhausted=True for empty pool"
    assert st_empty["has_available_keys"] is False, "Expected has_available_keys=False for empty pool"

    # Exhaust keys
    asyncio.run(km1.mark_exhausted("test-key-1"))
    asyncio.run(km1.mark_exhausted("test-key-2"))
    st_exhausted = km1.get_status()
    assert st_exhausted["all_exhausted"] is True, "Expected all_exhausted=True when all marked exhausted"
    assert st_exhausted["has_available_keys"] is False, "Expected has_available_keys=False when exhausted"
    print("PASS: APIKeyManager.get_status() returns valid bools without unawaited coroutines.")

def test_per_ip_lockout():
    print("\n--- 2. Testing Per-IP Lockout in verify_api_key ---")
    deps._failed_attempts.clear()
    
    attacker_ip = "192.168.1.100"
    victim_ip = "192.168.1.200"

    # Mock requests
    def make_req(ip):
        req = MagicMock()
        req.headers = {}
        req.client.host = ip
        return req

    attacker_req = make_req(attacker_ip)
    victim_req = make_req(victim_ip)

    # Attacker attempts 9 failed logins -> should get 401
    for i in range(1, 10):
        try:
            asyncio.run(deps.verify_api_key(request=attacker_req, x_api_key="bad-key"))
            assert False, f"Attempt {i} should have raised 401"
        except HTTPException as e:
            assert e.status_code == 401, f"Attempt {i} got {e.status_code}, expected 401"

    # Attacker attempt 10 -> should trigger 429 Too Many Requests
    try:
        asyncio.run(deps.verify_api_key(request=attacker_req, x_api_key="bad-key"))
        assert False, "Attempt 10 should have raised 429"
    except HTTPException as e:
        assert e.status_code == 429, f"Attempt 10 got {e.status_code}, expected 429"
        print(f"PASS: Attacker IP ({attacker_ip}) received HTTP 429 after 10 failed attempts.")

    # Even with correct key, attacker IP is locked out
    try:
        asyncio.run(deps.verify_api_key(request=attacker_req, x_api_key=AppConfig.API_KEY))
        assert False, "Attacker should still be locked out even with valid key"
    except HTTPException as e:
        assert e.status_code == 429
        print(f"PASS: Attacker IP ({attacker_ip}) remains locked out on valid key.")

    # Legitimate user (victim_ip) with valid key MUST succeed with no lockout
    result = asyncio.run(deps.verify_api_key(request=victim_req, x_api_key=AppConfig.API_KEY))
    assert result == AppConfig.API_KEY, f"Legitimate user auth failed: {result}"
    print(f"PASS: Legitimate IP ({victim_ip}) authenticates successfully despite attacker lockout (SEC-02 verified fixed!).")

def test_secrets_scrubbed():
    print("\n--- 3. Testing Secrets Scrubbed from backend/.env ---")
    with open("backend/.env", "r", encoding="utf-8") as f:
        content = f.read()
    assert "AIzaSyD9Wki2OsZ6QKhINjK4VESE5vOCck4pCqU" not in content, "Found leaked primary key in backend/.env!"
    assert "AIzaSyCcgkA_iiFJo5BuVZTHfncmZiLzZinu-BA" not in content, "Found leaked secondary key in backend/.env!"
    print("PASS: Leaked Gemini API keys successfully removed from backend/.env.")

if __name__ == "__main__":
    test_api_key_manager_status()
    test_per_ip_lockout()
    test_secrets_scrubbed()
    print("\nALL STEP 1 VERIFICATION CHECKS PASSED SUCCESSFULLY!")
