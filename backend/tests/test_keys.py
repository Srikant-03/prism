"""
Tests for API key pool initialization and behavior.
"""
import pytest
from llm.api_manager import APIKeyManager

def test_empty_api_key_pool():
    km = APIKeyManager([])
    assert km.are_all_keys_exhausted() is True
    st = km.get_status()
    assert st["total_keys"] == 0
    assert st["all_exhausted"] is True
    assert st["has_available_keys"] is False

def test_api_key_pool_with_keys():
    km = APIKeyManager(["key-1", "key-2"])
    assert km.are_all_keys_exhausted() is False
    st = km.get_status()
    assert st["total_keys"] == 2
    assert st["all_exhausted"] is False
    assert st["has_available_keys"] is True
