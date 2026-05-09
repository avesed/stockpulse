"""Tests for app.providers.constants — market detection, symbol normalization, metal search."""
from __future__ import annotations

import pytest

from app.providers.constants import (
    detect_market,
    is_precious_metal,
    normalize_symbol,
    search_metals,
)


# --- detect_market ---

class TestDetectMarket:
    def test_us_symbol(self):
        assert detect_market("AAPL") == "us"
        assert detect_market("MSFT") == "us"

    def test_hk_suffix(self):
        assert detect_market("0700.HK") == "hk"
        assert detect_market("9988.hk") == "hk"

    def test_shanghai_suffix(self):
        assert detect_market("600519.SS") == "sh"

    def test_shenzhen_suffix(self):
        assert detect_market("000858.SZ") == "sz"

    def test_precious_metals_before_us(self):
        assert detect_market("GC=F") == "metal"
        assert detect_market("SI=F") == "metal"
        assert detect_market("PL=F") == "metal"
        assert detect_market("PA=F") == "metal"

    def test_case_insensitive(self):
        assert detect_market("gc=f") == "metal"
        assert detect_market("aapl") == "us"


# --- normalize_symbol ---

class TestNormalizeSymbol:
    def test_us_passthrough(self):
        assert normalize_symbol("AAPL", "us") == "AAPL"

    def test_metal_passthrough(self):
        assert normalize_symbol("GC=F", "metal") == "GC=F"

    def test_hk_removes_suffix_and_pads(self):
        assert normalize_symbol("0700.HK", "hk") == "00700"
        assert normalize_symbol("9988.HK", "hk") == "09988"

    def test_hk_already_5_digits(self):
        assert normalize_symbol("00700.HK", "hk") == "00700"

    def test_sh_removes_suffix(self):
        assert normalize_symbol("600519.SS", "sh") == "600519"

    def test_sz_removes_suffix(self):
        assert normalize_symbol("000858.SZ", "sz") == "000858"

    def test_strips_whitespace(self):
        assert normalize_symbol("  AAPL  ", "us") == "AAPL"


# --- is_precious_metal ---

class TestIsPreciousMetal:
    def test_metals_return_true(self):
        assert is_precious_metal("GC=F") is True
        assert is_precious_metal("SI=F") is True

    def test_case_insensitive(self):
        assert is_precious_metal("gc=f") is True

    def test_stock_returns_false(self):
        assert is_precious_metal("AAPL") is False
        assert is_precious_metal("SI") is False


# --- search_metals ---

class TestSearchMetals:
    def test_english_keyword_gold(self):
        results = search_metals("gold")
        assert len(results) == 1
        assert results[0]["symbol"] == "GC=F"

    def test_chinese_keyword_gold(self):
        results = search_metals("黄金")
        assert len(results) == 1
        assert results[0]["symbol"] == "GC=F"

    def test_symbol_keyword_xau(self):
        results = search_metals("xau")
        assert len(results) == 1
        assert results[0]["symbol"] == "GC=F"

    def test_no_match(self):
        results = search_metals("bitcoin")
        assert results == []

    def test_result_has_expected_keys(self):
        results = search_metals("silver")
        assert len(results) == 1
        r = results[0]
        assert r["symbol"] == "SI=F"
        assert "name" in r
        assert "exchange" in r
        assert r["market"] == "metal"

    def test_platinum_chinese(self):
        results = search_metals("铂金")
        assert len(results) == 1
        assert results[0]["symbol"] == "PL=F"
