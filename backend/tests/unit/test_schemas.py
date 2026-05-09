"""Tests for Pydantic schemas — ApiResponse envelope and CamelModel aliasing."""
from __future__ import annotations

import pytest

from app.schemas.base import ApiResponse, CamelModel
from app.schemas.auth import LoginRequest, TokenResponse


class TestApiResponse:
    def test_default_success_true(self):
        r = ApiResponse()
        assert r.success is True
        assert r.data is None
        assert r.error is None

    def test_with_data(self):
        r = ApiResponse(data={"price": 150.0}, source="yfinance")
        assert r.data == {"price": 150.0}
        assert r.source == "yfinance"

    def test_error_response(self):
        r = ApiResponse(success=False, error="Not found")
        assert r.success is False
        assert r.error == "Not found"

    def test_elapsed_ms(self):
        r = ApiResponse(elapsed_ms=42)
        assert r.elapsed_ms == 42


class TestCamelModel:
    def test_snake_to_camel_serialization(self):
        class MyModel(CamelModel):
            some_field: str = "val"
            another_field_here: int = 1

        m = MyModel()
        d = m.model_dump(by_alias=True)
        assert "someField" in d
        assert "anotherFieldHere" in d
        assert "some_field" not in d

    def test_populate_by_name(self):
        class MyModel(CamelModel):
            some_field: str

        m = MyModel(some_field="hello")
        assert m.some_field == "hello"

    def test_populate_by_alias(self):
        class MyModel(CamelModel):
            some_field: str

        m = MyModel(someField="hello")
        assert m.some_field == "hello"


class TestAuthSchemas:
    def test_login_request(self):
        r = LoginRequest(email="a@b.com", password="pw")
        assert r.email == "a@b.com"

    def test_token_response_default_type(self):
        r = TokenResponse(access_token="at", refresh_token="rt")
        assert r.token_type == "bearer"

    def test_token_response_camel_serialization(self):
        r = TokenResponse(access_token="at", refresh_token="rt")
        d = r.model_dump(by_alias=True)
        assert "accessToken" in d
        assert "refreshToken" in d
        assert "tokenType" in d
