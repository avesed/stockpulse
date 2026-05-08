"""Authentication schemas."""

from __future__ import annotations

from app.schemas.base import CamelModel


class LoginRequest(CamelModel):
    email: str
    password: str


class RegisterRequest(CamelModel):
    email: str
    password: str
    display_name: str | None = None


class TokenResponse(CamelModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class RefreshRequest(CamelModel):
    refresh_token: str


class LogoutRequest(CamelModel):
    refresh_token: str | None = None


class ChangePasswordRequest(CamelModel):
    current_password: str
    new_password: str


class UserResponse(CamelModel):
    id: int
    email: str
    display_name: str | None = None
    role: str
    locale: str
