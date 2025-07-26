"""
@file: test_offers_api.py
@description: Тесты для эндпоинта обновления запаса офферов (/api/v1/offers/update-stock)
@dependencies: pytest, httpx, fastapi, unittest.mock
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, AsyncMock, MagicMock

from app.main import app
from app.services.offer_service import OfferService
from app.services.allegro_auth_service import AllegroAuthService
from app.services.token_service import TokenService

client = TestClient(app)


def test_update_stock_success(test_jwt_token):
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    body = {"external_id": "ext1", "stock": 10}
    mock_token = MagicMock(id="token1", allegro_token="access1")
    offer = {"id": "offer1", "available": {"stock": 5}}
    update_res = {"offers": [{"id": "offer1", "stock": 10}]}

    with patch.object(TokenService, "get_user_tokens", AsyncMock(return_value=[mock_token])), \
         patch.object(AllegroAuthService, "validate_and_refresh_token", AsyncMock(return_value=mock_token)), \
         patch.object(OfferService, "get_offers_by_external_id", AsyncMock(return_value=[offer])), \
         patch.object(OfferService, "update_offer_stock", AsyncMock(return_value=update_res)):

        response = client.post("/api/v1/offers/update-stock", json=body, headers=headers)

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["offer_id"] == "offer1"
    assert data[0]["updated"] is True
    assert data[0]["result"] == update_res


def test_update_stock_no_change(test_jwt_token):
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    body = {"external_id": "ext2", "stock": 20}
    mock_token = MagicMock(id="token2", allegro_token="access2")
    offer = {"id": "offer2", "available": {"stock": 20}}

    with patch.object(TokenService, "get_user_tokens", AsyncMock(return_value=[mock_token])), \
         patch.object(AllegroAuthService, "validate_and_refresh_token", AsyncMock(return_value=mock_token)), \
         patch.object(OfferService, "get_offers_by_external_id", AsyncMock(return_value=[offer])):

        response = client.post("/api/v1/offers/update-stock", json=body, headers=headers)

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["offer_id"] == "offer2"
    assert data[0]["updated"] is False
    assert "note" in data[0]
    assert data[0]["note"] == "Запас не изменился"


def test_update_stock_no_offers(test_jwt_token):
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    body = {"external_id": "no_ext", "stock": 5}
    mock_token = MagicMock(id="token3", allegro_token="access3")

    with patch.object(TokenService, "get_user_tokens", AsyncMock(return_value=[mock_token])), \
         patch.object(AllegroAuthService, "validate_and_refresh_token", AsyncMock(return_value=mock_token)), \
         patch.object(OfferService, "get_offers_by_external_id", AsyncMock(return_value=[])):

        response = client.post("/api/v1/offers/update-stock", json=body, headers=headers)

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert data == []


def test_update_stock_invalid_token(test_jwt_token):
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    body = {"external_id": "ext4", "stock": 15}
    mock_token = MagicMock(id="token4", allegro_token="access4")

    with patch.object(TokenService, "get_user_tokens", AsyncMock(return_value=[mock_token])), \
         patch.object(AllegroAuthService, "validate_and_refresh_token", AsyncMock(return_value=None)):

        response = client.post("/api/v1/offers/update-stock", json=body, headers=headers)

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["token_id"] == str(mock_token.id)
    assert "error" in data[0]
    assert data[0]["error"] == "Недействительный токен"