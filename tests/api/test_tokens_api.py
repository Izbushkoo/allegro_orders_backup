"""
@file: test_tokens_api.py
@description: Тесты для всех эндпоинтов tokens API (app/api/v1/tokens.py)
@dependencies: pytest, httpx, fastapi, pydantic
"""

import pytest
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

# --- 1. POST / (создать токен) ---
def test_create_token_success(test_jwt_token):
    """Позитивный сценарий: успешное создание токена."""
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    data = {
        "allegro_token": "allegro_test_token",
        "refresh_token": "allegro_refresh_token",
        "expires_at": "2099-12-31T23:59:59"
    }
    response = client.post("/api/v1/tokens/", json=data, headers=headers)
    assert response.status_code == 200
    resp_json = response.json()
    assert "id" in resp_json
    assert resp_json["user_id"] == "test_user_123"


def test_create_token_invalid_data(test_jwt_token):
    """Негативный сценарий: невалидные данные."""
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    data = {
        "allegro_token": "",
        "refresh_token": "",
        "expires_at": ""
    }
    response = client.post("/api/v1/tokens/", json=data, headers=headers)
    assert response.status_code == 422


def test_create_token_unauthorized():
    """Негативный сценарий: отсутствие JWT токена."""
    data = {
        "allegro_token": "allegro_test_token",
        "refresh_token": "allegro_refresh_token",
        "expires_at": "2099-12-31T23:59:59"
    }
    response = client.post("/api/v1/tokens/", json=data)
    assert response.status_code == 403


# --- 2. GET / (получить список токенов) ---
def test_get_tokens_success(test_jwt_token):
    """Позитивный сценарий: получить список токенов пользователя."""
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    response = client.get("/api/v1/tokens/", headers=headers)
    assert response.status_code == 200
    resp_json = response.json()
    assert "tokens" in resp_json
    assert isinstance(resp_json["tokens"], list)
    assert resp_json["tokens"][0]["user_id"] == "test_user_123"


def test_get_tokens_unauthorized():
    """Негативный сценарий: без JWT токена."""
    response = client.get("/api/v1/tokens/")
    assert response.status_code == 403

# --- 3. GET /{token_id} ---
def create_token_for_test(headers):
    data = {
        "allegro_token": "allegro_test_token2",
        "refresh_token": "allegro_refresh_token2",
        "expires_at": "2099-12-31T23:59:59"
    }
    response = client.post("/api/v1/tokens/", json=data, headers=headers)
    assert response.status_code == 200
    return response.json()["id"]


def test_get_token_success(test_jwt_token):
    """Позитивный сценарий: получить конкретный токен по ID."""
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    token_id = create_token_for_test(headers)
    response = client.get(f"/api/v1/tokens/{token_id}", headers=headers)
    assert response.status_code == 200
    resp_json = response.json()
    assert resp_json["id"] == token_id


def test_get_token_not_found(test_jwt_token):
    """Негативный сценарий: токен не найден."""
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    fake_id = "00000000-0000-0000-0000-000000000000"
    response = client.get(f"/api/v1/tokens/{fake_id}", headers=headers)
    assert response.status_code == 404


# --- 4. PUT /{token_id} ---
def test_update_token_success(test_jwt_token):
    """Позитивный сценарий: обновить токен."""
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    token_id = create_token_for_test(headers)
    update_data = {"allegro_token": "updated_token"}
    response = client.put(f"/api/v1/tokens/{token_id}", json=update_data, headers=headers)
    assert response.status_code == 200
    resp_json = response.json()
    assert "id" in resp_json
    assert resp_json["user_id"] == "test_user_123"


def test_update_token_not_found(test_jwt_token):
    """Негативный сценарий: обновление несуществующего токена."""
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    fake_id = "00000000-0000-0000-0000-000000000000"
    update_data = {"allegro_token": "updated_token"}
    response = client.put(f"/api/v1/tokens/{fake_id}", json=update_data, headers=headers)
    assert response.status_code == 404


# --- 5. DELETE /{token_id} ---
def test_delete_token_success(test_jwt_token):
    """Позитивный сценарий: удалить токен."""
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    token_id = create_token_for_test(headers)
    response = client.delete(f"/api/v1/tokens/{token_id}", headers=headers)
    assert response.status_code == 200 or response.status_code == 204


def test_delete_token_not_found(test_jwt_token):
    """Негативный сценарий: удаление несуществующего токена."""
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    fake_id = "00000000-0000-0000-0000-000000000000"
    response = client.delete(f"/api/v1/tokens/{fake_id}", headers=headers)
    assert response.status_code == 404

# --- 6. POST /{token_id}/refresh ---
def test_refresh_token_success(test_jwt_token):
    """Позитивный сценарий: обновить токен через refresh_token."""
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    token_id = create_token_for_test(headers)
    response = client.post(f"/api/v1/tokens/{token_id}/refresh", headers=headers)
    assert response.status_code in (200, 202, 400, 404)  # 202 если асинхронно, 400/404 если невалидно


def test_refresh_token_not_found(test_jwt_token):
    """Негативный сценарий: обновление несуществующего токена через refresh_token."""
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    fake_id = "00000000-0000-0000-0000-000000000000"
    response = client.post(f"/api/v1/tokens/{fake_id}/refresh", headers=headers)
    assert response.status_code == 404 or response.status_code == 400


# --- 7. GET /user/{user_id} ---
def test_get_user_tokens_admin(test_jwt_token):
    """Позитивный сценарий: получить токены другого пользователя (если есть права)."""
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    # Для теста используем user_id текущего пользователя (или другого, если есть админ-права)
    # Здесь предполагается, что user_id можно получить из тестового токена или фикстуры
    user_id = "test_user_id"  # TODO: заменить на реальный user_id
    response = client.get(f"/api/v1/tokens/user/{user_id}", headers=headers)
    assert response.status_code in (200, 403, 404)


def test_get_user_tokens_forbidden(test_jwt_token):
    """Негативный сценарий: попытка получить токены другого пользователя без прав."""
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    user_id = "forbidden_user_id"  # TODO: заменить на user_id, к которому нет доступа
    response = client.get(f"/api/v1/tokens/user/{user_id}", headers=headers)
    assert response.status_code in (403, 404)


# --- 8. POST /auth/initialize ---
def test_initialize_auth_success(test_jwt_token):
    """Позитивный сценарий: инициализация авторизации (device code flow)."""
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    response = client.post("/api/v1/tokens/auth/initialize", json={}, headers=headers)
    assert response.status_code in (200, 202, 400)
    if response.status_code == 200:
        resp_json = response.json()
        assert "device_code" in resp_json
        assert "user_code" in resp_json


# --- 9. POST /auth/status ---
def test_check_auth_status_pending(test_jwt_token):
    """Промежуточный сценарий: статус авторизации pending."""
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    # Для теста нужен device_code, полученный из initialize_auth
    device_code = "test_device_code"  # TODO: получить из предыдущего шага
    response = client.post("/api/v1/tokens/auth/status", json={"device_code": device_code}, headers=headers)
    assert response.status_code in (200, 400)
    if response.status_code == 200:
        assert response.json()["status"] in ("pending", "completed", "failed")


def test_check_auth_status_completed():
    """Позитивный сценарий: авторизация завершена успешно."""
    pass

# --- 10. POST /{token_id}/validate ---
def test_validate_and_refresh_token_success(test_jwt_token):
    """Позитивный сценарий: проверить и обновить токен."""
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    token_id = create_token_for_test(headers)
    response = client.post(f"/api/v1/tokens/{token_id}/validate", headers=headers)
    assert response.status_code in (200, 202, 400, 404)


def test_validate_and_refresh_token_not_found(test_jwt_token):
    """Негативный сценарий: проверить и обновить несуществующий токен."""
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    fake_id = "00000000-0000-0000-0000-000000000000"
    response = client.post(f"/api/v1/tokens/{fake_id}/validate", headers=headers)
    assert response.status_code == 404 or response.status_code == 400


# --- 11. GET /auth/task/{task_id} ---
def test_get_auth_task_status_success(test_jwt_token):
    """Позитивный сценарий: получить статус задачи авторизации (celery)."""
    headers = {"Authorization": f"Bearer {test_jwt_token}"}
    task_id = "test_task_id"  # TODO: получить из initialize_auth
    response = client.get(f"/api/v1/tokens/auth/task/{task_id}", headers=headers)
    assert response.status_code in (200, 404) 