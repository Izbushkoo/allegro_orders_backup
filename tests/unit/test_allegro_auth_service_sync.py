"""
@file: tests/unit/test_allegro_auth_service_sync.py
@description: Unit-тесты для sync-методов AllegroAuthService
@dependencies: pytest, unittest.mock, AllegroAuthService, requests
"""
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
from app.services.allegro_auth_service import AllegroAuthService
from app.models.user_token import UserToken
from app.exceptions import ValidationError, InternalServerErrorHTTPException

class DummySession:
    pass

@pytest.fixture
def service():
    return AllegroAuthService(DummySession())

@patch('app.services.allegro_auth_service.requests.post')
def test_initialize_device_flow_sync_success(mock_post, service):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        'device_code': 'dev123',
        'user_code': 'user123',
        'verification_uri': 'https://allegro.pl/auth',
        'expires_in': 600
    }
    mock_post.return_value = mock_response
    result = service.initialize_device_flow_sync('user1')
    assert result['device_code'] == 'dev123'
    assert 'expires_at_iso' in result

@patch('app.services.allegro_auth_service.requests.post')
def test_initialize_device_flow_sync_fail(mock_post, service):
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.text = 'Bad Request'
    mock_post.return_value = mock_response
    with pytest.raises(ValidationError):
        service.initialize_device_flow_sync('user1')

@patch('app.services.allegro_auth_service.requests.post')
@patch.object(AllegroAuthService, 'token_service')
def test_check_auth_status_sync_completed(mock_token_service, mock_post, service):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        'access_token': 'token',
        'refresh_token': 'refresh',
        'expires_in': 3600
    }
    mock_post.return_value = mock_response
    mock_token_service.create_token_sync = MagicMock()
    result = service.check_auth_status_sync('devcode', 'user1')
    assert result['status'] == 'completed'
    mock_token_service.create_token_sync.assert_called_once()

@patch('app.services.allegro_auth_service.requests.post')
def test_check_auth_status_sync_pending(mock_post, service):
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.json.return_value = {'error': 'authorization_pending'}
    mock_post.return_value = mock_response
    result = service.check_auth_status_sync('devcode', 'user1')
    assert result['status'] == 'pending'

@patch('app.services.allegro_auth_service.requests.post')
def test_check_auth_status_sync_failed(mock_post, service):
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.json.return_value = {'error': 'access_denied'}
    mock_post.return_value = mock_response
    result = service.check_auth_status_sync('devcode', 'user1')
    assert result['status'] == 'failed'

@patch('app.services.allegro_auth_service.requests.post')
@patch.object(AllegroAuthService, 'token_service')
def test_refresh_token_sync_success(mock_token_service, mock_post, service):
    token = UserToken(id=1, user_id='user1', allegro_token='old', refresh_token='refresh', expires_at=datetime.utcnow())
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        'access_token': 'newtoken',
        'refresh_token': 'newrefresh',
        'expires_in': 3600
    }
    mock_post.return_value = mock_response
    mock_token_service.update_token_sync = MagicMock(return_value=token)
    result = service.refresh_token_sync(token)
    assert result == token
    mock_token_service.update_token_sync.assert_called_once()

@patch('app.services.allegro_auth_service.requests.post')
def test_refresh_token_sync_fail(mock_post, service):
    token = UserToken(id=1, user_id='user1', allegro_token='old', refresh_token='refresh', expires_at=datetime.utcnow())
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.text = 'Bad Request'
    mock_post.return_value = mock_response
    with pytest.raises(ValidationError):
        service.refresh_token_sync(token)

@patch('app.services.allegro_auth_service.requests.get')
def test_validate_token_sync_valid(mock_get, service):
    token = UserToken(id=1, user_id='user1', allegro_token='tok', refresh_token='ref', expires_at=datetime.utcnow())
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_get.return_value = mock_response
    assert service.validate_token_sync(token) is True

@patch('app.services.allegro_auth_service.requests.get')
def test_validate_token_sync_expired(mock_get, service):
    token = UserToken(id=1, user_id='user1', allegro_token='tok', refresh_token='ref', expires_at=datetime.utcnow())
    mock_response = MagicMock()
    mock_response.status_code = 401
    mock_get.return_value = mock_response
    assert service.validate_token_sync(token) is False

@patch('app.services.allegro_auth_service.requests.get')
def test_validate_token_sync_unexpected(mock_get, service):
    token = UserToken(id=1, user_id='user1', allegro_token='tok', refresh_token='ref', expires_at=datetime.utcnow())
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_get.return_value = mock_response
    assert service.validate_token_sync(token) is False 