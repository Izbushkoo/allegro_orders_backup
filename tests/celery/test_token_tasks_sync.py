"""
@file: tests/celery/test_token_tasks_sync.py
@description: Unit-тесты для Celery-задачи poll_authorization_status (sync)
@dependencies: pytest, unittest.mock, Celery, AllegroAuthService
"""
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
from app.tasks.token_tasks import poll_authorization_status

class DummyRequest:
    def __init__(self, retries=0, max_retries=3):
        self.retries = retries
        self.max_retries = max_retries

class DummySelf:
    def __init__(self, retries=0, max_retries=3):
        self.request = DummyRequest(retries, max_retries)
        self.retry_called = False
        self.retry_args = None
    def retry(self, *args, **kwargs):
        self.retry_called = True
        self.retry_args = (args, kwargs)
        raise Exception('retry')

@patch('app.tasks.token_tasks.get_sync_db_session_direct')
@patch('app.tasks.token_tasks.AllegroAuthService')
def test_poll_authorization_status_completed(mock_service_cls, mock_get_db):
    mock_service = MagicMock()
    mock_service.check_auth_status_sync.return_value = {'status': 'completed'}
    mock_service_cls.return_value = mock_service
    mock_get_db.return_value = MagicMock(close=MagicMock())
    self = DummySelf()
    expires_at = (datetime.utcnow() + timedelta(minutes=10)).isoformat()
    result = poll_authorization_status(self, 'devcode', 'user1', expires_at)
    assert result['status'] == 'completed'
    assert not self.retry_called

@patch('app.tasks.token_tasks.get_sync_db_session_direct')
@patch('app.tasks.token_tasks.AllegroAuthService')
def test_poll_authorization_status_failed(mock_service_cls, mock_get_db):
    mock_service = MagicMock()
    mock_service.check_auth_status_sync.return_value = {'status': 'failed', 'message': 'fail'}
    mock_service_cls.return_value = mock_service
    mock_get_db.return_value = MagicMock(close=MagicMock())
    self = DummySelf()
    expires_at = (datetime.utcnow() + timedelta(minutes=10)).isoformat()
    result = poll_authorization_status(self, 'devcode', 'user1', expires_at)
    assert result['status'] == 'failed'
    assert not self.retry_called

@patch('app.tasks.token_tasks.get_sync_db_session_direct')
@patch('app.tasks.token_tasks.AllegroAuthService')
def test_poll_authorization_status_pending_retry(mock_service_cls, mock_get_db):
    mock_service = MagicMock()
    mock_service.check_auth_status_sync.return_value = {'status': 'pending'}
    mock_service_cls.return_value = mock_service
    mock_get_db.return_value = MagicMock(close=MagicMock())
    self = DummySelf()
    expires_at = (datetime.utcnow() + timedelta(minutes=10)).isoformat()
    with pytest.raises(Exception) as exc:
        poll_authorization_status(self, 'devcode', 'user1', expires_at)
    assert self.retry_called
    assert 'retry' in str(exc.value)

@patch('app.tasks.token_tasks.get_sync_db_session_direct')
@patch('app.tasks.token_tasks.AllegroAuthService')
def test_poll_authorization_status_expired(mock_service_cls, mock_get_db):
    self = DummySelf()
    expires_at = (datetime.utcnow() - timedelta(minutes=1)).isoformat()
    result = poll_authorization_status(self, 'devcode', 'user1', expires_at)
    assert result['status'] == 'expired'
    assert not self.retry_called

@patch('app.tasks.token_tasks.get_sync_db_session_direct')
@patch('app.tasks.token_tasks.AllegroAuthService')
def test_poll_authorization_status_max_retries(mock_service_cls, mock_get_db):
    mock_service = MagicMock()
    mock_service.check_auth_status_sync.side_effect = Exception('fail')
    mock_service_cls.return_value = mock_service
    mock_get_db.return_value = MagicMock(close=MagicMock())
    self = DummySelf(retries=3, max_retries=3)
    expires_at = (datetime.utcnow() + timedelta(minutes=10)).isoformat()
    result = poll_authorization_status(self, 'devcode', 'user1', expires_at)
    assert result['status'] == 'failed'
    assert not self.retry_called 