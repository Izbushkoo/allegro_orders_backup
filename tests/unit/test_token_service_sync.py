"""
@file: tests/unit/test_token_service_sync.py
@description: Unit-тесты для sync-методов TokenService
@dependencies: pytest, unittest.mock, TokenService, SQLModel
"""
import pytest
from unittest.mock import MagicMock
from datetime import datetime, timedelta
from app.services.token_service import TokenService
from app.models.user_token import UserToken

class DummySession:
    def __init__(self):
        self.added = []
        self.committed = False
        self.refreshed = []
        self._tokens = {}
    def add(self, obj):
        self.added.append(obj)
        if hasattr(obj, 'id'):
            self._tokens[obj.id] = obj
    def commit(self):
        self.committed = True
    def refresh(self, obj):
        self.refreshed.append(obj)
    def exec(self, query):
        class Result:
            def __init__(self, token):
                self._token = token
            def first(self):
                return self._token
        # query.where(UserToken.id == token_id)
        # Симулируем поиск по id
        try:
            token_id = query._whereclause.right.value
            return Result(self._tokens.get(token_id))
        except Exception:
            return Result(None)

@pytest.fixture
def session():
    return DummySession()

@pytest.fixture
def service(session):
    return TokenService(session)

def test_create_token_sync_success(service, session):
    token = service.create_token_sync(
        user_id='user1',
        allegro_token='tok',
        refresh_token='ref',
        expires_at=datetime.utcnow() + timedelta(hours=1)
    )
    assert token.user_id == 'user1'
    assert session.committed
    assert token in session.refreshed

def test_update_token_sync_success(service, session):
    # Сначала создаём токен
    token = service.create_token_sync(
        user_id='user2',
        allegro_token='tok2',
        refresh_token='ref2',
        expires_at=datetime.utcnow() + timedelta(hours=1)
    )
    update_data = {'allegro_token': 'newtok', 'refresh_token': 'newref'}
    updated = service.update_token_sync(token.id, update_data)
    assert updated.allegro_token == 'newtok'
    assert updated.refresh_token == 'newref'
    assert session.committed
    assert updated in session.refreshed

def test_update_token_sync_not_found(service, session):
    import uuid
    fake_id = uuid.uuid4()
    with pytest.raises(ValueError):
        service.update_token_sync(fake_id, {'allegro_token': 'x'}) 