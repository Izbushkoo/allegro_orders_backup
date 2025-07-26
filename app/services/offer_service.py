"""
@file: app/services/offer_service.py
@description: Сервис для работы с офферами Allegro API
@dependencies: httpx
"""

import httpx
from typing import List, Dict, Any

from app.core.settings import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

class OfferService:
    """
    Сервис для работы с офферами пользователя в Allegro API.
    """
    API_URL: str = settings.allegro.api_url
    SEARCH_OFFERS_PATH: str = "/sale/offers"
    EDIT_OFFERS_PATH: str = "/sale/offer-management/product-offers"

    @classmethod
    async def get_offers_by_external_id(
        cls,
        user_id: str,
        token: str,
        external_id: str
    ) -> List[Dict[str, Any]]:
        """
        Получить офферы пользователя по external.id.

        Args:
            user_id: ID пользователя (для логирования)
            token: Access token Allegro
            external_id: Значение external.id для фильтрации офферов

        Returns:
            Список офферов в виде словарей
        """
        logger.info(f"Пользователь {user_id}: запрос офферов по external_id={external_id}")
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.allegro.public.v1+json"
        }
        params = {"external.id": external_id}
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                f"{cls.API_URL}{cls.SEARCH_OFFERS_PATH}",
                headers=headers,
                params=params
            )
            response.raise_for_status()
            data = response.json()
        offers = data.get("offers", [])
        logger.info(f"Найдено {len(offers)} офферов для external_id={external_id}")
        return offers

    @classmethod
    async def update_offer_stock(
        cls,
        user_id: str,
        token: str,
        offer_id: str,
        new_stock: int
    ) -> Dict[str, Any]:
        """
        Обновить количество товара в оффере.

        Args:
            user_id: ID пользователя (для логирования)
            token: Access token Allegro
            offer_id: ID оффера
            new_stock: Новое значение запаса

        Returns:
            Результат обновления оффера
        """
        logger.info(f"Пользователь {user_id}: обновление запаса оффера {offer_id} -> {new_stock}")
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.allegro.public.v1+json",
            "Content-Type": "application/vnd.allegro.public.v1+json"
        }
        body = {
            "offers": [
                {"id": offer_id, "stock": new_stock}
            ]
        }
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.put(
                f"{cls.API_URL}{cls.EDIT_OFFERS_PATH}",
                headers=headers,
                json=body
            )
            response.raise_for_status()
            result = response.json()
        logger.info(f"Оффер {offer_id} обновлен: {result}")
        return result