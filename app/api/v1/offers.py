"""
@file: app/api/v1/offers.py
@description: API эндпоинты для работы с офферами Allegro
@dependencies: fastapi, pydantic, sqlmodel
"""

from typing import List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api.dependencies import DatabaseSession, CurrentUserDep
from app.core.auth import CurrentUser
from app.models.offer import ExternalStockUpdateRequest
from app.services.token_service import TokenService
from app.services.allegro_auth_service import AllegroAuthService
from app.services.offer_service import OfferService

router = APIRouter()

@router.post(
    "/update-stock",
    response_model=List[Dict[str, Any]],
    summary="Обновление запаса офферов",
    description="Обновление запаса офферов для всех токенов пользователя"
)
async def update_stock(
    body: ExternalStockUpdateRequest,
    current_user: CurrentUser = CurrentUserDep,
    session: AsyncSession = DatabaseSession
) -> List[Dict[str, Any]]:
    """
    Обновление запаса для офферов с указанным external_id
    """
    results: List[Dict[str, Any]] = []
    token_service = TokenService(session)
    tokens = await token_service.get_user_tokens(current_user.user_id)
    if not tokens:
        raise HTTPException(status_code=404, detail="Активные токены не найдены")

    auth_service = AllegroAuthService(session)
    for token in tokens:
        valid_token = await auth_service.validate_and_refresh_token(token)
        if not valid_token:
            results.append({"token_id": str(token.id), "error": "Недействительный токен"})
            continue
        access_token = valid_token.allegro_token

        try:
            offers = await OfferService.get_offers_by_external_id(
                current_user.user_id,
                access_token,
                body.external_id
            )
        except Exception as e:
            results.append({
                "token_id": str(token.id),
                "error": f"Ошибка получения офферов: {str(e)}"
            })
            continue

        for offer in offers:
            offer_id = offer.get("id")
            in_stock = offer.get("available", {}).get("stock")
            if in_stock is None:
                continue
            if in_stock != body.stock:
                try:
                    update_result = await OfferService.update_offer_stock(
                        current_user.user_id,
                        access_token,
                        offer_id,
                        body.stock
                    )
                    results.append({
                        "token_id": str(token.id),
                        "offer_id": offer_id,
                        "updated": True,
                        "result": update_result
                    })
                except Exception as e:
                    results.append({
                        "token_id": str(token.id),
                        "offer_id": offer_id,
                        "updated": False,
                        "error": str(e)
                    })
            else:
                results.append({
                    "token_id": str(token.id),
                    "offer_id": offer_id,
                    "updated": False,
                    "note": "Запас не изменился"
                })
    return results