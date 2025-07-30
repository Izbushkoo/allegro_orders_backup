"""
@file: app/api/v1/offers.py
@description: API эндпоинты для работы с офферами Allegro
@dependencies: fastapi, pydantic, sqlmodel
"""

from typing import List, Dict, Any, Optional
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api.dependencies import DatabaseSession, CurrentUserDep
from app.core.auth import CurrentUser
from app.models.offer import ExternalStockUpdateRequest
from app.services.token_service import TokenService
from app.services.offer_service import OfferService

router = APIRouter()

@router.get(
    "/by-external-id",
    response_model=List[Dict[str, Any]],
    summary="Получение офферов по external_id",
    description="Получение всех офферов с указанным external_id для выбранных токенов"
)
async def get_offers_by_external_id(
    token_ids: List[UUID] = Query(..., description="Список ID токенов для доступа к Allegro API"),
    external_id: str = Query(..., description="External ID для поиска офферов"),
    current_user: CurrentUser = CurrentUserDep,
    session: AsyncSession = DatabaseSession
) -> List[Dict[str, Any]]:
    """
    Получить все офферы с указанным external_id для выбранных токенов пользователя.
    
    **Требует аутентификации через JWT токен.**
    """
    results = []
    token_service = TokenService(session)
    
    for token_id in token_ids:
        try:
            # Получаем и валидируем токен
            token = await token_service.get_user_token_by_id(token_id, current_user.user_id)
            
            if not token:
                results.append({
                    "token_id": str(token_id),
                    "error": "Токен не найден или не принадлежит пользователю",
                    "offers": []
                })
                continue
            
            # Валидируем и обновляем токен если нужно
            valid_token = await token_service.validate_and_refresh_token(token.id)
            
            if not valid_token:
                results.append({
                    "token_id": str(token_id),
                    "error": "Токен недействителен или не может быть обновлен",
                    "offers": []
                })
                continue
            
            # Получаем офферы
            offers = await OfferService.get_offers_by_external_id(
                current_user.user_id,
                valid_token.allegro_token,
                external_id
            )
            
            results.append({
                "token_id": str(token_id),
                "account_name": valid_token.account_name,
                "offers": offers,
                "offers_count": len(offers)
            })
            
        except Exception as e:
            results.append({
                "token_id": str(token_id),
                "error": f"Ошибка при получении офферов: {str(e)}",
                "offers": []
            })
    
    return results


@router.post(
    "/update-stock",
    response_model=List[Dict[str, Any]],
    summary="Обновление запаса офферов",
    description="Обновление запаса офферов для указанных токенов пользователя"
)
async def update_stock(
    body: ExternalStockUpdateRequest,
    token_ids: Optional[List[UUID]] = Query(None, description="Список ID токенов (если не указан - для всех токенов)"),
    current_user: CurrentUser = CurrentUserDep,
    session: AsyncSession = DatabaseSession
) -> List[Dict[str, Any]]:
    """
    Обновление запаса для офферов с указанным external_id
    """
    results: List[Dict[str, Any]] = []
    token_service = TokenService(session)
    
    # Получаем токены для обработки
    if token_ids:
        # Если указаны конкретные токены - проверяем каждый
        tokens = []
        for token_id in token_ids:
            token = await token_service.get_user_token_by_id(token_id, current_user.user_id)
            if token:
                tokens.append(token)
            else:
                results.append({
                    "token_id": str(token_id),
                    "error": "Токен не найден или не принадлежит пользователю"
                })
    else:
        # Если токены не указаны - используем все активные токены пользователя
        tokens = await token_service.get_user_tokens(current_user.user_id)
    
    if not tokens:
        raise HTTPException(status_code=404, detail="Активные токены не найдены")

    for token in tokens:
        # Валидируем токен
        valid_token = await token_service.validate_and_refresh_token(token.id)
        if not valid_token:
            results.append({
                "token_id": str(token.id), 
                "account_name": token.account_name,
                "error": "Недействительный токен"
            })
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
                "account_name": token.account_name,
                "error": f"Ошибка получения офферов: {str(e)}"
            })
            continue

        for offer in offers:
            offer_id = offer.get("id")
            current_stock = offer.get("stock", {}).get("available")
            
            if current_stock is None:
                results.append({
                    "token_id": str(token.id),
                    "account_name": token.account_name,
                    "offer_id": offer_id,
                    "updated": False,
                    "note": "Информация о стоке недоступна"
                })
                continue
                
            if current_stock != body.stock:
                try:
                    update_result = await OfferService.update_offer_stock(
                        current_user.user_id,
                        access_token,
                        offer_id,
                        body.stock
                    )
                    results.append({
                        "token_id": str(token.id),
                        "account_name": token.account_name,
                        "offer_id": offer_id,
                        "old_stock": current_stock,
                        "new_stock": body.stock,
                        "updated": True,
                        "result": update_result
                    })
                except Exception as e:
                    results.append({
                        "token_id": str(token.id),
                        "account_name": token.account_name,
                        "offer_id": offer_id,
                        "updated": False,
                        "error": str(e)
                    })
            else:
                results.append({
                    "token_id": str(token.id),
                    "account_name": token.account_name,
                    "offer_id": offer_id,
                    "updated": False,
                    "note": "Запас не изменился"
                })
    return results