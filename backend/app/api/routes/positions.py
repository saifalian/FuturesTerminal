from fastapi import APIRouter

router = APIRouter(prefix="/positions", tags=["positions"])


@router.get("")
async def list_positions() -> dict:
    return {"positions": []}
