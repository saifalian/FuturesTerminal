from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

router = APIRouter(prefix="/terminal", tags=["terminal"])


def _require_live_enabled(request: Request) -> None:
    controls = getattr(request.app.state, "workload_controls", None) or {}
    if not bool(controls.get("live_streaming_enabled", True)):
        raise HTTPException(status_code=503, detail="Live streaming service is disabled in Settings.")


@router.get("/snapshot")
async def terminal_snapshot(request: Request, tab_id: str = Query(default="default")) -> dict:
    _require_live_enabled(request)
    manager = request.app.state.tab_sessions
    return await manager.snapshot(tab_id)


@router.get("/heatmap/bootstrap")
async def terminal_heatmap_bootstrap(request: Request, tab_id: str = Query(default="default")) -> dict:
    _require_live_enabled(request)
    manager = request.app.state.tab_sessions
    return await manager.heatmap_bootstrap(tab_id)


@router.get("/source-coverage")
async def terminal_source_coverage(request: Request, tab_id: str = Query(default="default")) -> dict:
    _require_live_enabled(request)
    manager = request.app.state.tab_sessions
    return await manager.source_coverage(tab_id)


class SourceCoverageSelectionRequest(BaseModel):
    tab_id: str = "default"
    enabled_exchange_ids: list[str] = Field(default_factory=list)


@router.post("/source-coverage/selection")
async def terminal_source_coverage_selection(
    request: Request, payload: SourceCoverageSelectionRequest
) -> dict:
    _require_live_enabled(request)
    manager = request.app.state.tab_sessions
    return await manager.apply_exchange_selection(payload.tab_id, payload.enabled_exchange_ids)


class LadderRangeRequest(BaseModel):
    tab_id: str = "default"
    range_multiplier: int = 1


class FeedCadenceRequest(BaseModel):
    tab_id: str = "default"
    on_screen_ms: int = Field(default=500)
    background_ms: int = Field(default=2000)
    render_ms: int = Field(default=500)


@router.post("/ladder/range")
async def terminal_ladder_range(request: Request, payload: LadderRangeRequest) -> dict:
    _require_live_enabled(request)
    manager = request.app.state.tab_sessions
    return await manager.apply_ladder_range(payload.tab_id, payload.range_multiplier)


@router.get("/feed-cadence")
async def terminal_feed_cadence(request: Request, tab_id: str = Query(default="default")) -> dict:
    _require_live_enabled(request)
    manager = request.app.state.tab_sessions
    cadence = await manager.get_feed_cadence(tab_id)
    return {"tab_id": tab_id, **cadence}


@router.post("/feed-cadence")
async def terminal_set_feed_cadence(request: Request, payload: FeedCadenceRequest) -> dict:
    _require_live_enabled(request)
    manager = request.app.state.tab_sessions
    cadence = await manager.set_feed_cadence(
        tab_id=payload.tab_id,
        on_screen_ms=payload.on_screen_ms,
        background_ms=payload.background_ms,
        render_ms=payload.render_ms,
    )
    return {"ok": True, "tab_id": payload.tab_id, **cadence}


@router.delete("/tab")
async def terminal_close_tab(request: Request, tab_id: str = Query(default="default")) -> dict:
    manager = request.app.state.tab_sessions
    closed = await manager.close_tab(tab_id)
    return {"ok": True, "closed": closed}
