from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field
from app.services.storage.storage_management import StorageManagementService

router = APIRouter(prefix="/ml", tags=["ml"])


def _manager(request: Request):
    manager = getattr(request.app.state, "ml_runtime", None)
    if manager is None:
        raise HTTPException(status_code=503, detail="ML runtime is not available.")
    return manager


def _workload_controls(request: Request) -> dict[str, Any]:
    controls = getattr(request.app.state, "workload_controls", None)
    if isinstance(controls, dict):
        return controls
    return {
        "live_streaming_enabled": True,
        "replay_enabled": True,
        "training_enabled": True,
        "bots_enabled": True,
        "backfill_enabled": True,
        "training_ram_budget_gb": None,
        "training_prefetch_enabled": False,
        "training_chunk_group_size": "auto",
        "training_process_recycle_enabled": True,
        "training_worker_groups_before_restart": 1,
        "training_worker_memory_cap_gb": None,
        "keep_training_shards": False,
    }


def _require_control(request: Request, key: str, label: str) -> None:
    controls = _workload_controls(request)
    if not bool(controls.get(key, True)):
        raise HTTPException(status_code=503, detail=f"{label} service is disabled in Settings.")


def _parse_klines_rows(rows: list[Any] | None) -> list[dict[str, Any]]:
    candles: list[dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, list) or len(row) < 5:
            continue
        try:
            candles.append(
                {
                    "ts_ms": int(float(row[0])),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                }
            )
        except Exception:
            continue
    return candles


class RunStartPayload(BaseModel):
    pair_symbol: str | None = Field(default=None, min_length=3, max_length=20)
    instance_id: str | None = Field(default=None, min_length=6, max_length=128)
    dataset_source: str = Field(default="features_manifest", min_length=3, max_length=64)


class RunControlPayload(BaseModel):
    run_id: str = Field(min_length=4, max_length=128)


class BackfillControlPayload(BaseModel):
    job_id: str = Field(min_length=6, max_length=128)


class InstanceCreatePayload(BaseModel):
    pair_symbol: str = Field(min_length=3, max_length=20)
    name: str = Field(min_length=1, max_length=80)


class InstanceUpdatePayload(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=80)
    archived: bool | None = Field(default=None)


class BotSessionCreatePayload(BaseModel):
    name: str = Field(min_length=1, max_length=80)


class BotSessionUpdatePayload(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=80)
    set_active: bool | None = Field(default=None)


class ReplayStartPayload(BaseModel):
    run_id: str = Field(min_length=4, max_length=128)
    source_mode: str = Field(default="prediction_trace", min_length=3, max_length=64)
    mode: str = Field(default="test_trace", min_length=2, max_length=32)
    speed: float = Field(default=1.0, ge=0.1, le=10.0)
    epoch_index: int | None = Field(default=None, ge=0, le=10000)
    split: str = Field(default="test", min_length=3, max_length=8)


class ReplayControlPayload(BaseModel):
    action: str = Field(default="pause", min_length=3, max_length=24)
    cursor: int | None = Field(default=None, ge=0)
    speed: float | None = Field(default=None, ge=0.1, le=10.0)
    source_mode: str | None = Field(default=None, min_length=3, max_length=64)
    epoch_index: int | None = Field(default=None, ge=0, le=10000)
    split: str | None = Field(default=None, min_length=3, max_length=8)


class StorageDeletePayload(BaseModel):
    targets: list[dict[str, Any]] = Field(default_factory=list)


@router.get("/pairs")
async def ml_pairs(request: Request) -> dict[str, Any]:
    manager = _manager(request)
    pairs = await manager.list_pairs()
    return {"pairs": pairs}


@router.get("/pairs/{pair_symbol}/profile")
async def ml_pair_profile(request: Request, pair_symbol: str) -> dict[str, Any]:
    manager = _manager(request)
    return await manager.get_profile(pair_symbol)


@router.post("/pairs/{pair_symbol}/profile")
async def ml_pair_profile_save(request: Request, pair_symbol: str, payload: dict[str, Any]) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.save_profile(pair_symbol, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/pairs/{pair_symbol}/data-status")
async def ml_pair_data_status(
    request: Request,
    pair_symbol: str,
    dataset_source: str = Query(default="features_manifest"),
) -> dict[str, Any]:
    manager = _manager(request)
    return await manager.data_status_for_source(pair_symbol, dataset_source=dataset_source)


@router.get("/pairs/{pair_symbol}/backfill")
async def ml_pair_backfill_status(request: Request, pair_symbol: str) -> dict[str, Any]:
    manager = _manager(request)
    payload = await manager.latest_backfill_for_pair(pair_symbol)
    return {"pair_symbol": pair_symbol.upper(), "job": payload}


@router.get("/runs")
async def ml_runs(
    request: Request,
    pair_symbol: str | None = Query(default=None),
    instance_id: str | None = Query(default=None),
) -> dict[str, Any]:
    manager = _manager(request)
    runs = await manager.list_runs(pair_symbol=pair_symbol, instance_id=instance_id)
    return {"runs": runs}


@router.post("/runs/start")
async def ml_run_start(request: Request, payload: RunStartPayload) -> dict[str, Any]:
    _require_control(request, "training_enabled", "Training")
    manager = _manager(request)
    try:
        if payload.instance_id:
            return await manager.start_run_by_instance(payload.instance_id, dataset_source=payload.dataset_source)
        if not payload.pair_symbol:
            raise ValueError("pair_symbol or instance_id is required.")
        return await manager.start_run(payload.pair_symbol, dataset_source=payload.dataset_source)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/pause")
async def ml_run_pause(request: Request, payload: RunControlPayload) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.pause_run(payload.run_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/stop")
async def ml_run_stop(request: Request, payload: RunControlPayload) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.stop_run(payload.run_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/runs/{run_id}")
async def ml_run_status(request: Request, run_id: str) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.run_status(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/runs/{run_id}/logs")
async def ml_run_logs(request: Request, run_id: str) -> dict[str, Any]:
    manager = _manager(request)
    logs = await manager.run_logs(run_id)
    return {"run_id": run_id, "logs": logs}


@router.get("/runs/{run_id}/metrics")
async def ml_run_metrics(request: Request, run_id: str) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.run_metrics(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/runs/{run_id}/prediction-trace")
async def ml_run_prediction_trace(
    request: Request,
    run_id: str,
    split: str = Query(default="test"),
    epoch_index: int = Query(default=0, ge=0, le=10000),
    limit: int = Query(default=250000, ge=1, le=500000),
) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.run_prediction_trace(
            run_id,
            split=split,
            epoch_index=epoch_index,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.delete("/runs/{run_id}")
async def ml_run_delete(request: Request, run_id: str) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.delete_run(run_id)
    except ValueError as exc:
        code = 404 if "not found" in str(exc).lower() else 400
        raise HTTPException(status_code=code, detail=str(exc)) from exc


@router.post("/runs/{run_id}/approve")
async def ml_run_approve(request: Request, run_id: str) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.approve_run(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/{run_id}/apply-recommended-gates")
async def ml_run_apply_recommended_gates(request: Request, run_id: str) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.apply_recommended_gates(run_id)
    except ValueError as exc:
        code = 404 if "not found" in str(exc).lower() else 400
        raise HTTPException(status_code=code, detail=str(exc)) from exc


@router.post("/backfill/start")
async def ml_backfill_start(request: Request, payload: RunStartPayload) -> dict[str, Any]:
    _require_control(request, "backfill_enabled", "Backfill")
    manager = _manager(request)
    try:
        if payload.instance_id:
            return await manager.start_backfill_by_instance(payload.instance_id)
        if not payload.pair_symbol:
            raise ValueError("pair_symbol or instance_id is required.")
        return await manager.start_backfill(payload.pair_symbol)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/backfill/pause")
async def ml_backfill_pause(request: Request, payload: BackfillControlPayload) -> dict[str, Any]:
    _require_control(request, "backfill_enabled", "Backfill")
    manager = _manager(request)
    try:
        return await manager.pause_backfill(payload.job_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/backfill/stop")
async def ml_backfill_stop(request: Request, payload: BackfillControlPayload) -> dict[str, Any]:
    _require_control(request, "backfill_enabled", "Backfill")
    manager = _manager(request)
    try:
        return await manager.stop_backfill(payload.job_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/backfill/clear")
async def ml_backfill_clear(request: Request, payload: RunStartPayload) -> dict[str, Any]:
    _require_control(request, "backfill_enabled", "Backfill")
    manager = _manager(request)
    try:
        if payload.instance_id:
            return await manager.clear_historic_data_by_instance(payload.instance_id)
        return await manager.clear_historic_data(payload.pair_symbol)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/backfill/{job_id}")
async def ml_backfill_status(request: Request, job_id: str) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.backfill_status(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/backfill/{job_id}/logs")
async def ml_backfill_logs(request: Request, job_id: str) -> dict[str, Any]:
    manager = _manager(request)
    try:
        logs = await manager.backfill_logs(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"job_id": job_id, "logs": logs}


@router.post("/bot/{pair_symbol}/start")
async def ml_bot_start(request: Request, pair_symbol: str) -> dict[str, Any]:
    _require_control(request, "bots_enabled", "Bot")
    manager = _manager(request)
    try:
        return await manager.start_bot(pair_symbol)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/bot/{pair_symbol}/stop")
async def ml_bot_stop(request: Request, pair_symbol: str) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.stop_bot(pair_symbol)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/bot/{pair_symbol}/pause")
async def ml_bot_pause(request: Request, pair_symbol: str) -> dict[str, Any]:
    _require_control(request, "bots_enabled", "Bot")
    manager = _manager(request)
    try:
        return await manager.pause_bot(pair_symbol)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/bot/{pair_symbol}/status")
async def ml_bot_status(request: Request, pair_symbol: str) -> dict[str, Any]:
    manager = _manager(request)
    return await manager.bot_status(pair_symbol)


@router.get("/bot/{pair_symbol}/trades")
async def ml_bot_trades(
    request: Request,
    pair_symbol: str,
    limit: int = Query(default=200, ge=1, le=5000),
    session_id: str = Query(default=""),
    session_scope: str = Query(default="selected"),
) -> dict[str, Any]:
    manager = _manager(request)
    return await manager.bot_trades(
        pair_symbol,
        limit=limit,
        session_id=session_id,
        session_scope=session_scope,
    )


@router.get("/instances")
async def ml_instances(request: Request, lite: bool = Query(default=False)) -> dict[str, Any]:
    manager = _manager(request)
    return {"instances": await manager.list_instances(lite=bool(lite))}


@router.post("/instances")
async def ml_instance_create(request: Request, payload: InstanceCreatePayload) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.create_instance(payload.pair_symbol, payload.name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.patch("/instances/{instance_id}")
async def ml_instance_update(request: Request, instance_id: str, payload: InstanceUpdatePayload) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.update_instance(instance_id, name=payload.name, archived=payload.archived)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/instances/{instance_id}")
async def ml_instance_delete(request: Request, instance_id: str) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.archive_instance(instance_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/instances/{instance_id}/profile")
async def ml_instance_profile(request: Request, instance_id: str) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.get_profile_by_instance(instance_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/instances/{instance_id}/profile")
async def ml_instance_profile_save(request: Request, instance_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.save_profile_by_instance(instance_id, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/instances/{instance_id}/data-status")
async def ml_instance_data_status(
    request: Request,
    instance_id: str,
    dataset_source: str = Query(default="features_manifest"),
) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.data_status_by_instance_for_source(instance_id, dataset_source=dataset_source)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/instances/{instance_id}/backfill")
async def ml_instance_backfill(request: Request, instance_id: str) -> dict[str, Any]:
    manager = _manager(request)
    payload = await manager.latest_backfill_for_instance(instance_id)
    return {"instance_id": instance_id, "job": payload}


@router.get("/instances/{instance_id}/runs")
async def ml_instance_runs(request: Request, instance_id: str) -> dict[str, Any]:
    manager = _manager(request)
    runs = await manager.list_runs(instance_id=instance_id)
    return {"runs": runs}


@router.post("/instances/{instance_id}/runs/start")
async def ml_instance_run_start(
    request: Request,
    instance_id: str,
    dataset_source: str = Query(default="features_manifest"),
) -> dict[str, Any]:
    _require_control(request, "training_enabled", "Training")
    manager = _manager(request)
    try:
        source_mode = str(dataset_source or "features_manifest").strip().lower()
        if source_mode not in {"features_manifest", "local_market_events"}:
            source_mode = "features_manifest"
        return await manager.start_run_by_instance(instance_id, dataset_source=source_mode)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/instances/{instance_id}/bot/start")
async def ml_instance_bot_start(request: Request, instance_id: str) -> dict[str, Any]:
    _require_control(request, "bots_enabled", "Bot")
    manager = _manager(request)
    try:
        return await manager.start_bot_by_instance(instance_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/instances/{instance_id}/bot/stop")
async def ml_instance_bot_stop(request: Request, instance_id: str) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.stop_bot_by_instance(instance_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/instances/{instance_id}/bot/pause")
async def ml_instance_bot_pause(request: Request, instance_id: str) -> dict[str, Any]:
    _require_control(request, "bots_enabled", "Bot")
    manager = _manager(request)
    try:
        return await manager.pause_bot_by_instance(instance_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/instances/{instance_id}/bot/status")
async def ml_instance_bot_status(request: Request, instance_id: str) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.bot_status_by_instance(instance_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/instances/{instance_id}/bot/trades")
async def ml_instance_bot_trades(
    request: Request,
    instance_id: str,
    limit: int = Query(default=200, ge=1, le=5000),
    session_id: str = Query(default=""),
    session_scope: str = Query(default="selected"),
) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.bot_trades_by_instance(
            instance_id,
            limit=limit,
            session_id=session_id,
            session_scope=session_scope,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/instances/{instance_id}/bot/trade-map-candles")
async def ml_instance_bot_trade_map_candles(
    request: Request,
    instance_id: str,
    limit: int = Query(default=120, ge=30, le=5000),
    for_replay: bool = Query(default=False),
) -> dict[str, Any]:
    manager = _manager(request)
    try:
        profile = await manager.get_profile_by_instance(instance_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    pair_symbol = str(profile.get("pair_symbol", "")).upper()
    selected_exchanges = [
        str(item).lower()
        for item in (profile.get("bot_execution_selected_exchanges") or profile.get("selected_exchanges") or [])
        if str(item).strip()
    ]
    if (not for_replay) and selected_exchanges and "binance" not in selected_exchanges:
        return {
            "instance_id": instance_id,
            "pair_symbol": pair_symbol,
            "source": "binance_unselected",
            "candles": [],
            "reason": "binance_not_selected_in_profile",
        }
    rest_client = getattr(request.app.state, "rest_client", None)
    if rest_client is None:
        raise HTTPException(status_code=503, detail="REST client is not available.")
    try:
        rows = await rest_client.klines(pair_symbol, interval="1m", limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch candles: {exc}") from exc
    candles = _parse_klines_rows(rows)
    since_ts_ms = int(candles[0]["ts_ms"]) if candles else 0
    inference_points = await manager._repository.list_bot_inference_points(
        instance_id=instance_id,
        since_ts_ms=since_ts_ms,
        limit=max(120, int(limit) * 6),
    )
    return {
        "instance_id": instance_id,
        "pair_symbol": pair_symbol,
        "source": "binance_fapi_klines",
        "candles": candles,
        "inference_points": inference_points,
    }


@router.get("/instances/{instance_id}/bot/sessions")
async def ml_instance_bot_sessions(request: Request, instance_id: str) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.bot_sessions_by_instance(instance_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/instances/{instance_id}/bot/sessions")
async def ml_instance_bot_session_create(
    request: Request,
    instance_id: str,
    payload: BotSessionCreatePayload,
) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.create_bot_session_by_instance(instance_id, payload.name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.patch("/instances/{instance_id}/bot/sessions/{session_id}")
async def ml_instance_bot_session_update(
    request: Request,
    instance_id: str,
    session_id: str,
    payload: BotSessionUpdatePayload,
) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.update_bot_session_by_instance(
            instance_id,
            session_id,
            name=payload.name,
            set_active=payload.set_active,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/instances/{instance_id}/bot/sessions/{session_id}")
async def ml_instance_bot_session_delete(
    request: Request,
    instance_id: str,
    session_id: str,
) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.delete_bot_session_by_instance(instance_id, session_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/instances/{instance_id}/bot/sessions/{session_id}/clear")
async def ml_instance_bot_session_clear(
    request: Request,
    instance_id: str,
    session_id: str,
) -> dict[str, Any]:
    manager = _manager(request)
    try:
        return await manager.clear_bot_session_history_by_instance(instance_id, session_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/replay/start")
async def ml_replay_start(request: Request, payload: ReplayStartPayload) -> dict[str, Any]:
    _require_control(request, "replay_enabled", "Replay")
    manager = _manager(request)
    try:
        return await manager.replay_start(
            run_id=payload.run_id,
            source_mode=payload.source_mode,
            mode=payload.mode,
            speed=payload.speed,
            epoch_index=int(payload.epoch_index or 0),
            split=payload.split,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/replay/{replay_id}/step")
async def ml_replay_step(request: Request, replay_id: str) -> dict[str, Any]:
    _require_control(request, "replay_enabled", "Replay")
    manager = _manager(request)
    try:
        return await manager.replay_step(replay_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/replay/{replay_id}/events")
async def ml_replay_events(
    request: Request,
    replay_id: str,
    since: int = Query(default=0, ge=0),
    limit: int = Query(default=200, ge=1, le=2000),
) -> dict[str, Any]:
    _require_control(request, "replay_enabled", "Replay")
    manager = _manager(request)
    try:
        return await manager.replay_events(replay_id, since=since, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/replay/{replay_id}/parity-stats")
async def ml_replay_parity_stats(request: Request, replay_id: str) -> dict[str, Any]:
    _require_control(request, "replay_enabled", "Replay")
    manager = _manager(request)
    try:
        return await manager.replay_parity_stats(replay_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/replay/{replay_id}/control")
async def ml_replay_control(request: Request, replay_id: str, payload: ReplayControlPayload) -> dict[str, Any]:
    _require_control(request, "replay_enabled", "Replay")
    manager = _manager(request)
    try:
        return await manager.replay_control(
            replay_id,
            action=payload.action,
            cursor=payload.cursor,
            speed=payload.speed,
            source_mode=payload.source_mode,
            epoch_index=payload.epoch_index,
            split=payload.split,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/replay/candles")
async def ml_replay_candles(
    request: Request,
    pair_symbol: str = Query(min_length=3, max_length=20),
    limit: int = Query(default=240, ge=30, le=5000),
) -> dict[str, Any]:
    _require_control(request, "replay_enabled", "Replay")
    rest_client = getattr(request.app.state, "rest_client", None)
    if rest_client is None:
        raise HTTPException(status_code=503, detail="REST client is not available.")
    try:
        rows = await rest_client.klines(str(pair_symbol).upper(), interval="1m", limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to load replay candles: {exc}") from exc
    candles = _parse_klines_rows(rows)
    return {
        "pair_symbol": str(pair_symbol).upper(),
        "source": "replay_pair_fallback",
        "candles": candles,
    }


@router.get("/storage/overview")
async def ml_storage_overview(request: Request) -> dict[str, Any]:
    service = StorageManagementService(request.app.state.settings)
    return service.overview()


@router.post("/storage/delete")
async def ml_storage_delete(request: Request, payload: StorageDeletePayload) -> dict[str, Any]:
    if not payload.targets:
        raise HTTPException(status_code=400, detail="No delete targets provided.")
    service = StorageManagementService(request.app.state.settings)
    try:
        return service.delete_targets(payload.targets)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
