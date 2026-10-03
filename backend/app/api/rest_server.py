from __future__ import annotations

from typing import Any, Callable

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.health import router as health_router
from app.api.routes.ml import router as ml_router
from app.api.routes.mobile import router as mobile_router
from app.api.routes.positions import router as positions_router
from app.api.routes.replay import router as replay_router
from app.api.routes.settings import router as settings_router
from app.api.routes.terminal import router as terminal_router
from app.api.routes.trading import router as trading_router
from app.api.ws_server import BroadcastHub
from app.config import Settings



def create_app(
    settings: Settings,
    hub: BroadcastHub,
    lifespan: Callable[[FastAPI], Any] | None = None,
) -> FastAPI:
    app = FastAPI(title="Futures Terminal Backend", version="0.1.0", lifespan=lifespan)
    app.state.settings = settings
    app.state.hub = hub
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(health_router)
    app.include_router(settings_router)
    app.include_router(terminal_router)
    app.include_router(mobile_router)
    app.include_router(ml_router)
    app.include_router(trading_router)
    app.include_router(positions_router)
    app.include_router(replay_router)

    @app.websocket("/ws/terminal")
    async def ws_terminal(websocket: WebSocket) -> None:
        controls = getattr(app.state, "workload_controls", None) or {}
        if not bool(controls.get("live_streaming_enabled", True)):
            # Accept first so clients receive a clean close frame (avoid handshake 403 reconnect storms).
            try:
                await websocket.accept()
                await websocket.close(code=1013, reason="Live streaming disabled")
            except (WebSocketDisconnect, RuntimeError):
                # Client may disconnect before close frame is sent.
                pass
            return
        tab_id = websocket.query_params.get("tab_id", "default")
        initial_symbol = websocket.query_params.get("symbol", "")
        tab_sessions = getattr(app.state, "tab_sessions", None)
        if tab_sessions is not None:
            await tab_sessions.get_or_create(tab_id, initial_symbol=initial_symbol)
        await hub.connect(websocket, tab_id=tab_id)
        if tab_sessions is not None:
            await tab_sessions.set_tab_visibility(tab_id, True)
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            await hub.disconnect(websocket, tab_id=tab_id)
            if tab_sessions is not None:
                await tab_sessions.set_tab_visibility(tab_id, False)
        except Exception:
            await hub.disconnect(websocket, tab_id=tab_id)
            if tab_sessions is not None:
                await tab_sessions.set_tab_visibility(tab_id, False)

    return app
