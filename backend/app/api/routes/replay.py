from pathlib import Path

from fastapi import APIRouter, Request

router = APIRouter(prefix="/replay", tags=["replay"])


@router.get("/files")
async def replay_files(request: Request) -> dict:
    replay_dir: Path = request.app.state.settings.replay_dir
    replay_dir.mkdir(parents=True, exist_ok=True)
    files = sorted([p.name for p in replay_dir.glob("*.jsonl")])
    return {"files": files}
