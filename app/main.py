"""FastAPI entry point: REST API (with Swagger UI) + in-process UDP DNS."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from .config import settings
from .dns_server import dns_service
from .models import (
    PublishResult,
    ReclaimResult,
    RecordOut,
    VersionOut,
    ZoneOut,
    ZonePayload,
)
from .storage import storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("main")


def _zone_out() -> ZoneOut:
    serial, records, published_at = storage.get_active_version()
    return ZoneOut(
        zone=settings.dns_zone,
        serial=serial,
        records=[RecordOut(**r) for r in records],
        published_at=published_at,
    )


def _version_out() -> VersionOut:
    serial, records, published_at = storage.get_active_version()
    sync_state = storage.get_sync_state()
    return VersionOut(
        role=settings.role,
        zone=settings.dns_zone,
        serial=serial,
        record_count=len(records),
        published_at=published_at,
        last_sync_at=sync_state["last_sync_at"],
        last_sync_status=sync_state["last_sync_status"],
        last_sync_error=sync_state["last_sync_error"] or None,
        upstream_serial=sync_state["upstream_serial"],
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info(
        "starting role=%s zone=%s db=%s dns_udp=%s api=%s",
        settings.role,
        settings.dns_zone,
        settings.db_path,
        settings.dns_port,
        settings.api_port,
    )
    dns_service.start()
    if not settings.is_primary:
        from .sync import sync_loop

        await sync_loop.start()
    try:
        yield
    finally:
        if not settings.is_primary:
            from .sync import sync_loop

            await sync_loop.stop()
        dns_service.stop()


app = FastAPI(
    title="Primary/Backup DNS for fixed intranet zone",
    version="1.0.0",
    description=(
        "Primary accepts a complete zone (up to 8 A records); the backup "
        "pulls whole versions over HTTP and answers A queries over UDP. "
        "Every answer uses one immutable version."
    ),
    lifespan=lifespan,
)


@app.get("/health", tags=["ops"])
def health() -> dict:
    return {"status": "ok", "role": settings.role, "zone": settings.dns_zone}


@app.get("/api/version", response_model=VersionOut, tags=["zone"])
def get_version() -> VersionOut:
    return _version_out()


@app.get("/api/zone", response_model=ZoneOut, tags=["zone"])
def get_zone() -> ZoneOut:
    return _zone_out()


# ---------------------------------------------------------------- primary
@app.post(
    "/api/zone",
    response_model=PublishResult,
    status_code=201,
    tags=["primary"],
)
def publish_zone(payload: ZonePayload) -> PublishResult:
    """Replace the whole zone. The serial increments only on success."""
    if not settings.is_primary:
        raise HTTPException(
            status_code=403,
            detail="zone publishing is only allowed on the primary",
        )
    records = [r.model_dump() for r in payload.sorted_records()]
    serial, published_at = storage.next_publish(records)
    log.info("published new version serial=%s records=%s",
             serial, len(records))
    return PublishResult(
        ok=True,
        serial=serial,
        record_count=len(records),
        published_at=published_at,
    )


# ----------------------------------------------------------------- backup
@app.post("/api/sync", tags=["backup"])
async def manual_sync() -> dict:
    """Pull the complete current zone from the primary once."""
    if settings.is_primary:
        raise HTTPException(
            status_code=403,
            detail="sync is only available on the backup",
        )
    from .sync import sync_loop

    outcome = await sync_loop.trigger()
    result = outcome.as_dict()
    result["zone"] = _zone_out().model_dump()
    return result


# ------------------------------------------------- version reclaim (both)
@app.get("/api/versions", tags=["reclaim"])
def list_versions() -> dict:
    """Serials of every complete version actually stored on this machine."""
    return {
        "role": settings.role,
        "zone": settings.dns_zone,
        "active_serial": storage.active_serial(),
        "serials": storage.list_serials(),
    }


@app.post("/api/reclaim", response_model=ReclaimResult, tags=["reclaim"])
def reclaim_versions() -> ReclaimResult:
    """Delete old versions, keeping the active one and the latest old one.

    Available on both roles. Only versions present in this machine's own
    database are considered, so a backup with gap/skip-number sync keeps
    the newest old version it actually stored. The active zone is never
    deleted and publishing/sync never depend on this call; a failure is
    reported as 500, nothing is removed and the request can be retried.
    """
    try:
        deleted = storage.reclaim_versions()
    except Exception as exc:  # storage rolls back; active zone untouched
        log.exception("version reclaim failed")
        raise HTTPException(
            status_code=500,
            detail=f"version reclaim failed: {type(exc).__name__}: {exc}",
        )
    active = storage.active_serial()
    retained = storage.list_serials()
    log.info(
        "reclaimed old versions deleted=%s retained=%s active=%s",
        deleted, retained, active,
    )
    return ReclaimResult(
        ok=True,
        role=settings.role,
        zone=settings.dns_zone,
        active_serial=active,
        retained_serials=retained,
        deleted_serials=deleted,
        deleted_count=len(deleted),
    )
