"""Unit tests for version reclaim and read/reclaim interleaving safety."""
import importlib
import threading

import pytest


def rec(name, ip):
    return {"name": name, "ip": ip}


@pytest.fixture()
def modules(tmp_path, monkeypatch):
    """Reload config/storage/models/main against a fresh temp database."""
    db = str(tmp_path / "reclaim.db")
    monkeypatch.setenv("DB_PATH", db)
    monkeypatch.setenv("DNS_ZONE", "internal.local")
    monkeypatch.setenv("MAX_RECORDS", "8")
    monkeypatch.setenv("DNS_ROLE", "primary")

    from app import config, models, storage as storage_mod, main

    importlib.reload(config)
    importlib.reload(models)
    importlib.reload(storage_mod)
    importlib.reload(main)
    yield storage_mod, main
    storage_mod.storage._connect().close()


# ------------------------------------------------------------- retention
def test_reclaim_keeps_active_and_latest_old(modules):
    storage_mod, _ = modules
    s = storage_mod.storage
    for i in range(1, 5):
        s.next_publish([rec("db.internal.local", f"10.0.0.{i}")])
    assert s.list_serials() == [0, 1, 2, 3, 4]

    deleted = s.reclaim_versions()
    assert deleted == [0, 1, 2]
    assert s.list_serials() == [3, 4]

    # Active version is untouched and still fully readable.
    serial, records, _ = s.get_active_version()
    assert serial == 4 and records == [rec("db.internal.local", "10.0.0.4")]
    assert s.version_exists(3) and not s.version_exists(2)


def test_reclaim_is_idempotent_and_retryable(modules):
    storage_mod, _ = modules
    s = storage_mod.storage
    for i in range(1, 4):
        s.next_publish([rec("db.internal.local", f"10.0.0.{i}")])
    first = s.reclaim_versions()
    assert first == [0, 1]
    second = s.reclaim_versions()
    assert second == []
    assert s.list_serials() == [2, 3]


def test_reclaim_with_nothing_to_delete(modules):
    storage_mod, _ = modules
    s = storage_mod.storage
    # Seed-only database: the empty version is both active and sole row.
    assert s.reclaim_versions() == []
    assert s.list_serials() == [0]
    # One published version: seed 0 stays as the latest (and only) old one.
    s.next_publish([rec("db.internal.local", "10.0.0.1")])
    assert s.reclaim_versions() == []
    assert sorted(s.list_serials()) == [0, 1]
    assert s.active_serial() == 1


def test_reclaim_gap_sync_keeps_newest_stored_old_version(modules):
    """Backup skip-number syncing: retention uses stored serials only."""
    storage_mod, _ = modules
    s = storage_mod.storage
    # Simulate a backup that only ever stored serials 0, 1, 3, 6.
    assert s.commit_version(1, [rec("a.internal.local", "10.0.0.1")], "t1")
    assert s.commit_version(3, [rec("a.internal.local", "10.0.0.3")], "t3")
    assert s.commit_version(6, [rec("a.internal.local", "10.0.0.6")], "t6")
    assert s.list_serials() == [0, 1, 3, 6]

    deleted = s.reclaim_versions()
    # Missing serials 2, 4, 5 are irrelevant: keep active 6 + newest old 3.
    assert deleted == [0, 1]
    assert s.list_serials() == [3, 6]
    serial, records, _ = s.get_active_version()
    assert serial == 6 and records == [rec("a.internal.local", "10.0.0.6")]


# ------------------------------------------------ failure handling
def test_reclaim_failure_rolls_back_and_can_be_retried(modules, monkeypatch):
    storage_mod, _ = modules
    s = storage_mod.storage
    for i in range(1, 4):
        s.next_publish([rec("db.internal.local", f"10.0.0.{i}")])
    before = s.list_serials()

    real_get_state = s._get_state

    def boom(conn, key):  # fail inside the reclaim transaction
        raise RuntimeError("injected storage fault")

    monkeypatch.setattr(s, "_get_state", boom)
    with pytest.raises(RuntimeError, match="injected storage fault"):
        s.reclaim_versions()
    monkeypatch.setattr(s, "_get_state", real_get_state)

    # Nothing was removed; the active zone kept serving and retry works.
    assert s.list_serials() == before
    assert s.active_serial() == 3
    assert s.reclaim_versions() == [0, 1]
    assert s.list_serials() == [2, 3]


# --------------------------------------------- interleaving concurrency
def test_concurrent_publish_reclaim_query_never_breaks_snapshot(modules):
    storage_mod, _ = modules
    s = storage_mod.storage
    stop = threading.Event()
    errors = []

    def publisher():
        try:
            for i in range(1, 41):
                s.next_publish([rec("db.internal.local", f"10.0.{i // 256}.{i % 256}")])
        except Exception as exc:  # pragma: no cover - failure signal
            errors.append(("publish", exc))
        finally:
            stop.set()

    def reclaimer():
        try:
            while not stop.is_set():
                s.reclaim_versions()
        except Exception as exc:  # pragma: no cover - failure signal
            errors.append(("reclaim", exc))

    def reader():
        try:
            for _ in range(800):
                serial, records, _ = s.get_active_version()
                if serial >= 1:
                    # A broken (two-statement) read could hand back an empty
                    # record list for a serial whose row was just deleted.
                    assert records, f"false NXDOMAIN snapshot for serial {serial}"
                    assert records == [
                        rec(
                            "db.internal.local",
                            f"10.0.{serial // 256}.{serial % 256}",
                        )
                    ], f"mixed-version snapshot for serial {serial}"
        except Exception as exc:  # pragma: no cover - failure signal
            errors.append(("reader", exc))

    threads = [
        threading.Thread(target=publisher),
        threading.Thread(target=reclaimer),
        threading.Thread(target=reader),
        threading.Thread(target=reader),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    # Reclaim never blocked publishing: all 40 versions were serialised.
    assert s.active_serial() == 40


# ------------------------------------------------------------- HTTP API
def test_reclaim_endpoint_primary_and_backup(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    for role in ("primary", "backup"):
        db = str(tmp_path / f"{role}.db")
        monkeypatch.setenv("DB_PATH", db)
        monkeypatch.setenv("DNS_ROLE", role)
        monkeypatch.setenv("DNS_ZONE", "internal.local")
        from app import config, models, storage as storage_mod, main

        importlib.reload(config)
        importlib.reload(models)
        importlib.reload(storage_mod)
        importlib.reload(main)
        s = storage_mod.storage
        for i in range(1, 4):
            s.next_publish([rec("db.internal.local", f"10.0.0.{i}")])

        client = TestClient(main.app)
        r = client.get("/api/versions")
        assert r.status_code == 200
        assert r.json()["serials"] == [0, 1, 2, 3]
        assert r.json()["role"] == role

        r = client.post("/api/reclaim")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["ok"] is True
        assert body["role"] == role
        assert body["deleted_serials"] == [0, 1]
        assert body["retained_serials"] == [2, 3]
        assert body["active_serial"] == 3
        assert body["deleted_count"] == 2

        # Reclaim is available to the backup too (no role-based 403).
        r = client.post("/api/reclaim")
        assert r.status_code == 200 and r.json()["deleted_serials"] == []


def test_reclaim_endpoint_reports_500_and_keeps_zone(modules, monkeypatch):
    from fastapi.testclient import TestClient

    storage_mod, main = modules
    s = storage_mod.storage
    for i in range(1, 4):
        s.next_publish([rec("db.internal.local", f"10.0.0.{i}")])

    def boom():
        raise RuntimeError("injected reclaim fault")

    monkeypatch.setattr(s, "reclaim_versions", boom)
    client = TestClient(main.app)
    r = client.post("/api/reclaim")
    assert r.status_code == 500
    assert "injected reclaim fault" in r.json()["detail"]
    monkeypatch.undo()

    # Active zone still served, nothing deleted, retry succeeds.
    assert s.list_serials() == [0, 1, 2, 3]
    assert s.active_serial() == 3
    r = client.post("/api/reclaim")
    assert r.status_code == 200 and r.json()["deleted_serials"] == [0, 1]
