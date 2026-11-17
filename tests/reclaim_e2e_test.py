"""End-to-end reclaim test: real primary + backup uvicorn + UDP DNS.

Run with the project venv:  .venv/bin/python -I tests/reclaim_e2e_test.py

Scenarios:
  1. backup restart from an empty DB does gap/skip-number sync straight to
     the newest serial; reclaim then retains only the versions it actually
     stored (active + newest old), missing serials are irrelevant
  2. reclaim on both roles returns the deleted serials and keeps serving
  3. reclaim is idempotent; a second call deletes nothing
  4. publish/sync never depend on reclaim: a new version still publishes
     and syncs after reclaim, and reclaim keeps only the newest two
  5. heavy interleaving: concurrent publish + reclaim on the primary while
     UDP queries run — every answer is NOERROR with the exact record of
     one complete version (no false NXDOMAIN, no mixed/new address)
"""
from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import httpx
from dnslib import DNSRecord, RCODE

ROOT = Path(__file__).resolve().parent.parent
PY = str(ROOT / ".venv" / "bin" / "python")
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)

ZONE = "internal.local"
PRIMARY_API = "http://127.0.0.1:18020"
BACKUP_API = "http://127.0.0.1:18021"
PRIMARY_DNS = ("127.0.0.1", 15059)
BACKUP_DNS = ("127.0.0.1", 15060)

failures = []


def check(name, cond, detail=""):
    print(("  PASS  " if cond else "  FAIL  ") + name +
          (" " + detail if not cond else ""))
    if not cond:
        failures.append(name)


def wait_http(url, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=1.5).status_code == 200:
                return
        except Exception:
            pass
        time.sleep(0.3)
    raise RuntimeError(url)


def wait_serial(api, serial, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(f"{api}/api/version", timeout=2).json()["serial"] == serial:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def dns_query(server, name, timeout=3.0):
    q = DNSRecord.question(name, "A")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    sock.sendto(q.pack(), server)
    data, _ = sock.recvfrom(65535)
    sock.close()
    return DNSRecord.parse(data)


def dns_ips(server, name):
    reply = dns_query(server, name)
    return reply.header.rcode, [str(rr.rdata) for rr in reply.rr]


def start(role, api_port, dns_port, db, upstream):
    env = os.environ.copy()
    env.update({
        "DNS_ROLE": role, "DNS_ZONE": ZONE, "DB_PATH": db,
        "DNS_HOST": "127.0.0.1", "DNS_PORT": str(dns_port),
        "API_HOST": "127.0.0.1", "API_PORT": str(api_port),
        "SYNC_INTERVAL": "1", "HTTP_TIMEOUT": "5", "PYTHONUNBUFFERED": "1",
    })
    if upstream:
        env["PRIMARY_API_URL"] = upstream
    log = open(DATA / f"{role}-reclaim.log", "w")
    return subprocess.Popen(
        [PY, "-I", "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(api_port)],
        cwd=str(ROOT), env=env, stdout=log, stderr=subprocess.STDOUT)


def stop(p):
    p.send_signal(signal.SIGTERM)
    try:
        p.wait(timeout=10)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()


def publish(ip):
    return httpx.post(
        f"{PRIMARY_API}/api/zone",
        json={"records": [{"name": f"db.{ZONE}", "ip": ip}]},
        timeout=5,
    )


def reclaim(api):
    return httpx.post(f"{api}/api/reclaim", timeout=8)


def main():
    for f in ("primary-reclaim.db", "backup-reclaim.db"):
        for suffix in ("", "-wal", "-shm"):
            p = DATA / (f + suffix)
            if p.exists():
                p.unlink()

    primary = start("primary", 18020, 15059,
                    str(DATA / "primary-reclaim.db"), None)
    backup = start("backup", 18021, 15060,
                   str(DATA / "backup-reclaim.db"), PRIMARY_API)
    try:
        wait_http(f"{PRIMARY_API}/health")
        wait_http(f"{BACKUP_API}/health")

        print("[1] build versions, then gap-sync the backup from an empty DB")
        for i in range(1, 4):
            r = publish(f"10.0.0.{i}")
            assert r.status_code == 201, r.text
        check("primary at serial 3",
              httpx.get(f"{PRIMARY_API}/api/version").json()["serial"] == 3)

        # Wipe the backup while the primary keeps advancing: when it comes
        # back it can only store whatever it pulls now (skipping 1..4).
        stop(backup)
        for suffix in ("", "-wal", "-shm"):
            p = DATA / ("backup-reclaim.db" + suffix)
            if p.exists():
                p.unlink()
        publish("10.0.0.4")
        publish("10.0.0.5")
        check("primary advanced to serial 5",
              httpx.get(f"{PRIMARY_API}/api/version").json()["serial"] == 5)

        backup = start("backup", 18021, 15060,
                       str(DATA / "backup-reclaim.db"), PRIMARY_API)
        wait_http(f"{BACKUP_API}/health")
        check("backup gap-synced straight to serial 5 (no 1..4 stored)",
              wait_serial(BACKUP_API, 5))
        versions = httpx.get(f"{BACKUP_API}/api/versions", timeout=3).json()
        check("backup only stored seed 0 and serial 5",
              versions["serials"] == [0, 5], str(versions))

        print("[2] reclaim on the gap-synced backup keeps active + newest old")
        r = reclaim(BACKUP_API)
        # Only 0 and 5 are stored: serial 0 IS the newest (and only) old
        # version present on this machine, so nothing can be deleted yet.
        check("backup reclaim keeps its only old version (seed serial 0)",
              r.status_code == 200 and r.json()["deleted_serials"] == []
              and r.json()["retained_serials"] == [0, 5], r.text)
        rcode, ips = dns_ips(BACKUP_DNS, f"db.{ZONE}")
        check("backup still answers complete v5 record after reclaim",
              rcode == RCODE.NOERROR and ips == ["10.0.0.5"], str((rcode, ips)))

        print("[3] reclaim is idempotent")
        r = reclaim(BACKUP_API)
        check("second backup reclaim also deletes nothing",
              r.status_code == 200 and r.json()["deleted_serials"] == []
              and r.json()["retained_serials"] == [0, 5], r.text)

        print("[4] primary reclaim returns deleted serials, keeps newest two")
        versions = httpx.get(f"{PRIMARY_API}/api/versions", timeout=3).json()
        check("primary holds 0..5 before reclaim",
              versions["serials"] == [0, 1, 2, 3, 4, 5], str(versions))
        r = reclaim(PRIMARY_API)
        body = r.json()
        check("primary reclaim deleted 0..3 and retained 4,5",
              r.status_code == 200
              and body["deleted_serials"] == [0, 1, 2, 3]
              and body["retained_serials"] == [4, 5]
              and body["active_serial"] == 5, r.text)
        rcode, ips = dns_ips(PRIMARY_DNS, f"db.{ZONE}")
        check("primary UDP unchanged after reclaim (still v5 record)",
              rcode == RCODE.NOERROR and ips == ["10.0.0.5"], str((rcode, ips)))

        print("[5] publish and sync do not depend on reclaim")
        r = publish("10.0.0.6")
        check("publish after reclaim still succeeds (serial 6)",
              r.status_code == 201 and r.json()["serial"] == 6, r.text)
        check("backup still auto-syncs after its reclaim",
              wait_serial(BACKUP_API, 6))
        r = reclaim(PRIMARY_API)
        check("reclaim again keeps only active 6 + newest old 5",
              r.status_code == 200 and r.json()["retained_serials"] == [5, 6]
              and r.json()["deleted_serials"] == [4], r.text)
        r = reclaim(BACKUP_API)
        # Serial 0 was the backup's newest old version until serial 5 became
        # the retained old one once serial 6 arrived; now 0 is finally gone.
        check("backup reclaim keeps its two stored versions 5,6, drops seed 0",
              r.status_code == 200 and r.json()["retained_serials"] == [5, 6]
              and r.json()["deleted_serials"] == [0], r.text)
        rcode, ips = dns_ips(BACKUP_DNS, f"db.{ZONE}")
        check("backup answers v6", rcode == RCODE.NOERROR
              and ips == ["10.0.0.6"], str((rcode, ips)))

        print("[6] interleaved publish + reclaim + UDP queries")
        stop_event = threading.Event()
        race_errors = []

        def publisher():
            n = 7
            while not stop_event.is_set() and n <= 26:
                ip = f"10.1.0.{n}"
                r = publish(ip)
                if r.status_code != 201:
                    race_errors.append(f"publish {n} -> {r.status_code} {r.text}")
                    break
                n += 1
                time.sleep(0.05)

        def reclaimer():
            while not stop_event.is_set():
                try:
                    r = reclaim(PRIMARY_API)
                    if r.status_code != 200:
                        race_errors.append(f"reclaim -> {r.status_code} {r.text}")
                except Exception as exc:  # pragma: no cover
                    race_errors.append(f"reclaim exc: {exc}")
                time.sleep(0.02)

        probe_count = {"n": 0}
        # Every published version maps serial s (1..26) to one exact IP;
        # the answer must always be the complete record of some single
        # snapshot that was active when the query was served.
        valid_ips = {f"10.0.0.{s}" for s in range(1, 7)} | \
            {f"10.1.0.{s}" for s in range(7, 27)}

        def querier():
            while not stop_event.is_set():
                try:
                    reply = dns_query(PRIMARY_DNS, f"db.{ZONE}", timeout=2)
                    probe_count["n"] += 1
                    if reply.header.rcode != RCODE.NOERROR:
                        race_errors.append(f"rcode={reply.header.rcode}")
                        continue
                    got = [str(rr.rdata) for rr in reply.rr]
                    # Exactly one A record, and it must belong to a real
                    # version: no false NXDOMAIN, no mixed/new address.
                    if len(got) != 1 or got[0] not in valid_ips:
                        race_errors.append(f"bad/mixed answer {got}")
                except socket.timeout:
                    pass
                except Exception as exc:  # pragma: no cover
                    race_errors.append(f"query exc: {exc}")
                time.sleep(0.005)

        threads = [
            threading.Thread(target=publisher),
            threading.Thread(target=reclaimer),
            threading.Thread(target=querier),
            threading.Thread(target=querier),
        ]
        for t in threads:
            t.start()
        # Let the storm run until all 20 publishes have been applied.
        deadline = time.time() + 30
        while time.time() < deadline and \
                httpx.get(f"{PRIMARY_API}/api/version", timeout=2).json()["serial"] < 26:
            time.sleep(0.2)
        stop_event.set()
        for t in threads:
            t.join()
        probes = probe_count["n"]
        check(f"all interleaved UDP answers are complete single-version "
              f"records ({probes} probes)", probes >= 20 and not race_errors,
              f"probes={probes} errors={race_errors[:5]}")
        check("primary reached serial 26; reclaim never blocked publishing",
              httpx.get(f"{PRIMARY_API}/api/version").json()["serial"] == 26)

        r = reclaim(PRIMARY_API)
        check("final reclaim leaves exactly two versions",
              r.status_code == 200
              and r.json()["retained_serials"] == [25, 26], r.text)
        rcode, ips = dns_ips(PRIMARY_DNS, f"db.{ZONE}")
        check("primary still serves v26 after final reclaim",
              rcode == RCODE.NOERROR and ips == ["10.1.0.26"], str((rcode, ips)))
        check("backup catches up to v26 on its own DB", wait_serial(BACKUP_API, 26))
        r = reclaim(BACKUP_API)
        check("backup final reclaim retains its newest two stored versions",
              r.status_code == 200 and len(r.json()["retained_serials"]) == 2
              and r.json()["active_serial"] == 26, r.text)
        rcode, ips = dns_ips(BACKUP_DNS, f"db.{ZONE}")
        check("backup still serves v26 over UDP",
              rcode == RCODE.NOERROR and ips == ["10.1.0.26"], str((rcode, ips)))
    finally:
        stop(primary)
        if backup.poll() is None:
            stop(backup)

    print(f"\n==== reclaim e2e: {len(failures)} failures ====")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
