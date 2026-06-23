"""Synthetic IoT telemetry publisher → HiveMQ.

Models a small fleet of devices spread across sites, each emitting
temperature / humidity / pressure / vibration on a configurable cadence.
Values walk smoothly from a per-device baseline so downstream anomaly
detection (iot-anomaly-ml) has a stable signal to learn against.
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

DEFAULT_SITES = ["site-london", "site-edinburgh", "site-leeds"]
DEVICES_PER_SITE = 3
FIRMWARE_VERSIONS = ["1.0.0", "1.1.0", "1.2.0", "2.0.0-beta"]


@dataclass
class Device:
    device_id: str
    site_id: str
    firmware_version: str = ""
    temp: float = field(default_factory=lambda: random.uniform(18.0, 24.0))
    humidity: float = field(default_factory=lambda: random.uniform(40.0, 60.0))
    pressure: float = field(default_factory=lambda: random.uniform(1005.0, 1020.0))
    vibration: float = field(default_factory=lambda: random.uniform(0.05, 0.15))

    def step(self) -> dict:
        self.temp += random.gauss(0.0, 0.15)
        self.humidity = max(0.0, min(100.0, self.humidity + random.gauss(0.0, 0.4)))
        self.pressure += random.gauss(0.0, 0.2)
        self.vibration = max(0.0, self.vibration + random.gauss(0.0, 0.01))
        reading = {
            "device_id": self.device_id,
            "site_id": self.site_id,
            "ts": datetime.now(timezone.utc).isoformat(),
            "temperature_c": round(self.temp, 3),
            "humidity_pct": round(self.humidity, 3),
            "pressure_hpa": round(self.pressure, 3),
            "vibration_g": round(self.vibration, 4),
        }
        if self.firmware_version:
            reading["firmware_version"] = self.firmware_version
        return reading


def build_fleet(sites: list[str], per_site: int, with_firmware: bool = False) -> list[Device]:
    fleet = []
    for site in sites:
        for i in range(per_site):
            fw = random.choice(FIRMWARE_VERSIONS) if with_firmware else ""
            fleet.append(Device(
                device_id=f"{site}-dev-{i:02d}", site_id=site,
                firmware_version=fw,
            ))
    return fleet


def topic_for(device: Device) -> str:
    return f"sensors/{device.site_id}/{device.device_id}/telemetry"


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=1883)
    p.add_argument("--rate-hz", type=float, default=2.0, help="readings per device per second")
    p.add_argument("--devices-per-site", type=int, default=DEVICES_PER_SITE)
    p.add_argument("--with-firmware", action="store_true",
                   help="Include firmware_version field (schema evolution demo)")
    args = p.parse_args()

    fleet = build_fleet(DEFAULT_SITES, args.devices_per_site, args.with_firmware)
    period = 1.0 / args.rate_hz

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="iot-sim")
    client.connect(args.host, args.port, keepalive=30)
    client.loop_start()

    print(f"[sim] {len(fleet)} devices @ {args.rate_hz} Hz → mqtt://{args.host}:{args.port}", flush=True)

    stop = False
    def _shutdown(*_):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    sent = 0
    t_start = time.time()
    while not stop:
        for dev in fleet:
            payload = dev.step()
            client.publish(topic_for(dev), json.dumps(payload), qos=0)
            sent += 1
        if sent % 200 == 0:
            elapsed = time.time() - t_start
            print(f"[sim] {sent} msgs ({sent / elapsed:.1f}/s)", flush=True)
        time.sleep(period)

    client.loop_stop()
    client.disconnect()
    print(f"[sim] stopped after {sent} msgs", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
