"""Battery monitoring for ECHO coordinator nodes.

Two modes:

* **Real** — reads battery voltage via an ADS1115 ADC on the Raspberry Pi
  I2C bus and maps it to a 0–100 % level (assumes a single-cell LiPo:
  3.0 V = 0 %, 4.2 V = 100 %).
* **Mock** — simulates a linearly-draining battery so the protocol can be
  demoed on any laptop without GPIO hardware.
"""

from __future__ import annotations

import logging
import threading
import time

from rpi.config import MOCK_BATTERY_DRAIN_RATE

logger = logging.getLogger(__name__)


class BatteryMonitor:
    """Read (or simulate) the current battery level."""

    def __init__(
        self,
        mock: bool = True,
        initial_level: float = 100.0,
        drain_rate: float = MOCK_BATTERY_DRAIN_RATE,
    ) -> None:
        self._mock = mock
        self._level = initial_level
        self._drain_rate = drain_rate  # % per second in mock mode
        self._mock_drain_paused = False
        self._last_read = time.monotonic()
        self._adc = None
        self._lock = threading.Lock()

        if not mock:
            self._init_hardware()

    # ---------------------------------------------------------- hardware init

    def _init_hardware(self) -> None:
        try:
            import board                         # type: ignore[import-untyped]
            import busio                         # type: ignore[import-untyped]
            import adafruit_ads1x15.ads1115 as ADS  # type: ignore[import-untyped]
            from adafruit_ads1x15.analog_in import AnalogIn  # type: ignore[import-untyped]

            i2c = busio.I2C(board.SCL, board.SDA)
            ads = ADS.ADS1115(i2c)
            self._adc = AnalogIn(ads, ADS.P0)
            logger.info("ADS1115 ADC initialised for battery monitoring")
        except Exception:
            logger.warning("GPIO / ADC unavailable — falling back to mock battery")
            self._mock = True

    # --------------------------------------------------------------- reading

    def read_level(self) -> int:
        """Return the current battery level as an integer 0–100."""
        if self._mock:
            return self._read_mock()
        return self._read_real()

    def _read_mock(self) -> int:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_read
            self._last_read = now
            if not self._mock_drain_paused:
                self._level = max(0.0, self._level - self._drain_rate * elapsed)
            return int(self._level)

    def _read_real(self) -> int:
        if self._adc is None:
            return 0
        voltage: float = self._adc.voltage
        pct = (voltage - 3.0) / (4.2 - 3.0) * 100.0
        return max(0, min(100, int(pct)))

    # --------------------------------------------------------------- helpers

    @property
    def is_mock(self) -> bool:
        return self._mock

    # ----------------------------------------------------------- demo (mock)

    def set_mock_level(self, level: float) -> None:
        """Set simulated battery 0–100 (mock mode only). Thread-safe."""
        if not self._mock:
            return
        clamped = max(0.0, min(100.0, float(level)))
        with self._lock:
            self._level = clamped
            self._last_read = time.monotonic()

    def set_mock_drain_rate(self, rate: float) -> None:
        """Set mock drain rate (% per second); non-negative. Mock mode only."""
        if not self._mock:
            return
        with self._lock:
            self._drain_rate = max(0.0, float(rate))
            self._last_read = time.monotonic()

    def set_mock_drain_paused(self, paused: bool) -> None:
        """Pause/resume linear drain in mock mode (level still readable)."""
        if not self._mock:
            return
        with self._lock:
            self._mock_drain_paused = bool(paused)
            self._last_read = time.monotonic()
