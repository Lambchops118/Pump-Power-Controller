# Part of TALOS - Monkey Butler Device Operations System
#
# Quad pump firmware: Wi-Fi and MQTT supervision.
#
# Reconnection is a state machine driven from the main loop rather than a
# blocking retry loop, so relay deadlines keep being enforced while the network
# is down.
#
# The watchdog budget is the hard constraint here. On RP2040 the watchdog
# cannot be set beyond ~8.4 s, and main.py feeds it once per loop iteration, so
# every blocking call this module makes has to fit inside that. The 2026-09-01
# investigation found it did not: joining Wi-Fi (~4 s measured) and then
# connecting to an unreachable broker (a full socket timeout, 5 s measured)
# happened inside a single ensure_connected() call, ~9 s against an 8 s
# watchdog. The board reset mid-connect, before the failure was ever recorded
# as a backoff, and repeated that on every reboot - a permanent reset loop for
# as long as the broker stayed down.
#
# So the connect sequence is now staged across loop iterations:
#
#   IDLE  -- start the association (non-blocking) -->  WIFI
#   WIFI  -- poll isconnected() each tick, no blocking --> handshake
#   handshake: the only blocking step, bounded by the socket timeout, with a
#              watchdog feed on either side of it and between subscriptions.
#
# Never wait on this module before turning a relay off.

import qp_config as config

STAGE_IDLE = "idle"
STAGE_WIFI = "wifi"


class OutboundQueue(object):
    """Bounded outbound buffer.

    Important state and acknowledgement events survive a brief outage. When the
    bound is reached, non-critical traffic (receipts, run_stopped notes, legacy
    status) is dropped before anything critical, and the drop count is reported
    in health rather than hidden.
    """

    def __init__(self, maximum=None):
        self._maximum = maximum or config.OUTBOUND_QUEUE_MAX
        self._items = []
        self.dropped = 0

    def __len__(self):
        return len(self._items)

    def push(self, message):
        if len(self._items) >= self._maximum:
            self._evict()
        self._items.append(message)

    def extend(self, messages):
        for message in messages:
            self.push(message)

    def peek(self):
        return self._items[0] if self._items else None

    def pop(self):
        return self._items.pop(0)

    def _evict(self):
        for index, item in enumerate(self._items):
            if not item.critical:
                self._items.pop(index)
                self.dropped += 1
                return
        self._items.pop(0)
        self.dropped += 1


class NetworkSupervisor(object):
    def __init__(
        self,
        client_id,
        broker,
        on_message,
        clock,
        port=0,
        user=None,
        password=None,
        subscriptions=(),
        last_will=None,
        watchdog=None,
    ):
        self._client_id = client_id
        self._broker = broker
        self._port = port
        self._user = user
        self._password = password
        self._on_message = on_message
        self._clock = clock
        self._subscriptions = tuple(subscriptions)
        self._last_will = last_will
        self._watchdog = watchdog

        self._client = None
        self._wlan = None
        self._stage = STAGE_IDLE
        self._wifi_deadline_ms = None
        self._attempt = 0
        self._next_attempt_ms = None
        self._last_ping_ms = None
        self._last_rx_ms = None
        self.reconnects = 0
        self.radio_resets = 0
        self.consecutive_failures = 0
        self.last_error = None

    # --- status ---------------------------------------------------------------

    @property
    def connected(self):
        return self._client is not None

    def wifi_connected(self):
        try:
            return bool(self._wlan is not None and self._wlan.isconnected())
        except Exception:
            return False

    def rssi(self):
        try:
            return int(self._wlan.status("rssi"))
        except Exception:
            return None

    # --- watchdog -------------------------------------------------------------

    def _feed(self):
        """Feed the watchdog around a blocking step.

        The main loop feeds once per iteration; a connect or a QoS 1 publish can
        outlast an iteration on its own, so those paths feed for themselves. A
        missing or failing watchdog is never allowed to break the network path.
        """
        if self._watchdog is None:
            return
        try:
            self._watchdog.feed()
        except Exception:
            pass

    # --- connection -----------------------------------------------------------

    def ensure_connected(self):
        """Advance the connection state machine by at most one step.

        Returns True when an MQTT session is available. Each call performs at
        most one blocking operation, bounded by ``SOCKET_TIMEOUT_SECONDS``.
        """
        if self._client is not None:
            return True

        if self._stage == STAGE_IDLE:
            now = self._clock.ticks_ms()
            if self._next_attempt_ms is not None:
                if self._clock.ticks_diff(now, self._next_attempt_ms) < 0:
                    return False
            try:
                self._start_wifi()
            except Exception as exc:
                self._fail(exc)
                return False
            self._stage = STAGE_WIFI
            self._wifi_deadline_ms = self._ticks_add(now, config.WIFI_JOIN_TIMEOUT_MS)
            # Association is asynchronous: come back next tick rather than
            # spending watchdog budget waiting for it.
            return False

        # STAGE_WIFI: poll only, never block.
        if not self.wifi_connected():
            if self._clock.ticks_diff(self._clock.ticks_ms(), self._wifi_deadline_ms) >= 0:
                self._fail(OSError("wifi join timeout"))
            return False

        # Wi-Fi is up. The MQTT handshake is the one blocking step left.
        self._feed()
        try:
            self._connect_mqtt()
        except Exception as exc:
            self._feed()
            self._fail(exc)
            return False
        self._feed()

        self._stage = STAGE_IDLE
        self._attempt = 0
        self._next_attempt_ms = None
        self.consecutive_failures = 0
        self.reconnects += 1
        self.last_error = None
        self._last_ping_ms = self._clock.ticks_ms()
        self._last_rx_ms = self._last_ping_ms
        return True

    def _ticks_add(self, ticks, delta):
        """Wrap-safe addition on a tick reading.

        MicroPython's tick counter wraps; plain addition is only correct by
        accident of the modulus. ``Clock`` provides ``ticks_add`` where the
        platform has it.
        """
        adder = getattr(self._clock, "ticks_add", None)
        if adder is not None:
            return adder(ticks, delta)
        return ticks + delta

    def _fail(self, exc):
        """Record a failed attempt, schedule backoff, and escalate if needed."""
        self._client = None
        self._stage = STAGE_IDLE
        self._wifi_deadline_ms = None
        self.last_error = _bounded_error(exc)
        self._attempt += 1
        self.consecutive_failures += 1
        self._next_attempt_ms = self._ticks_add(
            self._clock.ticks_ms(), backoff_delay_ms(self._attempt)
        )
        limit = config.RECONNECT_RADIO_RESET_ATTEMPTS
        if limit and self.consecutive_failures % limit == 0:
            self.reset_radio()

    def reset_radio(self):
        """Power-cycle the Wi-Fi interface.

        A CYW43 that has wedged after days of uptime does not recover from
        another ``connect()`` call - the interface has to be brought down and
        re-initialized. If even that does not help, main.py escalates to a full
        board reset once no pump is running.
        """
        self.radio_resets += 1
        wlan = self._wlan
        self._wlan = None
        if wlan is None:
            return
        for method in ("disconnect", "active", "deinit"):
            try:
                if method == "active":
                    wlan.active(False)
                else:
                    getattr(wlan, method)()
            except Exception:
                pass

    def _start_wifi(self):
        """Begin association. Does not wait for it."""
        import network

        if self._wlan is None:
            self._wlan = network.WLAN(network.STA_IF)
        self._wlan.active(True)
        if self._wlan.isconnected():
            return
        from qp_secrets import WIFI_SSID, WIFI_PASSWORD

        self._wlan.connect(WIFI_SSID, WIFI_PASSWORD)

    def _connect_mqtt(self):
        client = _retain_aware_client_class()(
            self._client_id,
            self._broker,
            port=self._port,
            user=self._user,
            password=self._password,
            keepalive=config.MQTT_KEEPALIVE_SECONDS,
        )
        client.set_callback(self._on_message)
        # Keeps every later read bounded; see RetainAwareMQTTClient.
        client.socket_timeout = config.SOCKET_TIMEOUT_SECONDS
        client.activity_hook = self._note_activity
        if self._last_will is not None:
            topic, payload = self._last_will
            # Retained so the backend sees the device as offline until it
            # publishes a fresh, non-retained boot snapshot.
            client.set_last_will(topic, payload, retain=True, qos=0)
        client.connect(clean_session=True, timeout=config.SOCKET_TIMEOUT_SECONDS)
        self._client = client
        self.resubscribe()

    def resubscribe(self):
        for topic in self._subscriptions:
            # Each subscribe blocks on its own SUBACK.
            self._feed()
            self._client.subscribe(
                topic if isinstance(topic, bytes) else topic.encode("utf-8"), qos=1
            )
        self._feed()

    def drop(self, error=None):
        if error is not None:
            self.last_error = _bounded_error(error)
        if self._client is not None:
            try:
                self._client.disconnect()
            except Exception:
                pass
        self._client = None
        self._stage = STAGE_IDLE
        self._wifi_deadline_ms = None
        self._last_rx_ms = None
        self._last_ping_ms = None
        # A dropped session counts toward escalation; a successful reconnect
        # clears the count. A link that keeps dropping is as dead as one that
        # never connects.
        self.consecutive_failures += 1
        self._next_attempt_ms = self._ticks_add(
            self._clock.ticks_ms(), backoff_delay_ms(1)
        )

    # --- traffic ---------------------------------------------------------------

    def _note_activity(self):
        """Called by the client for every packet actually received."""
        self._last_rx_ms = self._clock.ticks_ms()

    def stale(self):
        """True when nothing has been received for longer than the bound.

        Writes to a half-open TCP connection succeed locally for a long time,
        so silence from the broker is the only reliable liveness signal.
        """
        if self._client is None or self._last_rx_ms is None:
            return False
        elapsed = self._clock.ticks_diff(self._clock.ticks_ms(), self._last_rx_ms)
        return elapsed >= config.MQTT_INACTIVITY_TIMEOUT_MS

    def poll(self):
        """Service one pending inbound message, if any."""
        if self._client is None:
            return
        try:
            self._client.check_msg()
        except Exception as exc:
            # Deliberately broad: the vendored client can raise AssertionError,
            # IndexError or MemoryError on a malformed or truncated packet, and
            # none of those should escape into the main loop and stop the
            # deadline checks.
            self.drop(exc)
            return
        if self.stale():
            self.drop(OSError("mqtt inactivity timeout"))
            return
        self._maybe_ping()

    def _maybe_ping(self):
        if self._last_ping_ms is None:
            return
        now = self._clock.ticks_ms()
        if self._clock.ticks_diff(now, self._last_ping_ms) < config.MQTT_PING_INTERVAL_MS:
            return
        try:
            self._client.ping()
            self._last_ping_ms = now
        except Exception as exc:
            self.drop(exc)

    def publish(self, topic, payload, qos=1, retain=False):
        if self._client is None:
            return False
        # A QoS 1 publish blocks until its PUBACK arrives, which can outlast a
        # loop iteration on a slow link.
        self._feed()
        try:
            self._client.publish(
                topic if isinstance(topic, bytes) else topic.encode("utf-8"),
                payload,
                retain=retain,
                qos=qos,
            )
            return True
        except Exception as exc:
            self.drop(exc)
            return False
        finally:
            self._feed()


def _retain_aware_client_class():
    """Build an ``MQTTClient`` subclass that exposes the PUBLISH retain flag.

    ``umqtt.simple`` invokes ``self.cb(topic, msg)`` and discards the fixed
    header, so a subscriber cannot tell a live command from a retained replay.
    Firmware must never water a plant just because a retained command was
    redelivered on reconnect, so ``wait_msg`` is re-implemented here with the
    retain bit passed through as a third callback argument.

    Two further departures from the vendored ``wait_msg``:

      - it restores the socket's *timeout* rather than calling
        ``setblocking(True)``. ``check_msg`` puts the socket in non-blocking
        mode, and the upstream restore clears the timeout set at connect time,
        after which any later read can block the main loop indefinitely;
      - it reports every received packet through ``activity_hook``, including
        PINGRESP, which is what lets the supervisor notice a half-open
        connection instead of publishing into a socket nobody is reading.

    This mirrors ``simple.MQTTClient.wait_msg``; keep the two in step if the
    vendored client is ever updated.
    """
    from simple import MQTTClient

    class RetainAwareMQTTClient(MQTTClient):
        socket_timeout = None
        activity_hook = None

        def _restore_timeout(self):
            if self.socket_timeout is None:
                self.sock.setblocking(True)
            else:
                self.sock.settimeout(self.socket_timeout)

        def _note_activity(self):
            hook = self.activity_hook
            if hook is not None:
                hook()

        def wait_msg(self):
            res = self.sock.read(1)
            self._restore_timeout()
            if res is None:
                return None
            if res == b"":
                raise OSError(-1)
            self._note_activity()
            if res == b"\xd0":  # PINGRESP
                size = self.sock.read(1)[0]
                assert size == 0
                return None
            op = res[0]
            if op & 0xF0 != 0x30:
                return op
            size = self._recv_len()
            topic_len = self.sock.read(2)
            topic_len = (topic_len[0] << 8) | topic_len[1]
            topic = self.sock.read(topic_len)
            size -= topic_len + 2
            pid = None
            if op & 6:
                pid = self.sock.read(2)
                pid = pid[0] << 8 | pid[1]
                size -= 2
            msg = self.sock.read(size)
            self.cb(topic, msg, bool(op & 0x01))
            if op & 6 == 2:
                import struct

                packet = bytearray(b"\x40\x02\0\0")
                struct.pack_into("!H", packet, 2, pid)
                self.sock.write(packet)
            elif op & 6 == 4:
                assert 0
            return op

    return RetainAwareMQTTClient


def backoff_delay_ms(attempt, base=None, maximum=None, jitter=None, rand=None):
    """Exponential backoff with bounded jitter, capped so a long outage still
    retries at a predictable floor."""
    base = base if base is not None else config.RECONNECT_BASE_MS
    maximum = maximum if maximum is not None else config.RECONNECT_MAX_MS
    jitter = jitter if jitter is not None else config.RECONNECT_JITTER_MS
    if attempt < 1:
        attempt = 1
    if attempt > 20:
        attempt = 20
    delay = base * (2 ** (attempt - 1))
    if delay > maximum:
        delay = maximum
    if jitter:
        if rand is None:
            import random

            rand = random.getrandbits(16)
        delay += rand % (jitter + 1)
    return delay


def _bounded_error(exc):
    # Bounded and non-secret: type name only, never a traceback or credentials.
    return type(exc).__name__[:60]
