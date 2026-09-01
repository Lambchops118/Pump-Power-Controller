# Part of TALOS - Monkey Butler Device Operations System
#
# Quad pump firmware: per-channel run control.
#
# This module owns local safety. It drives relays directly and only then hands
# outbound messages back to the caller, so a relay always stops on time even
# when Wi-Fi, the broker, the awareness backend, its database, or the LLM is
# unavailable. Nothing here performs network I/O.
#
# Pure logic — host tests drive it with fake relays, fuses, and a fake clock.

import qp_config as config
import qp_protocol as protocol
from qp_ledger import PHASE_FINAL


class Message(object):
    """One outbound publication. ``envelope`` is a canonical event; ``raw`` is
    a legacy compatibility payload."""

    def __init__(self, topic, envelope=None, raw=None, critical=True, retain=False):
        self.topic = topic
        self.envelope = envelope
        self.raw = raw
        self.critical = critical
        self.retain = retain

    def payload(self):
        if self.raw is not None:
            return self.raw
        return protocol.encode(self.envelope)

    def __repr__(self):
        return "<Message %s>" % (self.topic,)


class Clock(object):
    """Monotonic millisecond clock with correct wraparound handling.

    MicroPython's ``time.ticks_ms`` wraps; ``ticks_diff`` is the only correct
    way to compare two readings. On a host both fall back to a plain counter.
    """

    def __init__(self):
        import time

        self._time = time
        self._ticks_ms = getattr(time, "ticks_ms", None)
        self._ticks_diff = getattr(time, "ticks_diff", None)
        self._ticks_add = getattr(time, "ticks_add", None)

    def ticks_ms(self):
        if self._ticks_ms is not None:
            return self._ticks_ms()
        return int(self._time.monotonic() * 1000)

    def ticks_diff(self, later, earlier):
        if self._ticks_diff is not None:
            return self._ticks_diff(later, earlier)
        return later - earlier

    def ticks_add(self, ticks, delta):
        """Offset a tick reading, staying inside the platform's tick range.

        Plain addition can leave the counter's domain near a wrap; only
        ``ticks_add`` is defined there.
        """
        if self._ticks_add is not None:
            return self._ticks_add(ticks, delta)
        return ticks + delta


class FuseMonitor(object):
    """Bounded sampling and debounce over the fuse inputs.

    Only debounced states are published. With the current hardware every read
    returns ``unknown`` (see qp_hardware.FuseBank), so this produces no
    transitions — the machinery exists so that adding a real sensing driver
    does not also require rewriting the reporting path.
    """

    def __init__(self, fuses, clock, samples=None, interval_ms=None):
        self._fuses = fuses
        self._clock = clock
        self._samples = samples or config.FUSE_DEBOUNCE_SAMPLES
        self._interval = interval_ms or config.FUSE_SAMPLE_INTERVAL_MS
        self._published = {}
        self._candidate = {}
        self._count = {}
        self._last_sample = None
        for channel in config.CHANNELS:
            self._published[channel] = config.FUSE_UNKNOWN
            self._candidate[channel] = None
            self._count[channel] = 0

    def state(self, channel):
        return self._published.get(channel, config.FUSE_UNKNOWN)

    def snapshot(self):
        result = {}
        for channel in config.CHANNELS:
            result[channel] = self.state(channel)
        return result

    def sample(self):
        """Take one bounded sampling pass; returns (channel, previous, current)
        tuples for channels whose debounced state changed."""
        now = self._clock.ticks_ms()
        if self._last_sample is not None:
            if self._clock.ticks_diff(now, self._last_sample) < self._interval:
                return []
        self._last_sample = now

        transitions = []
        for channel in config.CHANNELS:
            reading = self._fuses.read(channel)
            published = self._published[channel]
            if reading == published:
                self._candidate[channel] = None
                self._count[channel] = 0
                continue
            if self._candidate[channel] == reading:
                self._count[channel] += 1
            else:
                self._candidate[channel] = reading
                self._count[channel] = 1
            if self._count[channel] >= self._samples:
                self._published[channel] = reading
                self._candidate[channel] = None
                self._count[channel] = 0
                transitions.append((channel, published, reading))
        return transitions


class _Run(object):
    def __init__(self, command_id, channel, started_ms, deadline_ms, correlation_id, legacy_pin):
        self.command_id = command_id
        self.channel = channel
        self.started_ms = started_ms
        self.deadline_ms = deadline_ms
        self.correlation_id = correlation_id
        self.legacy_pin = legacy_pin


class PumpController(object):
    def __init__(self, relays, fuses, events, ledger, clock=None, fuse_monitor=None):
        self._relays = relays
        self._fuses = fuses
        self._events = events
        self._ledger = ledger
        self._clock = clock or Clock()
        self._monitor = fuse_monitor or FuseMonitor(fuses, self._clock)
        self._runs = {}  # channel -> _Run
        self._pending = []  # commands waiting for the concurrency limit

    # --- lifecycle -----------------------------------------------------------

    def initialize(self):
        """Drive every relay safely off and report the resulting truth.

        Any command still marked ``accepted`` in the ledger was interrupted by
        a reset. It is finalized as a failure rather than resumed, so a reboot
        inside the awareness retry window cannot repeat a watering cycle.
        """
        self._relays.initialize_safe()
        self._runs = {}
        self._pending = []
        messages = []
        for entry in self._ledger.recover_unfinished():
            messages.append(
                self._ack(
                    entry.get("command_id"),
                    False,
                    config.RESULT_STOPPED,
                    entry.get("channel"),
                    "off",
                    error=entry.get("error"),
                )
            )
        messages.append(self.state_message())
        return messages

    def state_message(self):
        topic, envelope = self._events.state(
            self._relays.snapshot(), self._monitor.snapshot()
        )
        return Message(topic, envelope=envelope)

    def running_channels(self):
        return tuple(sorted(self._runs))

    def pending_channels(self):
        return tuple(command["channel"] for command in self._pending)

    def _pending_for(self, channel):
        for command in self._pending:
            if command["channel"] == channel:
                return command
        return None

    def is_running(self, channel):
        return channel in self._runs

    def deadline_for(self, channel):
        run = self._runs.get(channel)
        return run.deadline_ms if run is not None else None

    # --- commands ------------------------------------------------------------

    def handle_command(self, command):
        """Execute a validated command. Returns outbound messages.

        Relays are moved before anything is published; publication failures can
        never leave a pump running.
        """
        command_id = command["command_id"]

        recorded = self._ledger.outcome(command_id)
        if recorded is not None:
            # Duplicate of a completed command: replay the recorded outcome
            # without repeating the physical effect.
            return [
                self._ack(
                    command_id,
                    recorded.get("ok", False),
                    recorded.get("result", config.RESULT_INTERNAL_ERROR),
                    recorded.get("channel"),
                    recorded.get("relay_state"),
                    fuse_state=recorded.get("fuse_state"),
                    error=recorded.get("error"),
                    correlation_id=command.get("correlation_id"),
                )
            ]
        if self._ledger.find(command_id) is not None:
            # Duplicate delivery of a command that is still running: silently
            # idempotent. The final acknowledgement follows at the deadline.
            return []

        if command["action"] == config.ACTION_STOP_PUMP:
            return self._stop_command(command)
        return self._run_command(command)

    def reject(self, command_id, code, message, correlation_id=None, channel=None):
        """Publish a negative acknowledgement for a command that parsed but
        cannot be executed, or failed validation entirely."""
        if command_id:
            self._ledger.finalize(
                command_id, False, code, channel=channel, error=message
            )
        return [
            self._ack(
                command_id,
                False,
                code,
                channel,
                None,
                error=message,
                correlation_id=correlation_id,
            )
        ]

    def _run_command(self, command):
        channel = command["channel"]
        correlation_id = command.get("correlation_id")
        fuse = self._monitor.state(channel)

        # A *confirmed* blown fuse inhibits startup. "unknown" is not a fault.
        if fuse == config.FUSE_BLOWN and config.FUSE_FAULT_INHIBITS_START:
            return self.reject(
                command["command_id"],
                config.RESULT_FUSE_FAULT,
                "channel fuse is blown",
                correlation_id,
                channel,
            )
        if channel in self._runs or self._pending_for(channel) is not None:
            return self.reject(
                command["command_id"],
                config.RESULT_BUSY,
                "channel is already running",
                correlation_id,
                channel,
            )
        if len(self._runs) >= config.MAX_CONCURRENT_PUMPS:
            # The supply budget allows one pump at a time, but a request that
            # arrives while another channel is busy is deferred, not discarded.
            # Dropping it here is what made channels 2-4 look dead whenever the
            # automation fired every pot in one batch.
            return self._enqueue(command)

        return self._begin_run(command)

    def _begin_run(self, command):
        channel = command["channel"]
        correlation_id = command.get("correlation_id")
        duration = command.get("duration_seconds") or config.DEFAULT_RUN_SECONDS
        if duration > config.MAX_RUN_SECONDS:
            duration = config.MAX_RUN_SECONDS

        # Durable intent first, then the physical effect. Already recorded if
        # this run waited in the pending queue; accept() is idempotent.
        self._ledger.accept(command["command_id"], command["action"], channel)
        now = self._clock.ticks_ms()
        self._relays.set(channel, True)
        self._runs[channel] = _Run(
            command["command_id"],
            channel,
            now,
            self._ticks_add(now, duration * 1000),
            correlation_id,
            command.get("legacy_pin"),
        )

        topic, envelope = self._events.command_receipt(
            command["command_id"], command["action"], channel, correlation_id
        )
        return [
            Message(topic, envelope=envelope, critical=False),
            self.state_message(),
        ]

    def _enqueue(self, command):
        """Defer a run until the concurrency limit frees up.

        The command is recorded as accepted before it is queued, so a duplicate
        delivery is idempotent and a reset while it waits is finalized as
        stopped by boot recovery rather than started later.
        """
        channel = command["channel"]
        correlation_id = command.get("correlation_id")
        if len(self._pending) >= config.PENDING_QUEUE_MAX:
            return self.reject(
                command["command_id"],
                config.RESULT_POWER_LIMIT,
                "pump queue is full",
                correlation_id,
                channel,
            )
        self._ledger.accept(command["command_id"], command["action"], channel)
        command["queued_ms"] = self._clock.ticks_ms()
        self._pending.append(command)

        # Progress only, never a final result: the run has not started.
        topic, envelope = self._events.command_receipt(
            command["command_id"], command["action"], channel, correlation_id
        )
        return [Message(topic, envelope=envelope, critical=False)]

    def _start_pending(self):
        """Start queued runs while capacity allows, dropping any that waited
        longer than the awareness request could plausibly still care about."""
        messages = []
        now = self._clock.ticks_ms()
        while self._pending:
            command = self._pending[0]
            waited = self._clock.ticks_diff(now, command.get("queued_ms", now))
            if waited >= config.PENDING_MAX_WAIT_MS:
                self._pending.pop(0)
                messages.extend(
                    self.reject(
                        command["command_id"],
                        config.RESULT_POWER_LIMIT,
                        "queued longer than the pump queue bound",
                        command.get("correlation_id"),
                        command["channel"],
                    )
                )
                continue
            if len(self._runs) >= config.MAX_CONCURRENT_PUMPS:
                break
            self._pending.pop(0)
            messages.extend(self._begin_run(command))
        return messages

    def _stop_command(self, command):
        channel = command["channel"]
        correlation_id = command.get("correlation_id")
        messages = []
        queued = self._pending_for(channel)
        if queued is not None:
            # Cancelling before the relay ever moved: the deferred run gets its
            # own truthful negative acknowledgement.
            self._pending.remove(queued)
            messages.extend(
                self.reject(
                    queued["command_id"],
                    config.RESULT_STOPPED,
                    "stop_command",
                    queued.get("correlation_id"),
                    channel,
                )
            )
        run = self._runs.get(channel)
        if run is not None:
            # The interrupted run gets its own truthful negative acknowledgement
            # so its awareness request fails instead of silently timing out.
            messages.extend(
                self._finish_run(run, False, config.RESULT_STOPPED, "stop_command")
            )
        else:
            # Idempotent: driving an already-off channel off is a success.
            self._relays.set(channel, False)
        self._ledger.finalize(
            command["command_id"],
            True,
            config.RESULT_COMPLETED,
            channel=channel,
            relay_state="off",
            fuse_state=self._monitor.state(channel),
        )
        messages.append(
            self._ack(
                command["command_id"],
                True,
                config.RESULT_COMPLETED,
                channel,
                "off",
                fuse_state=self._monitor.state(channel),
                correlation_id=correlation_id,
            )
        )
        messages.append(self.state_message())
        return messages

    # --- periodic ------------------------------------------------------------

    def tick(self):
        """Enforce deadlines and fuse policy. Must be called frequently and
        must never block."""
        messages = []
        now = self._clock.ticks_ms()

        for channel, previous, current in self._monitor.sample():
            topic, envelope = self._events.fuse_transition(channel, previous, current)
            messages.append(Message(topic, envelope=envelope))

        for channel in list(self._runs):
            run = self._runs[channel]
            fuse = self._monitor.state(channel)
            if fuse == config.FUSE_BLOWN and config.FUSE_FAULT_STOPS_RUN:
                messages.extend(
                    self._finish_run(run, False, config.RESULT_FUSE_FAULT, "fuse_fault")
                )
                continue
            if self._clock.ticks_diff(now, run.deadline_ms) >= 0:
                messages.extend(
                    self._finish_run(run, True, config.RESULT_COMPLETED, "deadline")
                )

        # Only after the finished runs released their capacity.
        messages.extend(self._start_pending())
        return messages

    def stop_all(self, reason):
        """Immediate local shutdown of every channel (fault paths, shutdown)."""
        messages = []
        while self._pending:
            command = self._pending.pop(0)
            messages.extend(
                self.reject(
                    command["command_id"],
                    config.RESULT_STOPPED,
                    reason,
                    command.get("correlation_id"),
                    command["channel"],
                )
            )
        for channel in list(self._runs):
            messages.extend(
                self._finish_run(
                    self._runs[channel], False, config.RESULT_STOPPED, reason
                )
            )
        self._relays.initialize_safe()
        return messages

    # --- internals -----------------------------------------------------------

    def _ticks_add(self, ticks, delta):
        adder = getattr(self._clock, "ticks_add", None)
        if adder is not None:
            return adder(ticks, delta)
        return ticks + delta

    def _finish_run(self, run, ok, result, reason):
        channel = run.channel
        ran_ms = self._clock.ticks_diff(self._clock.ticks_ms(), run.started_ms)
        self._relays.set(channel, False)
        self._runs.pop(channel, None)

        fuse = self._monitor.state(channel)
        self._ledger.finalize(
            run.command_id,
            ok,
            result,
            channel=channel,
            relay_state="off",
            fuse_state=fuse,
            error=None if ok else reason,
        )

        messages = []
        topic, envelope = self._events.run_stopped(channel, reason, ran_ms)
        messages.append(Message(topic, envelope=envelope, critical=False))
        # Success is acknowledged only after the relay has been driven off.
        messages.append(
            self._ack(
                run.command_id,
                ok,
                result,
                channel,
                "off",
                fuse_state=fuse,
                error=None if ok else reason,
                correlation_id=run.correlation_id,
            )
        )
        messages.append(self.state_message())
        if run.legacy_pin is not None:
            # Minimum legacy evidence the still-deployed water_plants action
            # needs to complete: the pin reported off after the cycle.
            messages.append(
                Message(
                    config.LEGACY_STATUS_PREFIX + str(run.legacy_pin),
                    raw=b"0",
                    critical=False,
                )
            )
        return messages

    def _ack(
        self,
        command_id,
        ok,
        result,
        channel,
        relay_state,
        fuse_state=None,
        error=None,
        correlation_id=None,
    ):
        topic, envelope = self._events.command_ack(
            command_id,
            ok,
            result,
            channel=channel,
            relay_state=relay_state,
            fuse_state=fuse_state,
            error=error,
            correlation_id=correlation_id,
        )
        return Message(topic, envelope=envelope)


__all__ = ["Message", "Clock", "FuseMonitor", "PumpController", "PHASE_FINAL"]
