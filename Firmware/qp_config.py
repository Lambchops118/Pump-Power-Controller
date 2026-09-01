# Part of TALOS - Monkey Butler Device Operations System
#
# Quad pump firmware: non-secret configuration.
#
# Nothing in this file is a credential. Wi-Fi and broker credentials live in a
# device-local, untracked ``qp_secrets.py`` (see ``qp_secrets_example.py``).
#
# Runs on MicroPython, so this module deliberately avoids typing/dataclasses/
# enum/__future__ and other CPython-only constructs.

FIRMWARE_VERSION = "quad_pump-2.0.0"

# --- device / awareness identity ---------------------------------------------

ENTITY_ID = "quad_pump"
DOMAIN = "irrigation"
SOURCE_ID = "quad_pump_canonical"

TOPIC_BASE = "home/" + DOMAIN + "/" + ENTITY_ID
TOPIC_COMMAND = TOPIC_BASE + "/command"
TOPIC_STATE = TOPIC_BASE + "/state"
TOPIC_EVENT = TOPIC_BASE + "/event"
TOPIC_HEALTH = TOPIC_BASE + "/health"
TOPIC_HEARTBEAT = TOPIC_BASE + "/heartbeat"

# The fan Pico also uses "pico-w-client"; a colliding MQTT client ID makes the
# broker evict one board whenever the other connects. The runtime appends this
# board's machine.unique_id() to the prefix.
MQTT_CLIENT_ID_PREFIX = "talos-quad-pump-"

EVENT_TYPE_COMMAND_ACK = ENTITY_ID + ".command_ack"
EVENT_TYPE_COMMAND_RECEIPT = ENTITY_ID + ".command_received"
EVENT_TYPE_FUSE_TRANSITION = ENTITY_ID + ".fuse_transition"
EVENT_TYPE_RUN_STOPPED = ENTITY_ID + ".run_stopped"

# --- confirmed hardware mapping (netlist-derived 2026-08-28) ------------------
#
# Taken from the Controller_Board_mk2 netlist, which is the authoritative record
# of what the board actually routes. Each channel drives a base resistor into a
# low-side NPN switch that energizes one relay coil:
#
#     GP6 -> R1 -> Q1 -> K4 -> output J2   (fuse divider F2 -> R5 -> GP0)
#     GP7 -> R4 -> Q4 -> K3 -> output J3   (fuse divider F1 -> R6 -> GP1)
#     GP8 -> R3 -> Q3 -> K2 -> output J4   (fuse divider F3 -> R7 -> GP2)
#     GP9 -> R2 -> Q2 -> K1 -> output J5   (fuse divider F4 -> R8 -> GP3)
#
# Channels are numbered in output-connector order (J2..J5). Which pot a channel
# waters is decided by which terminal its pump is plugged into, not by firmware.
#
# This supersedes the GP9/GP10/GP11/GP12 table, which was inferred rather than
# measured (SESSION_HANDOFF_2026-07-26_QUAD_PUMP_GPIO_MAPPING.md records that no
# physical command was ever issued against it). The board marks GP10, GP11 and
# GP12 as unconnected, so channels 2-4 drove floating pins and no relay moved.
#
# Logical channel numbers 1-4 are the only identifiers used off-device. GPIO
# numbers never appear in an MQTT topic, payload, or action parameter.
CHANNELS = (1, 2, 3, 4)

CHANNEL_RELAY_GPIO = {1: 6, 2: 7, 3: 8, 4: 9}
CHANNEL_FUSE_GPIO = {1: 0, 2: 1, 3: 2, 4: 3}

# Netlist-confirmed: Q1-Q4 are low-side NPN switches, so driving the GPIO high
# energizes the relay and runs the pump; driving it low is the safe state.
# Boot/reset leaves the pins as inputs (high impedance) until initialize()
# drives them low.
RELAY_ACTIVE_HIGH = True

# --- confirmed safety policy --------------------------------------------------

# Owner-confirmed: one pump at a time until the supply/wiring budget is
# measured, with a 30 second hard ceiling enforced locally by the firmware.
MAX_CONCURRENT_PUMPS = 1
MAX_RUN_SECONDS = 30

# One pump at a time is a supply limit, not a reason to discard a request.
# Runs that arrive while another channel is busy wait here and start in order.
# Bounded so a stuck queue cannot outlive the awareness timeout by much:
# four channels at MAX_RUN_SECONDS each is the worst honest wait.
PENDING_QUEUE_MAX = 4
PENDING_MAX_WAIT_MS = (len(CHANNELS) * MAX_RUN_SECONDS) * 1000
MIN_RUN_SECONDS = 1
# Used when a command omits duration_seconds, and always on the legacy topics
# (bare 0/1 payloads carry no duration). Owner-set 2026-08-28, replacing the
# legacy firmware's fixed 8 second cycle.
DEFAULT_RUN_SECONDS = 30

# Owner-confirmed policy for a *confirmed* blown fuse: refuse to start the
# channel and immediately stop it if it is already running. "unknown" is not a
# fault and never inhibits anything.
#
# NOTE (revisit): fuse sensing is NOT IMPLEMENTED on this hardware revision.
# The fuse signal is an analog divider, but GP0/GP1/GP2/GP3 have no ADC — the Pico W
# exposes only GP26-GP28. Every channel therefore reports "unknown" forever and
# this policy currently has nothing to act on. See FUSE_SENSING_AVAILABLE below
# and docs/awareness-memory/OPEN_QUESTIONS.md (OQ-D).
FUSE_FAULT_INHIBITS_START = True
FUSE_FAULT_STOPS_RUN = True

# Flip to True only once the fuse signal can actually be measured (external
# I2C ADC, or a divider proven to meet 3.3 V logic V_IH/V_IL margins). Until
# then the firmware must not claim a fuse is "ok".
FUSE_SENSING_AVAILABLE = False

FUSE_UNKNOWN = "unknown"
FUSE_OK = "ok"
FUSE_BLOWN = "blown"
FUSE_INVALID = "invalid"

# Consecutive agreeing samples required before a fuse state change is published.
FUSE_DEBOUNCE_SAMPLES = 5
FUSE_SAMPLE_INTERVAL_MS = 100

# --- command validation bounds ------------------------------------------------

# Rejected before the JSON parser sees the bytes.
MAX_COMMAND_BYTES = 1024
ACTION_RUN_PUMP = "run_pump"
ACTION_STOP_PUMP = "stop_pump"
SUPPORTED_ACTIONS = (ACTION_RUN_PUMP, ACTION_STOP_PUMP)
TARGET_ENTITY_ID = ENTITY_ID

# Negative result codes published in command acknowledgements.
RESULT_COMPLETED = "completed"
RESULT_INVALID_COMMAND = "invalid_command"
RESULT_UNSUPPORTED_ACTION = "unsupported_action"
RESULT_INVALID_CHANNEL = "invalid_channel"
RESULT_INVALID_DURATION = "invalid_duration"
RESULT_BUSY = "busy"
RESULT_FUSE_FAULT = "fuse_fault"
RESULT_POWER_LIMIT = "power_limit"
RESULT_STOPPED = "stopped"
RESULT_INTERNAL_ERROR = "internal_error"

# --- command ledger -----------------------------------------------------------

# Bounded in-memory + on-flash record of recent command outcomes. Sized to
# comfortably cover the awareness retry window without meaningful flash wear:
# at most two small writes per command (accept, complete).
LEDGER_CAPACITY = 16
LEDGER_PATH = "qp_ledger.json"
LEDGER_TEMP_PATH = "qp_ledger.tmp"

# --- legacy compatibility (staged rollout) ------------------------------------
#
# The awareness action registry still dispatches water_plants on the legacy
# flat topics. Keeping these alive means flashing this firmware does not break
# watering before the canonical action is validated end to end.
#
# Owner-confirmed chain: pot 1 = legacy pin 17 = logical channel 1,
# pot 2 = legacy pin 19 = logical channel 2. Legacy pins 16 and 18 have no
# owner-confirmed pot and are deliberately absent — an unmapped legacy pin is
# rejected rather than guessed from list ordering.
LEGACY_COMPAT_ENABLED = True
LEGACY_COMMAND_PREFIX = "quad_pump/"
LEGACY_STATUS_PREFIX = "status/"
LEGACY_PIN_TO_CHANNEL = {17: 1, 19: 2}

# --- loop timing / network ----------------------------------------------------

TICK_SLEEP_MS = 20
HEARTBEAT_INTERVAL_MS = 30000
HEALTH_INTERVAL_MS = 300000
STATE_SNAPSHOT_INTERVAL_MS = 300000

MQTT_KEEPALIVE_SECONDS = 60
MQTT_PING_INTERVAL_MS = 20000

# A PINGREQ that never gets its PINGRESP is the only evidence the firmware has
# that a TCP connection has gone half-open: writes keep "succeeding" into a
# socket the far end has forgotten. Without this bound the board reports
# mqtt_connected forever and publishes into the void. Four missed pings.
MQTT_INACTIVITY_TIMEOUT_MS = 90000

RECONNECT_BASE_MS = 1000
RECONNECT_MAX_MS = 60000
RECONNECT_JITTER_MS = 500

# Escalation ladder for a network that does not come back. Another connect()
# call does not recover a wedged CYW43; the interface has to be brought down
# and re-initialized, and past that only a full reset will do.
#
# Consecutive failed attempts before the Wi-Fi interface is power-cycled:
RECONNECT_RADIO_RESET_ATTEMPTS = 5
# Consecutive failed attempts before main.py resets the board. Only ever taken
# while no pump is running and none is queued. At the 60 s backoff cap this is
# roughly twenty minutes offline.
RECONNECT_HARD_RESET_ATTEMPTS = 20

# Bounded outbound buffer: important state/ack events survive a brief outage
# without letting a long outage exhaust RAM. Oldest non-critical entries are
# dropped first and the drop is reported in health.
OUTBOUND_QUEUE_MAX = 32

# The watchdog must be longer than any single blocking operation between two
# feeds. On RP2040 the hardware maximum is ~8388 ms, so the budget cannot be
# raised to fit the network - the blocking work has to fit under it instead.
#
# Measured on the board 2026-09-01: a Wi-Fi join takes ~4000 ms and a TCP
# connect to an unreachable LAN host blocks for the full socket timeout. Doing
# both between two feeds cost ~9000 ms against this 8000 ms watchdog, so the
# board reset mid-connect, before any backoff was recorded, and did it again on
# every reboot: a permanent reset loop for as long as the broker was down.
#
# Two things keep that from recurring:
#   - the Wi-Fi join is now polled from the main loop instead of blocked on, so
#     it costs no watchdog budget at all (and may take longer than the socket
#     timeout without being treated as a failure);
#   - the MQTT handshake is the only blocking step left, and connect + CONNACK
#     is bounded by 2 x SOCKET_TIMEOUT_SECONDS, which must stay under this.
WATCHDOG_TIMEOUT_MS = 8000
SOCKET_TIMEOUT_SECONDS = 3

# How long the firmware will poll for a Wi-Fi association before calling the
# attempt failed and re-issuing connect(). Larger than SOCKET_TIMEOUT_SECONDS
# on purpose: waiting here costs no watchdog budget, because it does not block.
#
# Measured on the board 2026-09-01 over repeated cold starts: bringing the
# interface up takes ~970 ms and a successful association ~4.1 s. But an
# association that stalls does not recover on its own - one run sat unjoined
# for the full 40 s of the test, and only a fresh connect() fixed it. So this
# is a stall detector, not a patience setting: roughly twice the observed join
# time, low enough to retry promptly and high enough not to cut short a slow
# but genuine association.
WIFI_JOIN_TIMEOUT_MS = 8000

# Publications drained per tick. Each one can block on its QoS 1 PUBACK, so
# the watchdog is fed before every publish rather than once per tick.
OUTBOUND_DRAIN_PER_TICK = 4
