# RelayKV

> A high-performance, Redis-compatible in-memory key-value store built from scratch in Go.

RelayKV implements the RESP (Redis Serialization Protocol) wire protocol, meaning any standard Redis client — including `redis-cli` — can connect to it directly without modification.

The name reflects what the system does: it relays data between clients in real time, and relays write commands from primary to replicas across the network.

Built as a deep-dive engineering project to understand how Redis works at the systems level — from raw TCP and protocol parsing, to concurrent data access, expiration algorithms, persistence, pub/sub messaging, and replication.

---

## Features

| Feature | Details |
|---|---|
| RESP Protocol | Custom-built parser and encoder for the Redis wire protocol |
| Concurrent clients | Each connection handled in its own goroutine |
| Thread safety | `sync.RWMutex` with correct read/write lock separation |
| Dual encoding | Values stored as `StringEncoding` or `IntEncoding` internally |
| TTL expiration | Millisecond-precision per-key expiration |
| Lazy expiration | Expired keys evicted on access |
| Active expiration | Background engine modelled after Redis 6's algorithm |
| Min-heap tracking | `container/heap` used to track keys expiring within 30s for fast eviction |
| AOF persistence | Write commands logged to disk, replayed on restart |
| Background fsync | AOF flushed to disk every second |
| Pub/Sub | Channel-based messaging with SUBSCRIBE, UNSUBSCRIBE, PUBLISH |
| Replication | Single-primary replication over persistent TCP connections |
| READONLY safety | Replicas reject write commands from clients |
| WRONGTYPE errors | Type safety across string and list operations |

---

## Supported Commands

### String Commands

| Command | Syntax | Description |
|---|---|---|
| `PING` | `PING [message]` | Returns PONG or echoes message |
| `ECHO` | `ECHO message` | Returns the message |
| `SET` | `SET key value [EX seconds]` | Set a key with optional TTL |
| `GET` | `GET key` | Get the value of a key |
| `DEL` | `DEL key [key ...]` | Delete one or more keys |
| `INCR` | `INCR key` | Atomically increment an integer value |
| `TTL` | `TTL key` | Get remaining time-to-live in seconds |

### List Commands

| Command | Syntax | Description |
|---|---|---|
| `LPUSH` | `LPUSH key value [value ...]` | Prepend values to a list |
| `RPUSH` | `RPUSH key value [value ...]` | Append values to a list |
| `LPOP` | `LPOP key` | Remove and return the first element |
| `RPOP` | `RPOP key` | Remove and return the last element |
| `LRANGE` | `LRANGE key start stop` | Get a range of elements |

### Pub/Sub Commands

| Command | Syntax | Description |
|---|---|---|
| `SUBSCRIBE` | `SUBSCRIBE channel [channel ...]` | Subscribe to channels |
| `UNSUBSCRIBE` | `UNSUBSCRIBE [channel ...]` | Unsubscribe from channels |
| `PUBLISH` | `PUBLISH channel message` | Send a message to a channel |

---

## Getting Started

**Requirements:** Go 1.21+

```bash
git clone https://github.com/blvckbill/relaykv
cd relaykv
go build -o relaykv ./cmd/goredis
```

**Run as primary:**
```bash
./relaykv
# RelayKV is listening on 127.0.0.1:6369
```

**Run as replica:**
```bash
./relaykv --port 6370 --replicaof 127.0.0.1:6369
```

**Connect with any Redis client:**
```bash
redis-cli -p 6369 PING
redis-cli -p 6369 SET name "RelayKV"
redis-cli -p 6369 GET name
redis-cli -p 6369 SET counter 0 EX 60
redis-cli -p 6369 INCR counter
redis-cli -p 6369 TTL counter
```

---

## Project Structure

```
.
├── cmd/
│   └── goredis/
│       └── main.go           # Entrypoint, CLI flags
├── internal/
│   ├── protocol/
│   │   └── resp.go           # Custom RESP parser and encoder
│   ├── store/
│   │   └── store.go          # In-memory store, expiration engine, min-heap
│   └── server/
│       ├── server.go         # TCP server, connection handler
│       ├── handlers.go       # Command handlers
│       └── aof.go            # AOF logger, fsync, replay
└── test_commands.sh          # Full command battery test (30 tests)
```

---

## How It Works

### RESP Protocol Parser

The RESP parser was built entirely from scratch — no third-party libraries. It reads raw bytes from the TCP stream and reconstructs typed command structures based on the first byte of each message:

- `+` Simple String
- `-` Error
- `:` Integer
- `$` Bulk String
- `*` Array

A command like `SET name RelayKV` arrives over the wire as:

```
*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$7\r\nRelayKV\r\n
```

The parser handles partial reads — data is accumulated in a buffer and parsed incrementally, so commands split across multiple TCP reads are handled correctly. A matching encoder converts response structs back into RESP bytes before writing to the client.

### Expiration Engine

Expiration works at two levels:

**Lazy** — on every `GET`, `TTL`, `INCR`, and `DEL`, the key's expiry timestamp is checked against the current time in milliseconds. If it has passed, the key is deleted on the spot and the caller gets a miss.

**Active** — a background goroutine runs every 100ms. Each cycle does two things:

1. **Min-heap sweep** — RelayKV uses Go's `container/heap` to maintain a min-heap of keys expiring within the next 30 seconds, ordered by expiry timestamp. The smallest expiry is always at the top. Each cycle pops and deletes everything at the top that has now expired — O(log n) per deletion. Keys found during random sampling that are expiring soon get added to the heap so the next cycle catches them early.

2. **Random sampling** — 20 keys are sampled randomly from the expiration dictionary. Expired ones are deleted immediately. If more than 25% of sampled keys are expired, the engine loops again without waiting for the next tick — the same adaptive behaviour Redis uses under high expiry load.

Each cycle is hard-capped at 25ms to avoid CPU starvation. All expiry timestamps are stored in milliseconds internally.

### Concurrency

Every store operation acquires the appropriate lock:

- `GET`, `TTL` → `RLock` (multiple readers run in parallel)
- `SET`, `DEL`, `INCR`, `LPUSH`, `RPUSH`, `LPOP`, `RPOP` → `Lock` (exclusive write)
- `INCR` holds the lock for the full read-modify-write cycle, making it atomic under concurrent load

Pub/Sub uses its own `RWMutex` separate from the store so subscribe and publish operations never block data commands. Replication uses its own `Mutex` protecting the replica connection list.

### AOF Persistence

Every successful write command is encoded as RESP and appended to `appendonly.aof`. On restart, the file is read back and each command is replayed through the command router to rebuild state in memory. The `isReplaying` flag prevents replayed commands from being re-written to the AOF or propagated to replicas during startup.

A background goroutine calls `fsync` every second to flush the OS write buffer to physical disk, ensuring durability between restarts.

### Pub/Sub

RelayKV maintains a map of channel names to sets of subscriber connections. When a client subscribes, its `net.Conn` is registered against the relevant channels. When a message is published, the server iterates the subscriber set and writes directly to each connection. Dead connections are detected on write failure and removed automatically.

Each subscribe and unsubscribe sends one RESP array response per channel, matching Redis protocol exactly:

```
1) "subscribe"
2) "news"
3) (integer) 1
```

### Replication

On startup with `--replicaof`, RelayKV connects to the primary as a TCP client, registers itself via `REPLICAOF`, then enters a replication loop — continuously reading encoded commands from the primary and applying them locally through the same command router used for regular client connections.

On the primary side, every successful write is propagated to all registered replicas immediately after execution. The same RESP-encoded bytes used for AOF are reused for propagation — no double encoding. Replicas reject direct write commands from clients with a `READONLY` error.

---

## Running the Test Suite

```bash
chmod +x test_commands.sh
./test_commands.sh
```

```
