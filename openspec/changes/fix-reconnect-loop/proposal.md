## Why

In dart_nats 1.2.x, `_reconnectLoopBackground` exits after the first failed reconnect attempt because the `while` condition requires `status == disconnected` while the loop body immediately sets `Status.reconnecting`. After airplane-mode (or any offline period), the client stays stuck in `reconnecting` and never recovers when the network returns—breaking Chat pub/sub and JetStream KV for Flutter apps. Version 0.6.5 / 1.1.x soft-retried by attempt count; 1.2's "robust reconnection" introduced this logic bug. Heartbeats do not fix it.

## What Changes

- Fix `_reconnectLoopBackground` so it keeps retrying while not `connected`/`closed` (and while `_retry` / attempt budget allow), applying `retryInterval` delay after failed attempts regardless of status being `reconnecting`.
- Make socket cleanup on reconnect failure non-blocking (timeout / soft-fail) so a zombie TCP close after airplane mode cannot hang the loop.
- Have TCP socket `onError` transition to `disconnected` (aligned with TLS path and `onDone`), so error-only drops still start reconnect.
- Fire `onReconnect` after a successful reconnect cycle (not only when `oldStatus == reconnecting`, which never holds once `infoHandshake` runs).
- Add a regression test that simulates repeated `Socket.connect` failures before success (more realistic than `tcpClose()` alone).
- Document reconnect behavior and relevant connect options in README (brief).

**Out of scope:** JetStream/KV watch re-binding (app responsibility on `connected`/`onReconnect`); changing default heartbeat intervals as an airplane-mode fix.

## Capabilities

### New Capabilities

- `client-reconnect`: Background reconnection after a previously successful connection—retry loop semantics, status transitions, callbacks (`onReconnect` / `onDisconnect`), and recovery when connect attempts fail while offline.

### Modified Capabilities

- (none — no existing specs under `openspec/specs/`)

## Impact

- **Code:** `lib/src/client.dart` (`_reconnectLoopBackground`, `_setStatus`, TCP `listen` handlers, `_cleanUpSockets` usage on reconnect failure).
- **Tests:** `test/connect_test.dart` and/or `test/api_enhancements_test.dart` — new failure-then-recover case; existing `tcpClose()` happy-path tests should still pass.
- **API:** No **BREAKING** public API changes expected; callback timing for `onReconnect` vs `onConnect` becomes correct for reconnect success (apps that only listened to `onConnect` for reconnect may see `onReconnect` fire instead—desired fix).
- **Docs:** README reconnect / retry notes.
- **Consumers:** Flutter motorized app Chat + remote-config KV benefit once client recovers; app still re-opens KV watches on reconnect.
