## Context

dart_nats 1.2.0 introduced `_reconnectLoopBackground`, triggered from `_setStatus(Status.disconnected)` when `_wasConnected && _retry`. The loop incorrectly gates on `status == Status.disconnected` while setting `Status.reconnecting` at the start of each iteration and never resetting status after a failed `Socket.connect`. After one offline failure the loop exits, clears `_reconnecting`, and leaves the client stuck in `reconnecting` with no further attempts—observed on Android emulator airplane mode (Chat pub/sub and JetStream KV both fail; Socket.IO recovers, so the network is fine).

Initial `_connectLoop` (first `connect()`) already retries by attempt count and is not the airplane bug. Existing tests use `tcpClose()` with a live server, so the first reconnect connect succeeds and the bug is invisible.

## Goals / Non-Goals

**Goals:**

- Keep retrying reconnect attempts across repeated connect failures until success, `closed`, `retry: false`, or finite `retryCount` exhaustion.
- Apply `retryInterval` between failed attempts even while status is `reconnecting`.
- Avoid hanging the reconnect loop on `Socket.close()` after abrupt network loss.
- Ensure TCP error paths can enter the reconnect flow (`onError` → `disconnected`).
- Invoke `onReconnect` on successful recovery after a prior connection (so apps can re-bind KV watches).
- Prove the fix with a test that fails connect multiple times before succeeding.

**Non-Goals:**

- Changing JetStream/KV watch lifecycle inside the library (app re-opens watches on reconnect).
- Using heartbeat defaults (`pingInterval` / `maxPingsOut`) as the airplane-mode fix.
- Reworking initial `_connectLoop` beyond what’s needed for shared helpers.
- Upstream PR process / version bump policy (fork may ship independently).

## Decisions

### 1. Loop predicate: allow non-terminal statuses

**Choice:** Continue while `_retry` and `status` is neither `connected` nor `closed`, and attempt budget remains (`retryCount == -1` or `attempts < maxAttempts`).

**Why:** Matches intended “keep trying until up or give up” semantics. Requiring `disconnected` contradicts the body’s `reconnecting` transition.

**Alternatives considered:**

- Reset to `disconnected` after every failed attempt so the old `while` condition works → noisy `onDisconnect` / status churn; risk of double-starting the loop if `_reconnecting` is cleared incorrectly.
- Only add `reconnecting` to the `while` OR condition but keep delay gated on `disconnected` → still skips delay; busy-spin risk while offline.

### 2. Delay gate: attempts / `_retry`, not `status == disconnected`

**Choice:** After a failed attempt, if still within budget and `_retry`, always `await Future.delayed(Duration(seconds: _reconnectInterval))` before the next iteration (unless status became `closed` / `connected`).

**Why:** Offline failures leave status at `reconnecting`; the current delay check is a no-op and also contributes to immediate loop exit patterns.

### 3. Do not bounce to `disconnected` on every failed connect

**Choice:** Stay on `reconnecting` across failed attempts; only leave via success (`infoHandshake` → `connected`), `closed`, or giving up.

**Why:** Avoids repeated `onDisconnect` callbacks and accidental re-entry into `_reconnectLoopBackground` via the disconnected handler.

### 4. Soft-fail / timed cleanup on reconnect failure

**Choice:** On reconnect catch (and preferably shared cleanup used only from the reconnect path), do not block indefinitely on `socket.close()`. Prefer: null out socket refs first, then close with a short timeout, ignoring close errors (spirit of older soft-fail `return false` without awaiting a zombie close).

**Why:** Android airplane can leave TCP sockets that hang on `close()`; awaiting that stalls or kills the retry loop.

**Alternatives considered:** Always `unawaited(close())` with no timeout → fire-and-forget leaks harder to reason about; full rewrite of ownership → out of scope.

### 5. TCP `onError` → `disconnected`

**Choice:** Mirror TLS / `onDone`: on TCP `onError`, call `_setStatus(Status.disconnected)` (after optional `onError` callback), without calling `close()` unless policy already does for WS.

**Why:** Airplane and some platforms may surface `SocketException` on `onError` without a timely `onDone`; without this, status can remain falsely `connected` until heartbeats expire.

### 6. `onReconnect` based on reconnect cycle, not `oldStatus == reconnecting`

**Choice:** Track that the client is completing a reconnect (e.g. `_reconnecting` was true / a `_reconnectCycle` flag set when the background loop started) and, when transitioning to `connected`, call `onReconnect` if that flag is set; otherwise `onConnect`. Clear the flag after.

**Why:** Success path is `reconnecting → infoHandshake → connected`, so `oldStatus` at `connected` is never `reconnecting`. Current check never fires `onReconnect` on the happy path.

**Alternatives considered:** Treat any `connected` after `_wasConnected` as reconnect → wrong for explicit `close()` + `connect()` if `_wasConnected` semantics differ; keep `oldStatus == reconnecting` and also jump status from reconnecting directly to connected → loses handshake visibility.

### 7. Test strategy: injectable connect + repeated failures

**Choice:** Add a `@visibleForTesting` (or package-private) hook to replace `Socket.connect` (and ideally WS connect if cheap), then write a test that fails N times then succeeds, asserting status eventually `connected` and that multiple reconnect attempts occurred. Keep existing `tcpClose()` tests.

**Why:** Cannot reliably toggle airplane in CI; `tcpClose()` never exercises failed connect. A delayed “start listening” server is fragile with NATS handshake; a connect stub is precise.

**Alternatives considered:** Only integration/manual airplane checklist → necessary for acceptance on device, insufficient as sole regression gate.

### 8. Exhaustion behavior

**Choice:** When finite attempts are exhausted while still not connected, set status appropriately and stop (`close()` or leave `disconnected` consistently with initial connect exhaustion). Ensure `_reconnecting` is cleared so a later explicit `connect()` can run. Prefer aligning with “give up → closed or disconnected + stop retry” without leaving a permanent `reconnecting` zombie.

**Why:** Today exhaustion with status stuck at `reconnecting` skips the final `if (status == disconnected) close()`.

## Risks / Trade-offs

- **[Risk] Soft-fail close leaves FD briefly open** → Mitigation: null refs first; short timeout close; accept brief leak vs hung client.
- **[Risk] `onReconnect` vs `onConnect` behavior change for apps that relied on `onConnect` firing on every recovery** → Mitigation: document; apps using only `onConnect` for “ready” still work if they also listen to status or we call the correct one; prefer correct NATS-client semantics.
- **[Risk] TCP `onError` + `onDone` both fire → double disconnect / double loop start** → Mitigation: `_reconnecting` guard and idempotent `_setStatus(disconnected)`; ignore events if socket identity changed.
- **[Risk] Test hook misuse in production** → Mitigation: `@visibleForTesting`, default null, only override in tests.
- **[Risk] Manual airplane still fails due to unrelated hang** → Mitigation: include timed cleanup; validate on emulator as acceptance outside unit tests.

## Migration Plan

- Fork consumers (Flutter app): bump dependency to the fixed fork revision; no API migration required. Ensure KV watches are re-created on `onReconnect` or `Status.connected` after a drop (already app responsibility).
- Rollback: revert client.dart reconnect changes; behavior returns to stuck-after-one-failure (known bad).

## Open Questions

- Exact cleanup timeout duration (e.g. 1–2s) — pick a small constant in implementation unless device testing suggests otherwise.
- Whether WS `onError` (currently `close()`) should also be softened for mobile WS deployments — only if we see the same hang; default leave WS path unchanged unless trivial to align.
