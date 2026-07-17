## 1. Reconnect loop fix

- [x] 1.1 Change `_reconnectLoopBackground` while-condition to continue while `_retry` and status is not `connected`/`closed` (within attempt budget)
- [x] 1.2 Apply `retryInterval` delay after failed attempts based on retry budget, not `status == disconnected`
- [x] 1.3 On finite attempt exhaustion, clear `_reconnecting` and exit to a terminal non-`reconnecting` state (align with design: close or disconnected)
- [x] 1.4 Ensure success path still clears `_reconnecting` and returns without starting a duplicate loop

## 2. Cleanup, TCP error path, callbacks

- [x] 2.1 Make reconnect-failure socket cleanup soft-fail / time-bounded so `close()` cannot hang the loop
- [x] 2.2 On TCP `listen` `onError`, set `Status.disconnected` (guarded by socket identity), consistent with `onDone` / TLS
- [x] 2.3 Track background reconnect cycle and invoke `onReconnect` (not `onConnect`) when that cycle reaches `connected`
- [x] 2.4 Keep initial `connect()` success calling `onConnect`

## 3. Testability and regression tests

- [x] 3.1 Add a `@visibleForTesting` Socket.connect (or equivalent) override hook used by `_connectUri`
- [x] 3.2 Add test: N failed connect attempts during background reconnect, then success → `connected`, with delay/retry observed
- [x] 3.3 Add/adjust test: successful reconnect fires `onReconnect` and not `onConnect`
- [x] 3.4 Confirm existing `tcpClose()` reconnect tests still pass

## 4. Docs and verification

- [x] 4.1 Document background reconnect + `retry` / `retryCount` / `retryInterval` (and note heartbeats are separate) in README briefly
- [x] 4.2 Run relevant `dart test` targets for connect/reconnect
- [x] 4.3 Manual acceptance note: airplane OFF→ON on Android emulator recovers to `connected` within a few seconds with `retryCount: -1` (checklist for app/fork QA; not automated)
