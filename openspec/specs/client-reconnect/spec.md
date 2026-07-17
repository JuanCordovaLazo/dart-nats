## Purpose

Defines how the NATS Dart client recovers from lost connectivity: background reconnect retries, disconnect detection, reconnect callbacks, cleanup bounds, and subscription re-bind after recovery.

## Requirements

### Requirement: Background reconnect retries after connect failures

After a client has successfully connected at least once with `retry` enabled, the client SHALL continue background reconnect attempts when the connection is lost, including when individual reconnect `Socket.connect` (or equivalent) attempts fail because the network is unavailable. The retry loop MUST NOT stop solely because status transitioned from `disconnected` to `reconnecting`. With `retryCount: -1`, the client MUST keep retrying until the connection succeeds or the client is closed / retry is disabled. With a finite `retryCount`, the client MUST retry up to the configured attempt budget (including server-pool multiplicity already used by the client) and then stop without remaining stuck in `reconnecting` forever.

#### Scenario: Repeated connect failures then recovery

- **WHEN** a previously connected client loses connectivity and reconnect connect attempts fail one or more times while offline
- **AND** the network (or server) becomes available again before retry is disabled or the client is closed
- **THEN** the client eventually completes the NATS handshake and reaches `Status.connected`

#### Scenario: Infinite retry does not abandon after first failure

- **WHEN** the client was connected with `retry: true` and `retryCount: -1`
- **AND** the first background reconnect connect attempt fails
- **THEN** the client schedules further reconnect attempts after `retryInterval` and does not leave a permanent no-retry `reconnecting` state

#### Scenario: Finite retry exhaustion leaves a terminal non-reconnecting state

- **WHEN** background reconnect exhausts a finite `retryCount` without success
- **THEN** the client stops the background reconnect loop
- **AND** status is not left indefinitely as `reconnecting` with no further action possible without an explicit new `connect()` / `close()` lifecycle

### Requirement: Reconnect delay applies while reconnecting

Between failed background reconnect attempts, the client MUST wait approximately `retryInterval` seconds before the next attempt whenever retries are still allowed, even if the current status is `Status.reconnecting`.

#### Scenario: Delay after failed attempt in reconnecting state

- **WHEN** a background reconnect attempt fails and further attempts are allowed
- **AND** status is `Status.reconnecting`
- **THEN** the client waits for the configured reconnect interval before the next attempt

### Requirement: Disconnect detection on TCP socket errors

When the active TCP connection reports a socket error, the client MUST transition toward reconnect (via `Status.disconnected` when retry applies) and MUST NOT rely solely on the socket `onDone` callback to detect loss of connectivity.

#### Scenario: TCP onError triggers disconnect path

- **WHEN** the active NATS TCP socket fires `onError` while the client believes it is connected
- **THEN** the client sets status to `disconnected` (subject to existing `closed` guards)
- **AND** if retry is enabled and the client was previously connected, background reconnect starts

### Requirement: onReconnect after successful recovery

When the client reaches `Status.connected` as the result of a background reconnect cycle (not the initial `connect()` success), the client MUST invoke `onReconnect` if set, and MUST NOT treat that transition as a first-time `onConnect` solely because the immediate previous status was `infoHandshake` or `tlsHandshake`.

#### Scenario: Successful reconnect fires onReconnect

- **WHEN** the client recovers via `_reconnectLoopBackground` (or equivalent) through handshake to `connected`
- **THEN** `onReconnect` is called if provided
- **AND** `onConnect` is not called for that same transition

#### Scenario: Initial connect still fires onConnect

- **WHEN** the client reaches `connected` for the first successful connection after `connect()` without an active background reconnect cycle
- **THEN** `onConnect` is called if provided

### Requirement: Reconnect cleanup must not block the retry loop indefinitely

Failed reconnect attempts MUST clean up socket resources in a way that cannot hang the retry loop indefinitely (for example after abrupt mobile network loss). Cleanup MAY soft-fail or time out; the loop MUST remain able to attempt the next connect.

#### Scenario: Cleanup after failed connect does not stall retries

- **WHEN** a reconnect connect attempt fails and socket cleanup is attempted
- **AND** underlying close would hang or error
- **THEN** the reconnect loop still proceeds to delay/retry (or exit per attempt budget) within a bounded time

### Requirement: Core subscriptions re-bind on reconnect success

On successful reconnect handshake completion, the client MUST re-bind existing client-side subscriptions to the server (same behavior as today’s `_backendSubscriptAll` on connect success). JetStream KV watch recreation remains outside this requirement.

#### Scenario: Pub/sub works after recovery

- **WHEN** the client had an active `sub()` before disconnect
- **AND** the client reaches `connected` again via background reconnect
- **THEN** subsequent publishes to that subject are received on the existing subscription stream without requiring the application to call `sub()` again
