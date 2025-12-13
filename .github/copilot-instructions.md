# dart-nats AI Coding Instructions

## Project Overview

A Dart/Flutter client library for NATS messaging system supporting both TCP (`nats://`, `tls://`) and WebSocket (`ws://`, `wss://`) transports. Designed for cross-platform use including Flutter Web.

## Architecture

### Core Components

- **[lib/src/client.dart](lib/src/client.dart)**: Main `Client` class managing connections, pub/sub, and state machine
- **[lib/src/subscription.dart](lib/src/subscription.dart)**: `Subscription<T>` wraps streams with optional generic type deserialization
- **[lib/src/message.dart](lib/src/message.dart)**: `Message<T>` and `Header` for NATS messages with NATS/1.0 header support
- **[lib/src/nkeys.dart](lib/src/nkeys.dart)**: NKey authentication using ed25519 signatures
- **[lib/src/common.dart](lib/src/common.dart)**: `ConnectOption`, `Info`, exceptions, and NUID generation

### Connection State Machine

Client transitions through: `disconnected` → `connecting` → `infoHandshake` → `tlsHandshake` (if TLS) → `connected`. Supports automatic reconnection with configurable retry (see `_retry`, `retryCount`, `retryInterval`).

### Transport Layer

- TCP/TLS: Direct `Socket`/`SecureSocket` (mobile/desktop)
- WebSocket: `WebSocketChannel` from `web_socket_channel` (required for Flutter Web)
- Protocol parser in `_processOp()` handles NATS line protocol: `MSG`, `HMSG`, `PING`, `PONG`, `+OK`, `-ERR`

## Key Patterns

### Publishing

```dart
// Async with optional verbose acknowledgement
await client.pub('subject', Uint8List.fromList(data), buffer: false);
await client.pubString('subject', 'data', header: Header().add('key', 'val'));
```

The `buffer` parameter controls whether messages are queued during reconnects (`true`) or sent immediately (`false`).

### Subscribing with Generics

```dart
// Register global decoder once
client.registerJsonDecoder<Student>(json2Student);
var sub = client.sub<Student>('subject'); // Automatic deserialization
var msg = await sub.stream.first;
var student = msg.data; // Already typed as Student

// Or provide inline decoder
var sub = client.sub<Student>('subject', jsonDecoder: json2Student);
```

Always implement decoders as `T Function(String)` converting JSON strings to objects.

### Request-Reply

The client manages inbox subscriptions automatically with `_inboxSubPrefix` optimization:

```dart
var response = await client.request('service', data, timeout: Duration(seconds: 2));
// Or respond to requests:
serviceSub.stream.listen((msg) => msg.respondString('reply'));
```

### Authentication Models

Set credentials BEFORE calling `connect()`:

```dart
// Token/User-Pass via ConnectOption
connectOption: ConnectOption(authToken: 'token') // or user/pass

// NKey: Set seed on client, public key in ConnectOption
client.seed = 'SUAC...'; // Base32 encoded seed
connectOption: ConnectOption(nkey: 'UDXU...')

// JWT: Set seed and JWT
client.seed = 'SUAJ...';
connectOption: ConnectOption(jwt: 'eyJ0...')
```

## Testing

### Test Infrastructure

- Tests run against local NATS via Docker Compose ([docker-compose.yml](docker-compose.yml))
- Start services: `docker-compose up -d`
- Default test endpoint: `ws://localhost:8080` (WebSocket proxy) or `nats://localhost:4222`
- Authentication variants on ports 4223-4226 (JWT, token, user/pass, nkey)

### Test Structure

- Group tests with `group('all', () {})` wrapper
- Use `await client.connect(...)` at test start, `await client.close()` at end
- Test naming: `test('simple', ...)` or `test('0016 ws: description', ...)` for issue tracking

### Common Test Patterns

```dart
test('simple', () async {
  var client = Client();
  await client.connect(Uri.parse('ws://localhost:8080'));
  var sub = client.sub('subject');
  client.pub('subject', Uint8List.fromList('data'.codeUnits));
  var msg = await sub.stream.first;
  await client.close();
  expect(msg.string, equals('data'));
});
```

## Development Workflows

### Running Tests

```bash
dart test                          # All tests
dart test test/nats_client_test.dart  # Single file
docker-compose up -d              # Start NATS servers first
```

### Running Examples

```bash
dart example/main.dart            # Basic pub/sub example
```

### Linting

Uses `lints: ^2.0.1` with custom rules in [analysis_options.yaml](analysis_options.yaml):

- `public_member_api_docs`: All public APIs require doc comments
- `unrelated_type_equality_checks`: Type-safe comparisons only

## Critical Implementation Details

### Message Parsing

`_processMsg()` and `_processHMsg()` parse line protocol. Headers use `\r\n` delimiters with format `NATS/1.0\r\nkey:value\r\n`.

### Subscription Management

- Client maintains `_subs` map by SID (subscription ID)
- `_backendSubs` tracks server-side subscriptions for reconnect replay
- Always call `client.unSub(sub)` or `sub.unSub()` to clean up

### Reconnection Logic

When `retry: true` and `retryCount: -1`, client loops indefinitely retrying connections via `statusStream` monitoring. On reconnect:

1. Resubscribe all `_backendSubs`
2. Flush buffered publications from `_pubBuffer`

### Mutex for Requests

The `request()` method uses `_mutex.acquire()`/`release()` to serialize concurrent requests through shared inbox subscription.

## Common Pitfalls

1. **Don't mutate `inboxPrefix` after connection**: Throws `NatsException`
2. **WebSocket-only on Flutter Web**: TCP sockets unavailable in browser
3. **Message buffering**: Set `buffer: false` in `pub()` for real-time critical messages
4. **Type safety**: Always register or provide `jsonDecoder` when using generic subscriptions
5. **Connection status**: Check `client.connected` or listen to `statusStream` before publishing

## Additional Resources

- NATS Protocol: https://docs.nats.io/reference/reference-protocols/nats-protocol
- Examples: [example/main.dart](example/main.dart), [test/structure_test.dart](test/structure_test.dart)
