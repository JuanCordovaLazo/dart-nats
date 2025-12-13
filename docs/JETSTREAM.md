# JetStream Support

This document describes the JetStream support in dart-nats.

## What is JetStream?

JetStream is NATS's built-in persistence engine that provides:

- **Message Persistence**: Messages are stored and can be replayed
- **At-least-once Delivery**: Guaranteed message delivery with acknowledgments
- **Stream Replay**: Replay messages from any point in time or sequence
- **Consumer Management**: Multiple consumers with different delivery policies
- **Key-Value Store**: Built-in key-value storage
- **Object Store**: File storage and transfer capabilities

## Getting Started

### Creating a JetStream Context

```dart
import 'package:dart_nats/dart_nats.dart';

final client = Client();
await client.connect(Uri.parse('nats://localhost:4222'));

// Get JetStream context
final js = client.jetStream();
```

## Stream Management

### Creating a Stream

A stream stores messages on one or more subjects:

```dart
final streamConfig = StreamConfig(
  name: 'ORDERS',
  subjects: ['orders.>'],
  storage: StorageType.file,
  retention: RetentionPolicy.limits,
  maxMsgs: 1000,
  maxBytes: 1024 * 1024, // 1MB
  maxAge: Duration(hours: 24).inNanoseconds,
  discard: DiscardPolicy.old,
  numReplicas: 1,
);

final streamInfo = await js.manager.addStream(streamConfig);
print('Created stream: ${streamInfo.config.name}');
```

### Stream Configuration Options

- **name**: Stream name (required)
- **subjects**: List of subjects the stream captures
- **storage**: `StorageType.file` or `StorageType.memory`
- **retention**: `RetentionPolicy.limits`, `RetentionPolicy.workQueue`, or `RetentionPolicy.interest`
- **maxMsgs**: Maximum number of messages
- **maxBytes**: Maximum total bytes
- **maxAge**: Maximum age of messages (in nanoseconds)
- **maxMsgSize**: Maximum size of individual messages
- **discard**: `DiscardPolicy.old` or `DiscardPolicy.new_`
- **numReplicas**: Number of replicas for fault tolerance (1-5)

### Getting Stream Information

```dart
final info = await js.manager.streamInfo('ORDERS');
print('Messages: ${info.state.messages}');
print('Bytes: ${info.state.bytes}');
print('Consumers: ${info.state.consumers}');
```

### Listing Streams

```dart
final streams = await js.manager.listStreams();
print('Available streams: ${streams.join(", ")}');
```

### Purging a Stream

```dart
await js.manager.purgeStream('ORDERS');
```

### Deleting a Stream

```dart
await js.manager.deleteStream('ORDERS');
```

## Publishing Messages

### Basic Publishing

Publishing to JetStream returns an acknowledgment:

```dart
final ack = await js.publishString('orders.new', 'Order data');
print('Published: seq=${ack.seq}, duplicate=${ack.duplicate}');
```

### Publishing with Options

```dart
final ack = await js.publishString(
  'orders.new',
  'Order data',
  options: JetStreamPublishOptions(
    msgId: 'unique-msg-id',  // For deduplication
    expectedLastSeq: 10,      // Expected last sequence
    timeout: Duration(seconds: 2),
  ),
);
```

### Publishing Binary Data

```dart
final data = Uint8List.fromList([1, 2, 3, 4]);
final ack = await js.publish('orders.binary', data);
```

### Publishing with Headers

```dart
final header = Header()
  ..add('Content-Type', 'application/json')
  ..add('User-ID', '12345');

final ack = await js.publishString(
  'orders.new',
  jsonEncode({'orderId': 123}),
  header: header,
);
```

## Consumers

Consumers are views on a stream that track which messages have been delivered and acknowledged.

### Pull Consumers

Pull consumers allow your application to request messages in batches:

```dart
// Create a pull consumer
final consumerConfig = ConsumerConfig(
  durable: 'MY_CONSUMER',
  ackPolicy: AckPolicy.explicit,
  deliverPolicy: DeliverPolicy.all,
  maxDeliver: 3,
  ackWait: Duration(seconds: 30).inNanoseconds,
);

final pullSub = await js.pullSubscribe('ORDERS', consumerConfig);

// Fetch messages
final messages = await pullSub.fetch(
  batch: 10,
  expires: Duration(seconds: 5),
);

for (var msg in messages) {
  print('Received: ${msg.string}');

  // Process the message
  // ...

  // Acknowledge
  await js.ack(msg);
}
```

### Push Consumers

Push consumers deliver messages to a specified subject:

```dart
// Create a push consumer
final consumerConfig = ConsumerConfig(
  durable: 'PUSH_CONSUMER',
  deliverSubject: 'orders.deliver',
  ackPolicy: AckPolicy.explicit,
  deliverPolicy: DeliverPolicy.all,
  maxAckPending: 10,
);

await js.manager.addConsumer('ORDERS', consumerConfig);

// Subscribe to delivery subject
final sub = js.subscribe('orders.deliver');

await for (var msg in sub.stream) {
  print('Received: ${msg.string}');

  // Process and acknowledge
  await js.ack(msg);
}
```

#### Real-Time Chat Example (Push Consumer)

Here's a real-world example using push consumers for a chat application:

```dart
// ✅ Push Consumer - Real-Time Event Stream
final consumerConfig = ConsumerConfig(
  durable: 'user_${userId}_friends',
  deliverSubject: 'chat.friends.user.$userId',  // Where to push messages
  ackPolicy: AckPolicy.explicit,
  deliverPolicy: DeliverPolicy.all,
  maxAckPending: 100,  // Buffer up to 100 unacked messages
);

await js.manager.addConsumer('CHAT_FRIENDS', consumerConfig);

// Subscribe once - messages arrive automatically!
final sub = js.subscribe<ChatMessage>('chat.friends.user.$userId');

// Listen to the stream - this is reactive and efficient
sub.stream.listen(
  (msg) {
    // Message arrives INSTANTLY when published
    displayMessageInUI(msg.data);
    js.ack(msg);  // Acknowledge receipt
  },
  onError: (e) => print('Error: $e'),
  onDone: () => print('Stream closed'),
);

// That's it! No polling loop needed.
```

### Consumer Configuration Options

- **durable**: Consumer name (makes it durable)
- **deliverSubject**: Delivery subject for push consumers
- **ackPolicy**: `AckPolicy.explicit`, `AckPolicy.all`, or `AckPolicy.none`
- **ackWait**: Time to wait for acknowledgment (in nanoseconds)
- **maxDeliver**: Maximum delivery attempts
- **deliverPolicy**: `DeliverPolicy.all`, `DeliverPolicy.last`, `DeliverPolicy.new_`, etc.
- **filterSubject**: Filter messages by subject
- **replayPolicy**: `ReplayPolicy.instant` or `ReplayPolicy.original`
- **maxAckPending**: Maximum outstanding unacknowledged messages

## Message Acknowledgment

### Explicit Acknowledgment

```dart
await js.ack(msg);
```

### Negative Acknowledgment (Redeliver)

```dart
await js.nak(msg);

// With delay
await js.nak(msg, delay: Duration(seconds: 10));
```

### In Progress (Extend Ack Wait)

```dart
await js.inProgress(msg);
```

### Terminate (Don't Redeliver)

```dart
await js.term(msg);
```

## Message Metadata

Get metadata about JetStream messages:

```dart
final metadata = js.getMetadata(msg);
print('Stream: ${metadata.stream}');
print('Consumer: ${metadata.consumer}');
print('Stream sequence: ${metadata.streamSeq}');
print('Consumer sequence: ${metadata.consumerSeq}');
print('Delivered: ${metadata.delivered} times');
print('Pending: ${metadata.pending}');
```

## Consumer Management

### Listing Consumers

```dart
final consumers = await js.manager.listConsumers('ORDERS');
print('Consumers: ${consumers.join(", ")}');
```

### Getting Consumer Info

```dart
final info = await js.manager.consumerInfo('ORDERS', 'MY_CONSUMER');
print('Pending: ${info.numPending}');
print('Redelivered: ${info.numRedelivered}');
print('Waiting: ${info.numWaiting}');
```

### Deleting a Consumer

```dart
await js.manager.deleteConsumer('ORDERS', 'MY_CONSUMER');
```

## Message Operations

### Get Message by Sequence

```dart
final msg = await js.manager.getMessage('ORDERS', 42);
print('Message: ${msg.string}');
```

### Delete Message

```dart
await js.manager.deleteMessage('ORDERS', 42);
```

## Delivery Policies

- **DeliverAll**: Deliver all messages from the beginning
- **DeliverLast**: Deliver only the last message
- **DeliverLastPerSubject**: Deliver last message per subject
- **DeliverNew**: Deliver only new messages (published after consumer creation)
- **DeliverByStartSequence**: Start at a specific sequence number
- **DeliverByStartTime**: Start at a specific time

Example:

```dart
// Start from sequence 100
final config = ConsumerConfig(
  durable: 'REPLAY_CONSUMER',
  deliverPolicy: DeliverPolicy.byStartSequence,
  optStartSeq: 100,
);

// Start from a specific time
final config2 = ConsumerConfig(
  durable: 'TIME_CONSUMER',
  deliverPolicy: DeliverPolicy.byStartTime,
  optStartTime: DateTime.now().subtract(Duration(hours: 1)).toIso8601String(),
);
```

## Retention Policies

### Limits (Default)

Messages are retained based on limits (max msgs, bytes, age):

```dart
final config = StreamConfig(
  name: 'EVENTS',
  retention: RetentionPolicy.limits,
  maxMsgs: 1000,
  maxAge: Duration(days: 7).inNanoseconds,
);
```

### Work Queue

Messages are deleted after being acknowledged:

```dart
final config = StreamConfig(
  name: 'JOBS',
  retention: RetentionPolicy.workQueue,
  subjects: ['jobs.>'],
);
```

### Interest

Messages are kept only while there are active consumers:

```dart
final config = StreamConfig(
  name: 'TEMP',
  retention: RetentionPolicy.interest,
  subjects: ['temp.>'],
);
```

## Best Practices

1. **Use Durable Consumers**: Set a `durable` name to persist consumer state
2. **Set Appropriate Timeouts**: Configure `ackWait` based on your processing time
3. **Handle Redeliveries**: Use `maxDeliver` to limit redelivery attempts
4. **Use Message IDs**: Set `msgId` for deduplication of published messages
5. **Monitor Consumer Lag**: Check `numPending` to identify processing delays
6. **Clean Up**: Delete unused consumers and streams
7. **Choose Storage Wisely**: Use `file` storage for persistence, `memory` for temporary data
8. **Set Limits**: Configure `maxMsgs`, `maxBytes`, and `maxAge` to prevent unbounded growth

## Error Handling

```dart
try {
  final ack = await js.publishString('orders.new', 'data');
  print('Published: ${ack.seq}');
} catch (e) {
  print('Publish failed: $e');
}

try {
  final info = await js.manager.streamInfo('ORDERS');
  print('Stream exists with ${info.state.messages} messages');
} catch (e) {
  print('Stream not found or error: $e');
}
```

## Examples

See [example/jetstream_example.dart](../example/jetstream_example.dart) for a complete working example demonstrating:

- Stream creation and configuration
- Publishing messages with acknowledgments
- Pull consumer with batch fetching
- Push consumer with real-time delivery
- Message acknowledgment patterns
- Consumer and stream management
- Cleanup operations

## Running the Example

1. Start NATS with JetStream enabled:

   ```bash
   docker run -p 4222:4222 nats:latest -js
   ```

2. Run the example:
   ```bash
   dart run example/jetstream_example.dart
   ```

## Further Reading

- [Official JetStream Documentation](https://docs.nats.io/nats-concepts/jetstream)
- [JetStream Streams](https://docs.nats.io/nats-concepts/jetstream/streams)
- [JetStream Consumers](https://docs.nats.io/nats-concepts/jetstream/consumers)
- [NATS by Example - JetStream](https://natsbyexample.com/)
