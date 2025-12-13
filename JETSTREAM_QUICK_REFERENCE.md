# JetStream Quick Reference

## Quick Start

```dart
import 'package:dart_nats/dart_nats.dart';

final client = Client();
await client.connect(Uri.parse('nats://localhost:4222'));
final js = client.jetStream();
```

## Stream Operations

```dart
// Create stream
await js.manager.addStream(StreamConfig(
  name: 'EVENTS',
  subjects: ['events.>'],
  maxMsgs: 1000,
));

// Get info
final info = await js.manager.streamInfo('EVENTS');

// List streams
final streams = await js.manager.listStreams();

// Purge messages
await js.manager.purgeStream('EVENTS');

// Delete stream
await js.manager.deleteStream('EVENTS');
```

## Publishing

```dart
// Publish with ack
final ack = await js.publishString('events.test', 'Hello');

// With options
final ack = await js.publishString(
  'events.test',
  'Hello',
  options: JetStreamPublishOptions(
    msgId: 'unique-id',  // Deduplication
    timeout: Duration(seconds: 2),
  ),
);

// Binary data
final ack = await js.publish('events.binary', [1, 2, 3]);
```

## Pull Consumer

```dart
// Create pull consumer
final consumer = await js.pullSubscribe('EVENTS', ConsumerConfig(
  durable: 'WORKER',
  ackPolicy: AckPolicy.explicit,
));

// Fetch messages
final messages = await consumer.fetch(
  batch: 10,
  expires: Duration(seconds: 5),
);

// Process and acknowledge
for (var msg in messages) {
  print(msg.string);
  await js.ack(msg);
}
```

## Push Consumer

```dart
// Create push consumer
await js.manager.addConsumer('EVENTS', ConsumerConfig(
  durable: 'PUSH_WORKER',
  deliverSubject: 'events.deliver',
  ackPolicy: AckPolicy.explicit,
));

// Subscribe
final sub = js.subscribe('events.deliver');
await for (var msg in sub.stream) {
  print(msg.string);
  await js.ack(msg);
}
```

## Message Acknowledgment

```dart
// Positive acknowledgment
await js.ack(msg);

// Negative (redeliver)
await js.nak(msg);
await js.nak(msg, delay: Duration(seconds: 10));

// In progress (extend ack wait)
await js.inProgress(msg);

// Terminate (don't redeliver)
await js.term(msg);
```

## Message Metadata

```dart
final metadata = js.getMetadata(msg);
print('Stream: ${metadata.stream}');
print('Sequence: ${metadata.streamSeq}');
print('Delivered: ${metadata.delivered} times');
print('Pending: ${metadata.pending}');
```

## Consumer Management

```dart
// List consumers
final consumers = await js.manager.listConsumers('EVENTS');

// Get consumer info
final info = await js.manager.consumerInfo('EVENTS', 'WORKER');

// Delete consumer
await js.manager.deleteConsumer('EVENTS', 'WORKER');
```

## Configuration Options

### StreamConfig

```dart
StreamConfig(
  name: 'NAME',
  subjects: ['subject.>'],
  storage: StorageType.file,        // or .memory
  retention: RetentionPolicy.limits, // .workQueue, .interest
  maxMsgs: 1000,
  maxBytes: 1024 * 1024,
  maxAge: Duration(days: 7).inMicroseconds * 1000,
  discard: DiscardPolicy.old,        // or .new_
  numReplicas: 1,
)
```

### ConsumerConfig

```dart
ConsumerConfig(
  durable: 'NAME',
  deliverSubject: 'deliver.subject', // For push consumers
  ackPolicy: AckPolicy.explicit,     // .none, .all
  ackWait: Duration(seconds: 30).inMicroseconds * 1000,
  maxDeliver: 3,
  deliverPolicy: DeliverPolicy.all,  // .last, .new_, etc
  filterSubject: 'specific.subject',
  replayPolicy: ReplayPolicy.instant, // or .original
  maxAckPending: 10,
)
```

## Delivery Policies

- **DeliverAll**: Start from beginning
- **DeliverLast**: Only last message
- **DeliverLastPerSubject**: Last per subject
- **DeliverNew**: Only new messages
- **DeliverByStartSequence**: Start at sequence (set `optStartSeq`)
- **DeliverByStartTime**: Start at time (set `optStartTime`)

## Retention Policies

- **Limits**: Keep until limits hit (default)
- **WorkQueue**: Delete after ack
- **Interest**: Keep while consumers exist

## Example: Complete Workflow

```dart
final client = Client();
await client.connect(Uri.parse('nats://localhost:4222'));
final js = client.jetStream();

// Setup
await js.manager.addStream(StreamConfig(
  name: 'ORDERS',
  subjects: ['orders.>'],
));

// Publish
for (var i = 1; i <= 5; i++) {
  await js.publishString('orders.new', 'Order $i');
}

// Consume
final consumer = await js.pullSubscribe('ORDERS', ConsumerConfig(
  durable: 'PROCESSOR',
  ackPolicy: AckPolicy.explicit,
));

final messages = await consumer.fetch(batch: 5);
for (var msg in messages) {
  print('Processing: ${msg.string}');
  await js.ack(msg);
}

// Cleanup
await js.manager.deleteStream('ORDERS');
await client.close();
```

## Running with Docker

```bash
# Start NATS with JetStream
docker run -p 4222:4222 nats:latest -js

# Or with Docker Compose (from project root)
docker-compose up -d
```

## Documentation

- Full Guide: [docs/JETSTREAM.md](docs/JETSTREAM.md)
- Example: [example/jetstream_example.dart](example/jetstream_example.dart)
- Implementation: [JETSTREAM_IMPLEMENTATION.md](JETSTREAM_IMPLEMENTATION.md)
