import 'package:dart_nats/dart_nats.dart';

void main() async {
  // Connect to NATS server with JetStream enabled
  final client = Client();
  await client.connect(Uri.parse('nats://localhost:4228'));

  print('Connected to NATS');

  // Get JetStream context
  final js = client.jetStream();

  try {
    // === Stream Management ===
    print('\n=== Creating Stream ===');

    // Create a stream
    final streamConfig = StreamConfig(
      name: 'ORDERS',
      subjects: ['orders.>'],
      storage: StorageType.file,
      retention: RetentionPolicy.limits,
      maxMsgs: 1000,
      maxBytes: 1024 * 1024, // 1MB
      maxAge:
          Duration(hours: 24).inMicroseconds * 1000, // Convert to nanoseconds
      discard: DiscardPolicy.old,
      numReplicas: 1,
    );

    final streamInfo = await js.manager.addStream(streamConfig);
    print('Created stream: ${streamInfo.config.name}');
    print('  Subjects: ${streamInfo.config.subjects}');
    print('  Messages: ${streamInfo.state.messages}');

    // === Publishing ===
    print('\n=== Publishing Messages ===');

    // Publish messages to JetStream with acknowledgment
    for (var i = 1; i <= 5; i++) {
      final ack = await js.publishString(
        'orders.new',
        'Order $i',
        options: JetStreamPublishOptions(
          msgId: 'order-$i', // For deduplication
          timeout: Duration(seconds: 2),
        ),
      );
      print('Published message $i: seq=${ack.seq}, duplicate=${ack.duplicate}');
    }

    // Get stream info to see message count
    final updatedInfo = await js.manager.streamInfo('ORDERS');
    print('Stream now has ${updatedInfo.state.messages} messages');

    // === Pull Consumer ===
    print('\n=== Pull Consumer ===');

    // Create a pull consumer
    final pullConsumerConfig = ConsumerConfig(
      durable: 'ORDERS_PULL_CONSUMER',
      ackPolicy: AckPolicy.explicit,
      deliverPolicy: DeliverPolicy.all,
      maxDeliver: 3,
      ackWait:
          Duration(seconds: 30).inMicroseconds * 1000, // Convert to nanoseconds
    );

    final pullSub = await js.pullSubscribe(
      'ORDERS',
      pullConsumerConfig,
    );

    print('Created pull consumer: ${pullConsumerConfig.durable}');

    // Fetch messages in batches
    final messages = await pullSub.fetch(
      batch: 3,
      expires: Duration(seconds: 5),
    );

    print('Fetched ${messages.length} messages:');
    for (var msg in messages) {
      final metadata = js.getMetadata(msg);
      print('  - ${msg.string}');
      print(
          '    Stream seq: ${metadata.streamSeq}, Consumer seq: ${metadata.consumerSeq}');
      print('    Delivered: ${metadata.delivered} times');

      // Acknowledge the message
      await js.ack(msg);
      print('    Acknowledged');
    }

    // === Push Consumer ===
    print('\n=== Push Consumer ===');

    // Create a push consumer
    final pushConsumerConfig = ConsumerConfig(
      durable: 'ORDERS_PUSH_CONSUMER',
      deliverSubject:
          'deliver.orders', // Use a different subject to avoid cycle
      ackPolicy: AckPolicy.explicit,
      deliverPolicy: DeliverPolicy.all,
      maxAckPending: 10,
    );

    await js.manager.addConsumer('ORDERS', pushConsumerConfig);
    print('Created push consumer: ${pushConsumerConfig.durable}');

    // Subscribe to the delivery subject
    final pushSub = js.subscribe('deliver.orders');

    // Process messages from push consumer
    var count = 0;
    await for (var msg in pushSub.stream) {
      final metadata = js.getMetadata(msg);
      print('Received: ${msg.string}');
      print('  Stream seq: ${metadata.streamSeq}');

      // Acknowledge the message
      await js.ack(msg);
      print('  Acknowledged');

      count++;
      if (count >= 2) {
        // Just process 2 messages for demo
        pushSub.unSub();
        break;
      }
    }

    // === Working with Message Metadata ===
    print('\n=== Message Operations ===');

    // Publish another message
    final ack =
        await js.publishString('orders.completed', 'Order 100 completed');
    print('Published message with seq: ${ack.seq}');

    // Get message by sequence
    final retrievedMsg = await js.manager.getMessage('ORDERS', ack.seq);
    print('Retrieved message: ${retrievedMsg.string}');

    // === Consumer Management ===
    print('\n=== Consumer Management ===');

    final consumers = await js.manager.listConsumers('ORDERS');
    print('Consumers on ORDERS stream: ${consumers.join(", ")}');

    // Get consumer info
    for (var consumerName in consumers) {
      final info = await js.manager.consumerInfo('ORDERS', consumerName);
      print('  $consumerName:');
      print('    Pending: ${info.numPending}');
      print('    Redelivered: ${info.numRedelivered}');
    }

    // === Stream Information ===
    print('\n=== Stream Information ===');

    final streams = await js.manager.listStreams();
    print('Available streams: ${streams.join(", ")}');

    // === Cleanup ===
    print('\n=== Cleanup ===');

    // Delete consumers
    for (var consumer in consumers) {
      await js.manager.deleteConsumer('ORDERS', consumer);
      print('Deleted consumer: $consumer');
    }

    // Purge stream
    await js.manager.purgeStream('ORDERS');
    print('Purged stream ORDERS');

    // Delete stream
    await js.manager.deleteStream('ORDERS');
    print('Deleted stream ORDERS');
  } catch (e) {
    print('Error: $e');
  } finally {
    // Close connection
    await client.close();
    print('\nConnection closed');
  }
}
