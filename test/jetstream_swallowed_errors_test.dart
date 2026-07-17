import 'dart:async';

import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

/// Both `Consumer.messages()`'s pull loop and `OrderedConsumer._start()`
/// used to swallow every failure into a silent 1-second retry loop, never
/// calling `addError` on the stream they hand back. A caller had no way to
/// know delivery had permanently broken (e.g. the server-side consumer was
/// deleted out from under a pull consumer, or an OrderedConsumer's hardcoded
/// no-ack config is rejected by a workqueue-retention stream) short of
/// polling consumer metadata themselves. These tests confirm both surface
/// the failure via the stream's error channel instead.
void main() {
  group('JetStream swallowed errors', () {
    late Client client;
    late JetStream js;
    final prefix = 'swerr-${DateTime.now().microsecondsSinceEpoch}';

    setUp(() async {
      client = Client();
      await client.connect(Uri.parse('nats://localhost:4222'), retry: false);
      js = client.jetStream();
    });

    tearDown(() async {
      await client.close();
    });

    test(
        'Consumer.messages() surfaces an error after the server-side '
        'consumer is deleted out from under it', () async {
      final streamName = '$prefix-pull';
      final consumerName = 'c1';
      await js.createStream(StreamConfig(
        name: streamName,
        subjects: ['$streamName.>'],
        storage: 'memory',
      ));
      await js.createConsumer(
          streamName, ConsumerConfig(durable: consumerName, ackPolicy: 'none'));

      final consumer = js.consumer(streamName, consumerName);
      final errorCompleter = Completer<Object>();
      final sub = consumer.messages(timeout: const Duration(seconds: 2)).listen(
        (_) {},
        onError: (Object e) {
          if (!errorCompleter.isCompleted) errorCompleter.complete(e);
        },
      );

      // Give the pull loop a moment to start, then delete the consumer
      // server-side -- every subsequent fetch() must fail.
      await Future.delayed(const Duration(milliseconds: 300));
      await js.deleteConsumer(streamName, consumerName);

      final error =
          await errorCompleter.future.timeout(const Duration(seconds: 10));
      expect(error, isNotNull);

      await sub.cancel();
      await js.deleteStream(streamName);
    });

    test(
        'OrderedConsumer surfaces createConsumer rejection on a '
        'workqueue-retention stream instead of retrying silently', () async {
      final streamName = '$prefix-wq';
      await js.createStream(StreamConfig(
        name: streamName,
        subjects: ['$streamName.>'],
        storage: 'memory',
        retention: 'workqueue',
      ));

      final oc = js.orderedConsumer(streamName, OrderedConsumerConfig());
      final errorCompleter = Completer<Object>();
      final sub = oc.messages().listen(
        (_) {},
        onError: (Object e) {
          if (!errorCompleter.isCompleted) errorCompleter.complete(e);
        },
      );

      final error =
          await errorCompleter.future.timeout(const Duration(seconds: 5));
      expect(error, isNotNull);

      await sub.cancel();
      oc.stop();
      await js.deleteStream(streamName);
    });
  });
}
