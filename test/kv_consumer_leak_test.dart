import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

/// `KeyValue.watch()`, `.history()`, and `.keys()` each create an ephemeral
/// push consumer to receive updates, but discarded the `Consumer` object
/// `createConsumer()` returned and never deleted it once the caller
/// cancelled/finished -- every call left a server-side consumer behind
/// forever. Confirms all three now clean up after themselves.
///
/// Waits for the empty-consumer-list outcome by polling rather than a
/// single fixed sleep: deleteConsumer() lands asynchronously (fire-and-
/// forget in cleanup()), and `history()`/`keys()` can fall back to their
/// own pre-existing 5s timeoutTimer when their pending-count-reaches-zero
/// fast path doesn't fire, so a fixed short wait is flaky under load.
Future<void> expectEventuallyNoConsumers(JetStream js, String streamName,
    {Duration timeout = const Duration(seconds: 15)}) async {
  final deadline = DateTime.now().add(timeout);
  while (true) {
    final consumers = await js.listConsumers(streamName);
    if (consumers.isEmpty) return;
    if (DateTime.now().isAfter(deadline)) {
      fail('expected no consumers on $streamName, still have '
          '${consumers.map((c) => c.name).toList()} after $timeout');
    }
    await Future.delayed(const Duration(milliseconds: 250));
  }
}

void main() {
  group('KeyValue ephemeral consumer cleanup', () {
    late Client client;
    late JetStream js;
    late String bucket;
    late String streamName;

    setUp(() async {
      client = Client();
      await client.connect(Uri.parse('nats://localhost:4222'), retry: false);
      js = client.jetStream();
      bucket = 'kvleak${DateTime.now().microsecondsSinceEpoch}';
      streamName = 'KV_$bucket';
      await js.createKeyValue(
          KeyValueConfig(bucket: bucket, storage: 'memory', history: 5));
    });

    tearDown(() async {
      try {
        await js.deleteStream(streamName);
      } catch (_) {}
      await client.close();
    });

    test('watch() deletes its ephemeral consumer on cancel', () async {
      final kv = await js.keyValue(bucket);
      final sub = kv.watch().listen((_) {});
      // Let createConsumer() resolve before cancelling.
      await Future.delayed(const Duration(milliseconds: 300));

      final duringWatch = await js.listConsumers(streamName);
      expect(duringWatch, isNotEmpty);

      await sub.cancel();
      await expectEventuallyNoConsumers(js, streamName);
    });

    test('history() deletes its ephemeral consumer once exhausted', () async {
      final kv = await js.keyValue(bucket);
      await kv.putString('k', 'v1');
      await kv.putString('k', 'v2');

      final entries = await kv.history('k').toList();
      expect(entries.length, equals(2));

      await expectEventuallyNoConsumers(js, streamName);
    });

    test('keys() deletes its ephemeral consumer once done', () async {
      final kv = await js.keyValue(bucket);
      await kv.putString('a', '1');
      await kv.putString('b', '2');

      final keys = await kv.keys();
      expect(keys, containsAll(['a', 'b']));

      await expectEventuallyNoConsumers(js, streamName);
    });
  });
}
