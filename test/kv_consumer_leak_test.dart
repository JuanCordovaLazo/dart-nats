import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

/// `KeyValue.watch()`, `.history()`, and `.keys()` each create an ephemeral
/// push consumer to receive updates, but discarded the `Consumer` object
/// `createConsumer()` returned and never deleted it once the caller
/// cancelled/finished -- every call left a server-side consumer behind
/// forever. Confirms all three now clean up after themselves.
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
      // deleteConsumer() runs inside onCancel; give it a moment to land.
      await Future.delayed(const Duration(milliseconds: 300));

      final afterCancel = await js.listConsumers(streamName);
      expect(afterCancel, isEmpty);
    });

    test('history() deletes its ephemeral consumer once exhausted', () async {
      final kv = await js.keyValue(bucket);
      await kv.putString('k', 'v1');
      await kv.putString('k', 'v2');

      final entries = await kv.history('k').toList();
      expect(entries.length, equals(2));

      // history()'s pending-count-reaches-zero fast path isn't always
      // reliable (same as keys(), below), so cleanup() can fall back to
      // its pre-existing 5s timeoutTimer -- wait past that worst case
      // rather than assume the fast path fired.
      await Future.delayed(const Duration(seconds: 6));

      final consumers = await js.listConsumers(streamName);
      expect(consumers, isEmpty);
    });

    test('keys() deletes its ephemeral consumer once done', () async {
      final kv = await js.keyValue(bucket);
      await kv.putString('a', '1');
      await kv.putString('b', '2');

      final keys = await kv.keys();
      expect(keys, containsAll(['a', 'b']));

      // keys()'s own pending-count-reaches-zero fast path doesn't always
      // fire for a small result set, so cleanup() falls back to its
      // pre-existing 5s timeoutTimer -- unrelated to the consumer-leak fix
      // under test here, just something to wait past.
      await Future.delayed(const Duration(seconds: 6));

      final consumers = await js.listConsumers(streamName);
      expect(consumers, isEmpty);
    });
  });
}
