import 'dart:typed_data';
import 'package:test/test.dart';
import 'package:dart_nats/dart_nats.dart';

void main() {
  group('JetStream API Models', () {
    test('StreamConfig serialization', () {
      final config = StreamConfig(
        name: 'TEST',
        subjects: ['test.>'],
        storage: StorageType.file,
        retention: RetentionPolicy.limits,
        maxMsgs: 100,
      );

      final json = config.toJson();
      expect(json['name'], equals('TEST'));
      expect(json['subjects'], equals(['test.>']));
      expect(json['storage'], equals('file'));
      expect(json['max_msgs'], equals(100));
    });

    test('StreamConfig deserialization', () {
      final json = {
        'name': 'TEST',
        'subjects': ['test.>'],
        'storage': 'file',
        'retention': 'limits',
        'max_msgs': 100,
      };

      final config = StreamConfig.fromJson(json);
      expect(config.name, equals('TEST'));
      expect(config.subjects, equals(['test.>']));
      expect(config.storage, equals(StorageType.file));
      expect(config.maxMsgs, equals(100));
    });

    test('ConsumerConfig serialization', () {
      final config = ConsumerConfig(
        durable: 'MY_CONSUMER',
        ackPolicy: AckPolicy.explicit,
        deliverPolicy: DeliverPolicy.all,
        maxDeliver: 3,
      );

      final json = config.toJson();
      expect(json['durable_name'], equals('MY_CONSUMER'));
      expect(json['ack_policy'], equals('explicit'));
      expect(json['deliver_policy'], equals('all'));
      expect(json['max_deliver'], equals(3));
    });

    test('PubAck deserialization', () {
      final json = {
        'stream': 'TEST',
        'seq': 42,
        'duplicate': true,
      };

      final ack = PubAck.fromJson(json);
      expect(ack.stream, equals('TEST'));
      expect(ack.seq, equals(42));
      expect(ack.duplicate, isTrue);
    });

    test('JetStreamError deserialization', () {
      final json = {
        'code': 500,
        'description': 'stream not found',
        'err_code': 10059,
      };

      final error = JetStreamError.fromJson(json);
      expect(error.code, equals(500));
      expect(error.description, equals('stream not found'));
      expect(error.errCode, equals(10059));
    });

    test('PullSubscribeOptions serialization', () {
      final options = PullSubscribeOptions(
        batch: 10,
        expires: Duration(seconds: 5),
        noWait: true,
      );

      final json = options.toJson();
      expect(json['batch'], equals(10));
      expect(json['expires'], equals(5000000000)); // 5 seconds in nanoseconds
      expect(json['no_wait'], isTrue);
    });

    test('JetStreamPublishOptions with msgId', () {
      final options = JetStreamPublishOptions(
        msgId: 'msg-123',
        expectedLastSeq: 10,
        timeout: Duration(seconds: 2),
      );

      expect(options.msgId, equals('msg-123'));
      expect(options.expectedLastSeq, equals(10));
      expect(options.timeout, equals(Duration(seconds: 2)));
    });
  });

  group('JetStream Message Metadata', () {
    test('Parse JetStream metadata from reply subject', () {
      final client = Client();
      final msg = Message(
        'orders.new',
        1,
        Uint8List.fromList([1, 2, 3]),
        client,
        replyTo: r'$JS.ACK.ORDERS.MY_CONSUMER.1.42.10.1234567890.5',
      );

      final metadata = JetStreamMessageMetadata.fromMessage(msg);
      expect(metadata.stream, equals('ORDERS'));
      expect(metadata.consumer, equals('MY_CONSUMER'));
      expect(metadata.delivered, equals(1));
      expect(metadata.streamSeq, equals(42));
      expect(metadata.consumerSeq, equals(10));
      expect(metadata.timestamp, equals('1234567890'));
      expect(metadata.pending, equals(5));
    });

    test('Invalid metadata throws exception', () {
      final client = Client();
      final msg = Message(
        'test',
        1,
        Uint8List.fromList([]),
        client,
        replyTo: 'invalid.reply.subject',
      );

      expect(
        () => JetStreamMessageMetadata.fromMessage(msg),
        throwsException,
      );
    });

    test('Missing reply subject throws exception', () {
      final client = Client();
      final msg = Message(
        'test',
        1,
        Uint8List.fromList([]),
        client,
        replyTo: null,
      );

      expect(
        () => JetStreamMessageMetadata.fromMessage(msg),
        throwsException,
      );
    });
  });

  group('JetStream Enums', () {
    test('StorageType values', () {
      expect(StorageType.file.toString(), contains('file'));
      expect(StorageType.memory.toString(), contains('memory'));
    });

    test('RetentionPolicy values', () {
      expect(RetentionPolicy.limits.toString(), contains('limits'));
      expect(RetentionPolicy.workQueue.toString(), contains('workQueue'));
      expect(RetentionPolicy.interest.toString(), contains('interest'));
    });

    test('AckPolicy values', () {
      expect(AckPolicy.none.toString(), contains('none'));
      expect(AckPolicy.all.toString(), contains('all'));
      expect(AckPolicy.explicit.toString(), contains('explicit'));
    });

    test('DeliverPolicy values', () {
      expect(DeliverPolicy.all.toString(), contains('all'));
      expect(DeliverPolicy.last.toString(), contains('last'));
      expect(DeliverPolicy.new_.toString(), contains('new_'));
    });
  });
}
