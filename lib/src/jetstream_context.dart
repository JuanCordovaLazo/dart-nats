import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'client.dart';
import 'inbox.dart';
import 'jetstream.dart';
import 'jetstream_manager.dart';
import 'message.dart';
import 'subscription.dart';

/// Options for JetStream publish
class JetStreamPublishOptions {
  /// Message ID for deduplication
  final String? msgId;

  /// Expected last message ID
  final String? expectedLastMsgId;

  /// Expected last sequence
  final int? expectedLastSeq;

  /// Expected last subject sequence
  final int? expectedLastSubjectSeq;

  /// Expected stream name
  final String? expectedStream;

  /// Timeout for publish acknowledgment
  final Duration timeout;

  /// Creates JetStream publish options
  JetStreamPublishOptions({
    this.msgId,
    this.expectedLastMsgId,
    this.expectedLastSeq,
    this.expectedLastSubjectSeq,
    this.expectedStream,
    this.timeout = const Duration(seconds: 5),
  });
}

/// Options for pull subscribe
class PullSubscribeOptions {
  /// Batch size for pull requests
  final int batch;

  /// Maximum time to wait for messages
  final Duration? expires;

  /// No wait (return immediately if no messages)
  final bool noWait;

  /// Idle heartbeat interval
  final Duration? idleHeartbeat;

  /// Creates pull subscribe options
  PullSubscribeOptions({
    this.batch = 1,
    this.expires,
    this.noWait = false,
    this.idleHeartbeat,
  });

  /// Converts options to JSON
  Map<String, dynamic> toJson() {
    final json = <String, dynamic>{
      'batch': batch,
    };

    if (expires != null) {
      json['expires'] =
          expires!.inMicroseconds * 1000; // Convert to nanoseconds
    }
    if (noWait) {
      json['no_wait'] = noWait;
    }
    if (idleHeartbeat != null) {
      json['idle_heartbeat'] =
          idleHeartbeat!.inMicroseconds * 1000; // Convert to nanoseconds
    }

    return json;
  }
}

/// JetStream message metadata
class JetStreamMessageMetadata {
  /// Stream name
  final String stream;

  /// Consumer name
  final String consumer;

  /// Sequence number in stream
  final int streamSeq;

  /// Sequence number delivered to consumer
  final int consumerSeq;

  /// Number of times delivered
  final int delivered;

  /// Timestamp
  final String timestamp;

  /// Number of pending messages
  final int? pending;

  /// Creates JetStream message metadata
  JetStreamMessageMetadata({
    required this.stream,
    required this.consumer,
    required this.streamSeq,
    required this.consumerSeq,
    required this.delivered,
    required this.timestamp,
    this.pending,
  });

  /// Creates metadata from a JetStream message
  factory JetStreamMessageMetadata.fromMessage(Message msg) {
    if (msg.replyTo == null) {
      throw Exception('Message does not have JetStream metadata');
    }

    // Parse reply subject: $JS.ACK.<stream>.<consumer>.<delivered>.<stream_seq>.<consumer_seq>.<timestamp>.<pending>
    final parts = msg.replyTo!.split('.');
    if (parts.length < 9 || parts[0] != r'$JS' || parts[1] != 'ACK') {
      throw Exception('Invalid JetStream reply subject');
    }

    return JetStreamMessageMetadata(
      stream: parts[2],
      consumer: parts[3],
      delivered: int.parse(parts[4]),
      streamSeq: int.parse(parts[5]),
      consumerSeq: int.parse(parts[6]),
      timestamp: parts[7],
      pending: parts.length > 8 ? int.tryParse(parts[8]) : null,
    );
  }
}

/// JetStream Context for publishing and consuming messages
class JetStreamContext {
  final Client _client;
  final JetStreamManager _manager;
  final String _apiPrefix;

  /// Creates a JetStream context
  JetStreamContext(
    this._client, {
    String apiPrefix = r'$JS.API',
    Duration timeout = const Duration(seconds: 5),
  })  : _apiPrefix = apiPrefix,
        _manager =
            JetStreamManager(_client, apiPrefix: apiPrefix, timeout: timeout);

  /// Get the JetStream manager for stream/consumer management
  JetStreamManager get manager => _manager;

  /// Publish a message to JetStream
  Future<PubAck> publish(
    String subject,
    List<int> data, {
    Header? header,
    JetStreamPublishOptions? options,
  }) async {
    options ??= JetStreamPublishOptions();

    // Add JetStream headers
    final jsHeader = header ?? Header();

    if (options.msgId != null) {
      jsHeader.add('Nats-Msg-Id', options.msgId!);
    }
    if (options.expectedLastMsgId != null) {
      jsHeader.add('Nats-Expected-Last-Msg-Id', options.expectedLastMsgId!);
    }
    if (options.expectedLastSeq != null) {
      jsHeader.add(
          'Nats-Expected-Last-Sequence', options.expectedLastSeq.toString());
    }
    if (options.expectedLastSubjectSeq != null) {
      jsHeader.add('Nats-Expected-Last-Subject-Sequence',
          options.expectedLastSubjectSeq.toString());
    }
    if (options.expectedStream != null) {
      jsHeader.add('Nats-Expected-Stream', options.expectedStream!);
    }

    // Publish and wait for ack
    final inbox = newInbox(inboxPrefix: '_INBOX');
    final sub = _client.sub(inbox);

    try {
      _client.pub(
        subject,
        Uint8List.fromList(data),
        replyTo: inbox,
        header: jsHeader,
      );

      final response = await sub.stream.first.timeout(options.timeout);
      sub.unSub();

      final json = jsonDecode(response.string) as Map<String, dynamic>;

      // Check for errors
      if (json['error'] != null) {
        final error = JetStreamError.fromJson(json['error']);
        throw Exception(error.toString());
      }

      return PubAck.fromJson(json);
    } catch (e) {
      sub.unSub();
      rethrow;
    }
  }

  /// Publish a string message to JetStream
  Future<PubAck> publishString(
    String subject,
    String data, {
    Header? header,
    JetStreamPublishOptions? options,
  }) async {
    return publish(subject, utf8.encode(data),
        header: header, options: options);
  }

  /// Subscribe to a JetStream consumer (push consumer)
  Subscription<T> subscribe<T>(
    String subject, {
    String? queueGroup,
    ConsumerConfig? consumerConfig,
    T Function(String)? jsonDecoder,
  }) {
    // For push consumers, we subscribe to the delivery subject
    return _client.sub<T>(
      subject,
      queueGroup: queueGroup,
      jsonDecoder: jsonDecoder,
    );
  }

  /// Create a pull-based subscription
  /// Returns a subscription that can be used with fetch() to pull messages
  Future<PullSubscription<T>> pullSubscribe<T>(
    String streamName,
    ConsumerConfig consumerConfig, {
    T Function(String)? jsonDecoder,
  }) async {
    // Ensure it's not a push consumer
    if (consumerConfig.deliverSubject != null) {
      throw Exception(
          'Pull subscription cannot have a delivery subject (push consumer)');
    }

    // Create or get the consumer
    final consumerInfo = await _manager.addConsumer(streamName, consumerConfig);

    return PullSubscription<T>(
      _client,
      streamName,
      consumerInfo.name,
      _apiPrefix,
      jsonDecoder: jsonDecoder,
    );
  }

  /// Acknowledge a JetStream message
  Future<void> ack(Message msg) async {
    if (msg.replyTo == null) {
      throw Exception(
          'Message does not have a reply subject for acknowledgment');
    }
    await _client.pub(msg.replyTo!, Uint8List(0));
  }

  /// Negatively acknowledge a message (will be redelivered)
  Future<void> nak(Message msg, {Duration? delay}) async {
    if (msg.replyTo == null) {
      throw Exception(
          'Message does not have a reply subject for acknowledgment');
    }

    final data = delay != null
        ? utf8.encode(jsonEncode({'delay': delay.inMicroseconds * 1000}))
        : Uint8List(0);

    await _client.pub(msg.replyTo!, Uint8List.fromList(data));
  }

  /// Acknowledge message in progress (extends ack wait time)
  Future<void> inProgress(Message msg) async {
    if (msg.replyTo == null) {
      throw Exception(
          'Message does not have a reply subject for acknowledgment');
    }
    await _client.pub('${msg.replyTo!}+WPI', Uint8List(0));
  }

  /// Terminate message processing (will not be redelivered)
  Future<void> term(Message msg) async {
    if (msg.replyTo == null) {
      throw Exception(
          'Message does not have a reply subject for acknowledgment');
    }
    await _client.pub('${msg.replyTo!}+TERM', Uint8List(0));
  }

  /// Get message metadata from a JetStream message
  JetStreamMessageMetadata getMetadata(Message msg) {
    return JetStreamMessageMetadata.fromMessage(msg);
  }
}

/// Pull-based subscription for JetStream
class PullSubscription<T> {
  final Client _client;
  final String _streamName;
  final String _consumerName;
  final String _apiPrefix;
  final T Function(String)? _jsonDecoder;

  /// Creates a pull subscription
  PullSubscription(
    this._client,
    this._streamName,
    this._consumerName,
    this._apiPrefix, {
    T Function(String)? jsonDecoder,
  }) : _jsonDecoder = jsonDecoder;

  /// Fetch messages from the consumer
  Future<List<Message<T>>> fetch({
    int batch = 1,
    Duration? expires,
    bool noWait = false,
  }) async {
    final subject = '$_apiPrefix.CONSUMER.MSG.NEXT.$_streamName.$_consumerName';

    final options = PullSubscribeOptions(
      batch: batch,
      expires: expires,
      noWait: noWait,
    );

    final payload = jsonEncode(options.toJson());

    // Create a temporary subscription for the responses
    final inbox = newInbox(inboxPrefix: '_INBOX');
    final sub = _client.sub<T>(inbox, jsonDecoder: _jsonDecoder);

    final messages = <Message<T>>[];
    final completer = Completer<List<Message<T>>>();

    // Set up listener for messages
    StreamSubscription? streamSub;
    streamSub = sub.stream.listen(
      (msg) {
        messages.add(msg);
        if (messages.length >= batch) {
          streamSub?.cancel();
          completer.complete(messages);
        }
      },
      onError: (e) {
        streamSub?.cancel();
        completer.completeError(e);
      },
      onDone: () {
        if (!completer.isCompleted) {
          completer.complete(messages);
        }
      },
    );

    try {
      // Send the pull request
      _client.pub(
        subject,
        Uint8List.fromList(utf8.encode(payload)),
        replyTo: inbox,
      );

      // Wait for messages or timeout
      final timeout = expires ?? const Duration(seconds: 5);
      final result = await completer.future.timeout(
        timeout,
        onTimeout: () {
          streamSub?.cancel();
          return messages;
        },
      );

      sub.unSub();
      return result;
    } catch (e) {
      streamSub.cancel();
      sub.unSub();
      rethrow;
    }
  }

  /// Unsubscribe and clean up
  Future<void> unsubscribe() async {
    // Consumer cleanup if needed
  }
}
