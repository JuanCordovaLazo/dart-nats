import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'client.dart';
import 'jetstream.dart';
import 'message.dart';

/// JetStream Manager handles stream and consumer management operations
class JetStreamManager {
  final Client _client;
  final String _apiPrefix;
  final Duration _timeout;

  /// Creates a JetStream manager
  JetStreamManager(
    this._client, {
    String apiPrefix = r'$JS.API',
    Duration timeout = const Duration(seconds: 5),
  })  : _apiPrefix = apiPrefix,
        _timeout = timeout;

  /// Add a stream
  Future<StreamInfo> addStream(StreamConfig config) async {
    final subject = '$_apiPrefix.STREAM.CREATE.${config.name}';
    final payload = jsonEncode(config.toJson());

    final response = await _client.request(
      subject,
      Uint8List.fromList(utf8.encode(payload)),
      timeout: _timeout,
    );

    final json = jsonDecode(response.string) as Map<String, dynamic>;
    final apiResponse = JetStreamApiResponse.fromJson(json);

    if (apiResponse.hasError) {
      throw Exception(apiResponse.error.toString());
    }

    return StreamInfo.fromJson(json);
  }

  /// Update a stream configuration
  Future<StreamInfo> updateStream(StreamConfig config) async {
    final subject = '$_apiPrefix.STREAM.UPDATE.${config.name}';
    final payload = jsonEncode(config.toJson());

    final response = await _client.request(
      subject,
      Uint8List.fromList(utf8.encode(payload)),
      timeout: _timeout,
    );

    final json = jsonDecode(response.string) as Map<String, dynamic>;
    final apiResponse = JetStreamApiResponse.fromJson(json);

    if (apiResponse.hasError) {
      throw Exception(apiResponse.error.toString());
    }

    return StreamInfo.fromJson(json);
  }

  /// Get stream information
  Future<StreamInfo> streamInfo(String streamName) async {
    final subject = '$_apiPrefix.STREAM.INFO.$streamName';
    final payload = jsonEncode(<String, dynamic>{});

    final response = await _client.request(
      subject,
      Uint8List.fromList(utf8.encode(payload)),
      timeout: _timeout,
    );

    final json = jsonDecode(response.string) as Map<String, dynamic>;
    final apiResponse = JetStreamApiResponse.fromJson(json);

    if (apiResponse.hasError) {
      throw Exception(apiResponse.error.toString());
    }

    return StreamInfo.fromJson(json);
  }

  /// Delete a stream
  Future<bool> deleteStream(String streamName) async {
    final subject = '$_apiPrefix.STREAM.DELETE.$streamName';
    final payload = jsonEncode(<String, dynamic>{});

    final response = await _client.request(
      subject,
      Uint8List.fromList(utf8.encode(payload)),
      timeout: _timeout,
    );

    final json = jsonDecode(response.string) as Map<String, dynamic>;
    final apiResponse = JetStreamApiResponse.fromJson(json);

    if (apiResponse.hasError) {
      throw Exception(apiResponse.error.toString());
    }

    return json['success'] as bool? ?? false;
  }

  /// Purge a stream (delete all messages)
  Future<bool> purgeStream(String streamName) async {
    final subject = '$_apiPrefix.STREAM.PURGE.$streamName';
    final payload = jsonEncode(<String, dynamic>{});

    final response = await _client.request(
      subject,
      Uint8List.fromList(utf8.encode(payload)),
      timeout: _timeout,
    );

    final json = jsonDecode(response.string) as Map<String, dynamic>;
    final apiResponse = JetStreamApiResponse.fromJson(json);

    if (apiResponse.hasError) {
      throw Exception(apiResponse.error.toString());
    }

    return json['success'] as bool? ?? false;
  }

  /// List all streams
  Future<List<String>> listStreams() async {
    final subject = '$_apiPrefix.STREAM.LIST';
    final payload = jsonEncode(<String, dynamic>{});

    final response = await _client.request(
      subject,
      Uint8List.fromList(utf8.encode(payload)),
      timeout: _timeout,
    );

    final json = jsonDecode(response.string) as Map<String, dynamic>;
    final apiResponse = JetStreamApiResponse.fromJson(json);

    if (apiResponse.hasError) {
      throw Exception(apiResponse.error.toString());
    }

    final streams = json['streams'] as List<dynamic>?;
    if (streams == null) return [];

    // Streams can be returned as objects with name field or as strings
    return streams
        .map((s) {
          if (s is String) return s;
          if (s is Map<String, dynamic>) {
            final name = s['name'] ?? s['config']?['name'];
            if (name != null) return name as String;
          }
          return s.toString();
        })
        .where((name) => name.isNotEmpty)
        .toList();
  }

  /// Add a consumer to a stream
  Future<ConsumerInfo> addConsumer(
    String streamName,
    ConsumerConfig config,
  ) async {
    final consumerName = config.durable ?? '';
    final subject = consumerName.isNotEmpty
        ? '$_apiPrefix.CONSUMER.DURABLE.CREATE.$streamName.$consumerName'
        : '$_apiPrefix.CONSUMER.CREATE.$streamName';

    final payload = jsonEncode({
      'stream_name': streamName,
      'config': config.toJson(),
    });

    final response = await _client.request(
      subject,
      Uint8List.fromList(utf8.encode(payload)),
      timeout: _timeout,
    );

    final json = jsonDecode(response.string) as Map<String, dynamic>;
    final apiResponse = JetStreamApiResponse.fromJson(json);

    if (apiResponse.hasError) {
      throw Exception(apiResponse.error.toString());
    }

    return ConsumerInfo.fromJson(json);
  }

  /// Get consumer information
  Future<ConsumerInfo> consumerInfo(
    String streamName,
    String consumerName,
  ) async {
    final subject = '$_apiPrefix.CONSUMER.INFO.$streamName.$consumerName';
    final payload = jsonEncode(<String, dynamic>{});

    final response = await _client.request(
      subject,
      Uint8List.fromList(utf8.encode(payload)),
      timeout: _timeout,
    );

    final json = jsonDecode(response.string) as Map<String, dynamic>;
    final apiResponse = JetStreamApiResponse.fromJson(json);

    if (apiResponse.hasError) {
      throw Exception(apiResponse.error.toString());
    }

    return ConsumerInfo.fromJson(json);
  }

  /// Delete a consumer
  Future<bool> deleteConsumer(
    String streamName,
    String consumerName,
  ) async {
    final subject = '$_apiPrefix.CONSUMER.DELETE.$streamName.$consumerName';
    final payload = jsonEncode(<String, dynamic>{});

    final response = await _client.request(
      subject,
      Uint8List.fromList(utf8.encode(payload)),
      timeout: _timeout,
    );

    final json = jsonDecode(response.string) as Map<String, dynamic>;
    final apiResponse = JetStreamApiResponse.fromJson(json);

    if (apiResponse.hasError) {
      throw Exception(apiResponse.error.toString());
    }

    return json['success'] as bool? ?? false;
  }

  /// List consumers for a stream
  Future<List<String>> listConsumers(String streamName) async {
    final subject = '$_apiPrefix.CONSUMER.LIST.$streamName';
    final payload = jsonEncode(<String, dynamic>{});

    final response = await _client.request(
      subject,
      Uint8List.fromList(utf8.encode(payload)),
      timeout: _timeout,
    );

    final json = jsonDecode(response.string) as Map<String, dynamic>;
    final apiResponse = JetStreamApiResponse.fromJson(json);

    if (apiResponse.hasError) {
      throw Exception(apiResponse.error.toString());
    }

    final consumers = json['consumers'] as List<dynamic>?;
    if (consumers == null) return [];

    // Consumers can be returned as objects with name field or as strings
    return consumers.map((c) {
      if (c is String) return c;
      if (c is Map<String, dynamic>) return c['name'] as String;
      return c.toString();
    }).toList();
  }

  /// Get a message from a stream by sequence number
  Future<Message> getMessage(
    String streamName,
    int sequence,
  ) async {
    final subject = '$_apiPrefix.STREAM.MSG.GET.$streamName';
    final payload = jsonEncode({'seq': sequence});

    final response = await _client.request(
      subject,
      Uint8List.fromList(utf8.encode(payload)),
      timeout: _timeout,
    );

    final json = jsonDecode(response.string) as Map<String, dynamic>;
    final apiResponse = JetStreamApiResponse.fromJson(json);

    if (apiResponse.hasError) {
      throw Exception(apiResponse.error.toString());
    }

    final msgData = json['message'] as Map<String, dynamic>;
    final msgSubject = msgData['subject'] as String;
    final data = msgData['data'] as String;
    final hdrs = msgData['hdrs'] as String?;

    Header? header;
    if (hdrs != null && hdrs.isNotEmpty) {
      header = Header.fromBytes(Uint8List.fromList(utf8.encode(hdrs)));
    }

    return Message(
      msgSubject,
      0,
      base64.decode(data),
      _client,
      replyTo: null,
      header: header,
    );
  }

  /// Delete a message from a stream
  Future<bool> deleteMessage(
    String streamName,
    int sequence,
  ) async {
    final subject = '$_apiPrefix.STREAM.MSG.DELETE.$streamName';
    final payload = jsonEncode({'seq': sequence});

    final response = await _client.request(
      subject,
      Uint8List.fromList(utf8.encode(payload)),
      timeout: _timeout,
    );

    final json = jsonDecode(response.string) as Map<String, dynamic>;
    final apiResponse = JetStreamApiResponse.fromJson(json);

    if (apiResponse.hasError) {
      throw Exception(apiResponse.error.toString());
    }

    return json['success'] as bool? ?? false;
  }
}
