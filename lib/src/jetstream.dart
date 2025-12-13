/// Storage type for streams
enum StorageType {
  /// File-based storage
  file,

  /// Memory-based storage
  memory,
}

/// Retention policy for streams
enum RetentionPolicy {
  /// Retention based on limits
  limits,

  /// Work queue retention (messages deleted after ack)
  workQueue,

  /// Interest-based retention (messages kept while there are consumers)
  interest,
}

/// Discard policy when stream limits are hit
enum DiscardPolicy {
  /// Discard old messages
  old,

  /// Discard new messages
  new_,

  /// Discard new messages per subject
  newPerSubject,
}

/// Acknowledgment policy for consumers
enum AckPolicy {
  /// No acknowledgment needed
  none,

  /// All messages acknowledged up to the acked message
  all,

  /// Each message must be explicitly acknowledged
  explicit,
}

/// Delivery policy for consumers
enum DeliverPolicy {
  /// Deliver all messages
  all,

  /// Deliver only last message
  last,

  /// Deliver last message per subject
  lastPerSubject,

  /// Deliver only new messages
  new_,

  /// Deliver starting at sequence
  byStartSequence,

  /// Deliver starting at time
  byStartTime,
}

/// Replay policy for consumers
enum ReplayPolicy {
  /// Replay messages instantly
  instant,

  /// Replay messages at original speed
  original,
}

/// Stream configuration
class StreamConfig {
  /// Stream name
  String name;

  /// Subjects that belong to this stream
  List<String>? subjects;

  /// Retention policy
  RetentionPolicy? retention;

  /// Maximum messages
  int? maxMsgs;

  /// Maximum bytes
  int? maxBytes;

  /// Maximum age in nanoseconds
  int? maxAge;

  /// Maximum message size
  int? maxMsgSize;

  /// Storage type
  StorageType? storage;

  /// Discard policy
  DiscardPolicy? discard;

  /// Number of replicas
  int? numReplicas;

  /// Duplicate tracking window in nanoseconds
  int? duplicateWindow;

  /// Placement cluster
  String? placement;

  /// Whether to allow message rollup
  bool? allowRollup;

  /// Whether to deny delete
  bool? denyDelete;

  /// Whether to deny purge
  bool? denyPurge;

  /// Maximum messages per subject
  int? maxMsgsPerSubject;

  /// Maximum number of consumers
  int? maxConsumers;

  /// Creates a new stream configuration
  StreamConfig({
    required this.name,
    this.subjects,
    this.retention,
    this.maxMsgs,
    this.maxBytes,
    this.maxAge,
    this.maxMsgSize,
    this.storage,
    this.discard,
    this.numReplicas,
    this.duplicateWindow,
    this.placement,
    this.allowRollup,
    this.denyDelete,
    this.denyPurge,
    this.maxMsgsPerSubject,
    this.maxConsumers,
  });

  /// Converts the stream configuration to JSON
  Map<String, dynamic> toJson() {
    final json = <String, dynamic>{
      'name': name,
    };

    if (subjects != null) json['subjects'] = subjects;
    if (retention != null) {
      json['retention'] = retention.toString().split('.').last;
    }
    if (maxMsgs != null) json['max_msgs'] = maxMsgs;
    if (maxBytes != null) json['max_bytes'] = maxBytes;
    if (maxAge != null) json['max_age'] = maxAge;
    if (maxMsgSize != null) json['max_msg_size'] = maxMsgSize;
    if (storage != null) {
      json['storage'] = storage.toString().split('.').last;
    }
    if (discard != null) {
      var discardStr = discard.toString().split('.').last;
      if (discardStr == 'new_') discardStr = 'new';
      json['discard'] = discardStr;
    }
    if (numReplicas != null) json['num_replicas'] = numReplicas;
    if (duplicateWindow != null) {
      json['duplicate_window'] = duplicateWindow;
    }
    if (placement != null) json['placement'] = {'cluster': placement};
    if (allowRollup != null) json['allow_rollup_hdrs'] = allowRollup;
    if (denyDelete != null) json['deny_delete'] = denyDelete;
    if (denyPurge != null) json['deny_purge'] = denyPurge;
    if (maxMsgsPerSubject != null) {
      json['max_msgs_per_subject'] = maxMsgsPerSubject;
    }
    if (maxConsumers != null) json['max_consumers'] = maxConsumers;

    return json;
  }

  /// Creates a stream configuration from JSON
  factory StreamConfig.fromJson(Map<String, dynamic> json) {
    return StreamConfig(
      name: json['name'] as String,
      subjects: (json['subjects'] as List<dynamic>?)?.cast<String>(),
      retention: json['retention'] != null
          ? _retentionPolicyFromString(json['retention'] as String)
          : null,
      maxMsgs: json['max_msgs'] as int?,
      maxBytes: json['max_bytes'] as int?,
      maxAge: json['max_age'] as int?,
      maxMsgSize: json['max_msg_size'] as int?,
      storage: json['storage'] != null
          ? _storageTypeFromString(json['storage'] as String)
          : null,
      discard: json['discard'] != null
          ? _discardPolicyFromString(json['discard'] as String)
          : null,
      numReplicas: json['num_replicas'] as int?,
      duplicateWindow: json['duplicate_window'] as int?,
      placement:
          (json['placement'] as Map<String, dynamic>?)?['cluster'] as String?,
      allowRollup: json['allow_rollup_hdrs'] as bool?,
      denyDelete: json['deny_delete'] as bool?,
      denyPurge: json['deny_purge'] as bool?,
      maxMsgsPerSubject: json['max_msgs_per_subject'] as int?,
      maxConsumers: json['max_consumers'] as int?,
    );
  }
}

/// Consumer configuration
class ConsumerConfig {
  /// Durable consumer name
  String? durable;

  /// Delivery subject for push consumers
  String? deliverSubject;

  /// Acknowledgment policy
  AckPolicy? ackPolicy;

  /// Acknowledgment wait time in nanoseconds
  int? ackWait;

  /// Maximum delivery attempts
  int? maxDeliver;

  /// Filter subject
  String? filterSubject;

  /// Filter subjects (multiple filters)
  List<String>? filterSubjects;

  /// Replay policy
  ReplayPolicy? replayPolicy;

  /// Maximum outstanding unacknowledged messages
  int? maxAckPending;

  /// Delivery policy
  DeliverPolicy? deliverPolicy;

  /// Start sequence for DeliverByStartSequence
  int? optStartSeq;

  /// Start time for DeliverByStartTime (as RFC3339 string)
  String? optStartTime;

  /// Sample frequency
  String? sampleFrequency;

  /// Maximum waiting pull requests
  int? maxWaiting;

  /// Maximum batch size
  int? maxBatch;

  /// Maximum expires duration in nanoseconds
  int? maxExpires;

  /// Inactive threshold in nanoseconds
  int? inactiveThreshold;

  /// Number of replicas
  int? numReplicas;

  /// Memory storage
  bool? memStorage;

  /// Description
  String? description;

  /// Creates a new consumer configuration
  ConsumerConfig({
    this.durable,
    this.deliverSubject,
    this.ackPolicy,
    this.ackWait,
    this.maxDeliver,
    this.filterSubject,
    this.filterSubjects,
    this.replayPolicy,
    this.maxAckPending,
    this.deliverPolicy,
    this.optStartSeq,
    this.optStartTime,
    this.sampleFrequency,
    this.maxWaiting,
    this.maxBatch,
    this.maxExpires,
    this.inactiveThreshold,
    this.numReplicas,
    this.memStorage,
    this.description,
  });

  /// Converts the consumer configuration to JSON
  Map<String, dynamic> toJson() {
    final json = <String, dynamic>{};

    if (durable != null) json['durable_name'] = durable;
    if (deliverSubject != null) json['deliver_subject'] = deliverSubject;
    if (ackPolicy != null) {
      var policyStr = ackPolicy.toString().split('.').last;
      json['ack_policy'] = policyStr;
    }
    if (ackWait != null) json['ack_wait'] = ackWait;
    if (maxDeliver != null) json['max_deliver'] = maxDeliver;
    if (filterSubject != null) json['filter_subject'] = filterSubject;
    if (filterSubjects != null) json['filter_subjects'] = filterSubjects;
    if (replayPolicy != null) {
      json['replay_policy'] = replayPolicy.toString().split('.').last;
    }
    if (maxAckPending != null) json['max_ack_pending'] = maxAckPending;
    if (deliverPolicy != null) {
      var policyStr = deliverPolicy.toString().split('.').last;
      if (policyStr == 'new_') policyStr = 'new';
      json['deliver_policy'] = policyStr;
    }
    if (optStartSeq != null) json['opt_start_seq'] = optStartSeq;
    if (optStartTime != null) json['opt_start_time'] = optStartTime;
    if (sampleFrequency != null) json['sample_freq'] = sampleFrequency;
    if (maxWaiting != null) json['max_waiting'] = maxWaiting;
    if (maxBatch != null) json['max_batch'] = maxBatch;
    if (maxExpires != null) json['max_expires'] = maxExpires;
    if (inactiveThreshold != null) {
      json['inactive_threshold'] = inactiveThreshold;
    }
    if (numReplicas != null) json['num_replicas'] = numReplicas;
    if (memStorage != null) json['mem_storage'] = memStorage;
    if (description != null) json['description'] = description;

    return json;
  }

  /// Creates a consumer configuration from JSON
  factory ConsumerConfig.fromJson(Map<String, dynamic> json) {
    return ConsumerConfig(
      durable: json['durable_name'] as String?,
      deliverSubject: json['deliver_subject'] as String?,
      ackPolicy: json['ack_policy'] != null
          ? _ackPolicyFromString(json['ack_policy'] as String)
          : null,
      ackWait: json['ack_wait'] as int?,
      maxDeliver: json['max_deliver'] as int?,
      filterSubject: json['filter_subject'] as String?,
      filterSubjects:
          (json['filter_subjects'] as List<dynamic>?)?.cast<String>(),
      replayPolicy: json['replay_policy'] != null
          ? _replayPolicyFromString(json['replay_policy'] as String)
          : null,
      maxAckPending: json['max_ack_pending'] as int?,
      deliverPolicy: json['deliver_policy'] != null
          ? _deliverPolicyFromString(json['deliver_policy'] as String)
          : null,
      optStartSeq: json['opt_start_seq'] as int?,
      optStartTime: json['opt_start_time'] as String?,
      sampleFrequency: json['sample_freq'] as String?,
      maxWaiting: json['max_waiting'] as int?,
      maxBatch: json['max_batch'] as int?,
      maxExpires: json['max_expires'] as int?,
      inactiveThreshold: json['inactive_threshold'] as int?,
      numReplicas: json['num_replicas'] as int?,
      memStorage: json['mem_storage'] as bool?,
      description: json['description'] as String?,
    );
  }
}

/// JetStream publish acknowledgment
class PubAck {
  /// Stream name
  final String stream;

  /// Sequence number
  final int seq;

  /// Whether it was a duplicate
  final bool duplicate;

  /// Domain
  final String? domain;

  /// Creates a publish acknowledgment
  PubAck({
    required this.stream,
    required this.seq,
    this.duplicate = false,
    this.domain,
  });

  /// Creates a publish acknowledgment from JSON
  factory PubAck.fromJson(Map<String, dynamic> json) {
    return PubAck(
      stream: json['stream'] as String,
      seq: json['seq'] as int,
      duplicate: json['duplicate'] as bool? ?? false,
      domain: json['domain'] as String?,
    );
  }
}

/// Stream state
class StreamState {
  /// Number of messages
  final int messages;

  /// Total bytes
  final int bytes;

  /// First sequence number
  final int firstSeq;

  /// First message timestamp
  final String? firstTs;

  /// Last sequence number
  final int lastSeq;

  /// Last message timestamp
  final String? lastTs;

  /// Number of consumers
  final int consumers;

  /// Creates stream state information
  StreamState({
    required this.messages,
    required this.bytes,
    required this.firstSeq,
    this.firstTs,
    required this.lastSeq,
    this.lastTs,
    required this.consumers,
  });

  /// Creates stream state from JSON
  factory StreamState.fromJson(Map<String, dynamic> json) {
    return StreamState(
      messages: json['messages'] as int,
      bytes: json['bytes'] as int,
      firstSeq: json['first_seq'] as int,
      firstTs: json['first_ts'] as String?,
      lastSeq: json['last_seq'] as int,
      lastTs: json['last_ts'] as String?,
      consumers: json['consumer_count'] as int,
    );
  }
}

/// Stream information
class StreamInfo {
  /// Stream configuration
  final StreamConfig config;

  /// Creation timestamp
  final String created;

  /// Stream state
  final StreamState state;

  /// Creates stream information
  StreamInfo({
    required this.config,
    required this.created,
    required this.state,
  });

  /// Creates stream information from JSON
  factory StreamInfo.fromJson(Map<String, dynamic> json) {
    return StreamInfo(
      config: StreamConfig.fromJson(json['config'] as Map<String, dynamic>),
      created: json['created'] as String,
      state: StreamState.fromJson(json['state'] as Map<String, dynamic>),
    );
  }
}

/// Consumer information
class ConsumerInfo {
  /// Stream name
  final String streamName;

  /// Consumer name
  final String name;

  /// Creation timestamp
  final String created;

  /// Consumer configuration
  final ConsumerConfig config;

  /// Delivered message info
  final Map<String, dynamic>? delivered;

  /// Acknowledgment floor info
  final Map<String, dynamic>? ackFloor;

  /// Number of pending messages
  final int numPending;

  /// Number of redelivered messages
  final int numRedelivered;

  /// Number of waiting pull requests
  final int numWaiting;

  /// Creates consumer information
  ConsumerInfo({
    required this.streamName,
    required this.name,
    required this.created,
    required this.config,
    this.delivered,
    this.ackFloor,
    required this.numPending,
    required this.numRedelivered,
    required this.numWaiting,
  });

  /// Creates consumer information from JSON
  factory ConsumerInfo.fromJson(Map<String, dynamic> json) {
    return ConsumerInfo(
      streamName: json['stream_name'] as String,
      name: json['name'] as String,
      created: json['created'] as String,
      config: ConsumerConfig.fromJson(json['config'] as Map<String, dynamic>),
      delivered: json['delivered'] as Map<String, dynamic>?,
      ackFloor: json['ack_floor'] as Map<String, dynamic>?,
      numPending: json['num_pending'] as int,
      numRedelivered: json['num_redelivered'] as int,
      numWaiting: json['num_waiting'] as int,
    );
  }
}

/// JetStream API error
class JetStreamError {
  /// Error code
  final int code;

  /// Error description
  final String description;

  /// JetStream error code
  final int? errCode;

  /// Creates a JetStream error
  JetStreamError({
    required this.code,
    required this.description,
    this.errCode,
  });

  /// Creates a JetStream error from JSON
  factory JetStreamError.fromJson(Map<String, dynamic> json) {
    return JetStreamError(
      code: json['code'] as int,
      description: json['description'] as String,
      errCode: json['err_code'] as int?,
    );
  }

  /// String representation of the error
  @override
  String toString() => 'JetStreamError($code): $description';
}

/// JetStream API response
class JetStreamApiResponse {
  /// Response type
  final String type;

  /// Response data
  final dynamic data;

  /// Error if any
  final JetStreamError? error;

  /// Creates a JetStream API response
  JetStreamApiResponse({
    required this.type,
    this.data,
    this.error,
  });

  /// Creates a JetStream API response from JSON
  factory JetStreamApiResponse.fromJson(Map<String, dynamic> json) {
    return JetStreamApiResponse(
      type: json['type'] as String,
      data: json['error'] == null ? json : null,
      error: json['error'] != null
          ? JetStreamError.fromJson(json['error'] as Map<String, dynamic>)
          : null,
    );
  }

  /// Whether this response contains an error
  bool get hasError => error != null;
}

// Helper functions to convert strings to enums
StorageType _storageTypeFromString(String value) {
  switch (value.toLowerCase()) {
    case 'file':
      return StorageType.file;
    case 'memory':
      return StorageType.memory;
    default:
      return StorageType.file;
  }
}

RetentionPolicy _retentionPolicyFromString(String value) {
  switch (value.toLowerCase()) {
    case 'limits':
      return RetentionPolicy.limits;
    case 'workqueue':
      return RetentionPolicy.workQueue;
    case 'interest':
      return RetentionPolicy.interest;
    default:
      return RetentionPolicy.limits;
  }
}

DiscardPolicy _discardPolicyFromString(String value) {
  switch (value.toLowerCase()) {
    case 'old':
      return DiscardPolicy.old;
    case 'new':
      return DiscardPolicy.new_;
    default:
      return DiscardPolicy.old;
  }
}

AckPolicy _ackPolicyFromString(String value) {
  switch (value.toLowerCase()) {
    case 'none':
      return AckPolicy.none;
    case 'all':
      return AckPolicy.all;
    case 'explicit':
      return AckPolicy.explicit;
    default:
      return AckPolicy.explicit;
  }
}

DeliverPolicy _deliverPolicyFromString(String value) {
  switch (value.toLowerCase()) {
    case 'all':
      return DeliverPolicy.all;
    case 'last':
      return DeliverPolicy.last;
    case 'last_per_subject':
      return DeliverPolicy.lastPerSubject;
    case 'new':
      return DeliverPolicy.new_;
    case 'by_start_sequence':
      return DeliverPolicy.byStartSequence;
    case 'by_start_time':
      return DeliverPolicy.byStartTime;
    default:
      return DeliverPolicy.all;
  }
}

ReplayPolicy _replayPolicyFromString(String value) {
  switch (value.toLowerCase()) {
    case 'instant':
      return ReplayPolicy.instant;
    case 'original':
      return ReplayPolicy.original;
    default:
      return ReplayPolicy.instant;
  }
}
