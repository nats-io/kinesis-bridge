# NATS-AWS Kinesis Bridge

The is a standalone program that bridges NATS and AWS Kinesis streams.

## Configuration

A configuration file is used to model the translation from the stream source to sink.

Here is a simple example:

```yaml
nats:
  # Optional NATS context to use which declares the configuration
  # to connect and authenticate with NATS.
  context: "kinesis-bridge"

  # Name of the KV bucket used to store shard offsets per stream. This
  # can be shared across all streams.
  bucket: "kinesis-bridge"

kinesis:
  # Each key is a stream name.
  sensor-data:
    # Encoding declares the encoding of the data. Must be set if
    # the properties will be accessible.
    encoding: json

    # Start position for shards when initialized. "earliest" or "new" (default).
    start_position: new

    # Declares the corresponding NATS configuration.
    nats:
      # The subject to publish a message to. This can be a concrete subject
      # like "sensor-data", but template variables are also supported (see below).
      subject: "sensor-data.{{.Data.facility_code}}.{{.Data.pointid}}"

      # Subject to publish if a record cannot be parsed and published to
      # the standard subject.
      dlq: "sensor-data.dlq"
```

### Subject

The defined subject and DLQ subject must be bound to a stream.

The subject supports the following template variables:

- `Data` - For the `Data` property to be accessible, the encoding type must be supported, e.g. `json`. The default encoding is assumed to be an opaque bytes.
- `PartitionKey` - The partition key set on the record, if any.
- `SequenceNumber` - The sequence number of the record in the stream.
- `ShardID` - The shard ID on the stream that the record was in.

### Headers

When a message is republished to NATS, the following headers are set:

- `Kinesis-Stream-Name` - Name of the stream the message was from.
- `Kinesis-Shard-Id` - ID of the shard the record was stored in.
- `Kinesis-Partition-Key` - Partition key of the record.
- `Kinesis-Sequence-Number` - Sequence number of the message within the shard.
- `Kinesis-Arrival-Timestamp` - The arrival timestamp of the record within the stream.
- `Nats-Msg-Id` - Hash of the stream name, shard, partition key, and sequence number.

## Setup

Before running the bridge, the streams and KV bucket must be created that match the configuration.

### Create a stream

The subjects specified in the configuration must be bound to a NATS stream. For example, a stream `sensor-data` could be created with a subject `sensor-data.>` which will match the messages that are successfully parsed as well as the ones needing to going into the DLQ subject.

To create the stream, use `nats stream add`. You will be prompted for each option, however three options that are important to define are the subjects, the replicas, and limits such as max age.

```
$ nats stream add --subjects "sensor-data.>" --replicas 3 --max-age "24h" sensor-data
```

### Create a KV bucket

```
$ nats kv add --replicas 3 kinesis-bridge
```
