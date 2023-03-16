# NATS-AWS Kinesis Bridge

The is a standalone program that bridges NATS and AWS Kinesis streams.

## Configuration

A configuration file is used to model the translation from the stream source to sink.

Here is a simple example:

```yaml
kinesis:
  # Each key is a stream name.
  sensor-data:
    # Encoding declares the encoding of the data. Must be set if
    # the properties will be accessible.
    encoding: json

    # Declares the corresponding NATS configuration.
    nats:
      # Subject the translated message will be publish to. If this
      # the messages must be persisted, streams must be created ahead
      # of time with this subject being mapped or bound to.
      subject: "sensor-data.{{.Data.facility_code}}.{{.Data.pointid}}"
```

### Subject

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

## Load Balancing

Multiple instances can be used to scale. There are three events that can occur:

- An instance joins
- An instance leaves
- The partitions change

TODO
