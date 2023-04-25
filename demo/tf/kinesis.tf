resource "aws_kinesis_stream" "test_stream" {
  name             = "test-stream"
  shard_count      = 120
  retention_period = 24

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}
