[2022-03-01 23:21:30,950] INFO after write accumulator value [
  StatusAccumulator(
  partitionOffsets: Map(
  test.db_gb18030_test.tbl_test_1:0 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 0, range: [0 -> 0], curOffset: 0),
test.db_gb18030_test.tbl_test_1:1 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 1, range: [0 -> 4], curOffset: 3),
test.db_gb18030_test.tbl_test_1:2 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 2, range: [0 -> 10], curOffset: 4)
),
schemaVersion: 1
)
] (org.apache.iceberg.streaming.Kafka2Iceberg)
  [2022-03-01 23:21:30,953] INFO Finished job streaming job 1646148080000 ms.0 from job set of time 1646148080000 ms (org.apache.spark.streaming.scheduler.JobScheduler)
  [2022-03-01 23:21:30,954] ERROR Error running job streaming job 1646148080000 ms.0 (org.apache.spark.streaming.scheduler.JobScheduler)
org.apache.iceberg.streaming.exception.SchemaChangedException: detect schema version changed, need restart spark streaming job...


[2022-03-01 23:21:43,002] INFO after write accumulator value [
StatusAccumulator(
partitionOffsets: Map(
test.db_gb18030_test.tbl_test_1:0 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 0, range: [0 -> 0], curOffset: 0),
test.db_gb18030_test.tbl_test_1:1 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 1, range: [3 -> 4], curOffset: 4),
test.db_gb18030_test.tbl_test_1:2 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 2, range: [4 -> 10], curOffset: 10)
),
schemaVersion: 2
)

[2022-03-01 23:37:22,164] INFO ----------------------------------------------------------------------------------
(org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-01 23:37:22,164] INFO after write accumulator value [
StatusAccumulator(
partitionOffsets: Map(
test.db_gb18030_test.tbl_test_1:0 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 0, range: [0 -> 0], curOffset: 0),
test.db_gb18030_test.tbl_test_1:1 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 1, range: [4 -> 4], curOffset: 4),
test.db_gb18030_test.tbl_test_1:2 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 2, range: [10 -> 11], curOffset: 10)
),
schemaVersion: 2
)
] (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-01 23:37:22,164] INFO Finished job streaming job 1646149040000 ms.0 from job set of time 1646149040000 ms (org.apache.spark.streaming.scheduler.JobScheduler)
[2022-03-01 23:37:22,165] INFO Total delay: 2.164 s for time 1646149040000 ms (execution: 2.160 s) (org.apache.spark.streaming.scheduler.JobScheduler)
[2022-03-01 23:37:22,165] INFO Removing RDD 111 from persistence list (org.apache.spark.streaming.kafka010.KafkaRDD)
[2022-03-01 23:37:22,166] INFO Removed broadcast_52_piece0 on hadoop:38511 in memory (size: 27.5 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
