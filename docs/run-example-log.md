

# 执行日志示例  

## 运行配置示范
```text
## Spark Configs

spark.master = local[2]
spark.app.name= Kafka2Iceberg

spark.sql.sources.partitionOverwriteMode = dynamic
spark.sql.extensions = org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.hive = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.hive.type = hive
spark.hadoop.hive.metastore.uris = thrift://hadoop:9083
spark.sql.warehouse.dir = hdfs://hadoop:8020/user/hive/warehouse
spark.streaming.kafka.maxRatePerPartition=10000


# Kafka Configs
kafka.bootstrap.servers=kafka:9092
kafka.schema.registry.url=http://kafka:8081
kafka.consumer.group.id=g1-3
kafka.consumer.topic=test.db_gb18030_test.tbl_test
kafka.consumer.max.poll.records=5
kafka.consumer.key.deserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer
kafka.consumer.value.deserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer
kafka.consumer.commit.timeout.millis=60000

# Metadata Columns
record.metadata.source.columns = name, db, table, ts_ms, server_id, file, pos
record.metadata.source.prefix = _src_
record.metadata.transaction.columns = id, total_order, data_collection_order
record.metadata.transaction.prefix = _tsc_
record.metadata.kafka.columns = topic, partition, offset, timestamp
record.metadata.kafka.prefix = _kfk_

# Iceberg Configs
iceberg.table.name = hive.db_gb18030_test.tbl_test
iceberg.table.partitionBy = c1
iceberg.table.location = hdfs://hadoop:8020/user/hive/warehouse/db_gb18030_test.db/tbl_test
iceberg.table.comment = db_gb18030_test tbl_test
iceberg.table.properties = 'read.split.target-size'='268435456'

iceberg.table.primaryKey = c1,c2

```
##  初始化 StatusAccumulator  /  SchemaBroadcast
```text
22/02/28 20:36:19 INFO Kafka2Iceberg: After Init StatusAccumulator [Map(hive.db_gb18030_test.tbl_test -> StatusAccumulator(Map(test.db_gb18030_test.tbl_test:0 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test', partition: 0, range: [0 -> 0], curOffset: 0), test.db_gb18030_test.tbl_test:1 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test', partition: 1, range: [0 -> 0], curOffset: 0), test.db_gb18030_test.tbl_test:2 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test', partition: 2, range: [0 -> 0], curOffset: 0)), 1))]
22/02/28 20:36:19 INFO Kafka2Iceberg: After Init SchemaBroadcast   [List(SchemaBroadcast(hive.db_gb18030_test.tbl_test, SchemaBroadcast(Map({"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID","type":"int"},{"name":"C1","type":["null","string"],"default":null},{"name":"C2","type":{"type":"string","connect.default":"CV2"},"default":"CV2"},{"name":"C3","type":["null","int"],"default":null},{"name":"C4","type":["null","long"],"default":null},{"name":"C5","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test.Envelope"} -> 2, {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID","type":"int"},{"name":"C1","type":["null","string"],"default":null},{"name":"C2","type":{"type":"string","connect.default":"CV2"},"default":"CV2"},{"name":"C3","type":["null","int"],"default":null},{"name":"C4","type":["null","long"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test.Envelope"} -> 1), Map(1 -> {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID","type":"int"},{"name":"C1","type":["null","string"],"default":null},{"name":"C2","type":{"type":"string","connect.default":"CV2"},"default":"CV2"},{"name":"C3","type":["null","int"],"default":null},{"name":"C4","type":["null","long"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test.Envelope"}, 2 -> {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID","type":"int"},{"name":"C1","type":["null","string"],"default":null},{"name":"C2","type":{"type":"string","connect.default":"CV2"},"default":"CV2"},{"name":"C3","type":["null","int"],"default":null},{"name":"C4","type":["null","long"],"default":null},{"name":"C5","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test.Envelope"}))))]
```

## 数据内容 
```text
+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+----------------------------------------+----------------+--------------------------+-----------------------------+--------------+-----------+--------------+---+---+---+---+---+----+-------------+-------------+
|_src_name|_src_db        |_src_table|_src_ts_ms   |_src_server_id|_src_file       |_src_pos|_src_op|_src_ts_ms_r |_tsc_id                                 |_tsc_total_order|_tsc_data_collection_order|_kfk_topic                   |_kfk_partition|_kfk_offset|_kfk_timestamp|id |c1 |c2 |c3 |c4 |c5  |create_time  |update_time  |
+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+----------------------------------------+----------------+--------------------------+-----------------------------+--------------+-----------+--------------+---+---+---+---+---+----+-------------+-------------+
|test     |db_gb18030_test|tbl_test  |1645690414000|1             |mysql-bin.000010|8775    |u      |1645690414404|e45b718e-906f-11ec-89e3-0242c0a8640a:219|1               |1                         |test.db_gb18030_test.tbl_test|0             |12         |1645690414488 |1  |v1 |v2 |5  |12 |null|1645157193000|1645690414000|
|test     |db_gb18030_test|tbl_test  |1645690498000|1             |mysql-bin.000010|9164    |c      |1645690498874|e45b718e-906f-11ec-89e3-0242c0a8640a:220|1               |1                         |test.db_gb18030_test.tbl_test|1             |0          |1645690499261 |2  |A1 |A2 |1  |1  |null|1645157193000|1645690414000|
|test     |db_gb18030_test|tbl_test  |1645895249000|1             |mysql-bin.000014|6745    |u      |1645895249588|e45b718e-906f-11ec-89e3-0242c0a8640a:234|1               |1                         |test.db_gb18030_test.tbl_test|1             |1          |1645895249714 |2  |A1 |A2 |2  |1  |null|1645157193000|1645895249000|
+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+----------------------------------------+----------------+--------------------------+-----------------------------+--------------+-----------+--------------+---+---+---+---+---+----+-------------+-------------+

```

## 自动创建表
```text
22/02/28 20:36:33 INFO DDLHelper: start create namespace[db_gb18030_test]-table[hive.db_gb18030_test.tbl_test] with sql [
CREATE TABLE hive.db_gb18030_test.tbl_test (
_src_name string, _src_db string, _src_table string, _src_ts_ms long, _src_server_id long, _src_file string, _src_pos long, _src_op string, _src_ts_ms_r long, _tsc_id string, _tsc_total_order long, _tsc_data_collection_order long, _kfk_topic string, _kfk_partition int, _kfk_offset long, _kfk_timestamp long, id int, c1 string, c2 string, c3 int, c4 long, create_time long, update_time long)
USING iceberg
PARTITIONED BY (c1)
LOCATION 'hdfs://hadoop:8020/user/hive/warehouse/db_gb18030_test.db/tbl_test'
COMMENT 'db_gb18030_test tbl_test'
TBLPROPERTIES ('read.split.target-size'='268435456')
]
22/02/28 20:36:33 INFO metastore: Trying to connect to metastore with URI thrift://hadoop:9083
22/02/28 20:36:33 INFO metastore: Opened a connection to metastore, current connections: 1
22/02/28 20:36:33 INFO metastore: Connected to metastore.
22/02/28 20:36:33 INFO BaseMetastoreTableOperations: Successfully committed to table hive.db_gb18030_test.tbl_test in 382 ms
22/02/28 20:36:34 INFO BaseMetastoreTableOperations: Refreshing table metadata from new version: hdfs://hadoop:8020/user/hive/warehouse/db_gb18030_test.db/tbl_test/metadata/00000-c5933bd0-7811-4e7b-b5ee-61f33bdbd75b.metadata.json
22/02/28 20:36:34 INFO RowLevelCommandScanRelationPushDown: 
Pushing operators to hive.db_gb18030_test.tbl_test
Pushed filters: 
Filters that were not pushed: 
Output: _src_name#176, _src_db#177, _src_table#178, _src_ts_ms#179L, _src_server_id#180L, _src_file#181, _src_pos#182L, _src_op#183, _src_ts_ms_r#184L, _tsc_id#185, _tsc_total_order#186L, _tsc_data_collection_order#187L, _kfk_topic#188, _kfk_partition#189, _kfk_offset#190L, _kfk_timestamp#191L, id#192, c1#193, c2#194, c3#195, c4#196L, create_time#197L, update_time#198L, _file#201

```


## Kafka 数据读取 / 丢弃 / 手动 Offset 提交  
```text
22/02/28 20:36:30 INFO SubscriptionState: [Consumer clientId=consumer-g1-3-3, groupId=g1-3] Resetting offset for partition test.db_gb18030_test.tbl_test-2 to position FetchPosition{offset=0, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[192.168.100.30:9092 (id: 0 rack: null)], epoch=0}}.
22/02/28 20:36:30 INFO SubscriptionState: [Consumer clientId=consumer-g1-3-3, groupId=g1-3] Resetting offset for partition test.db_gb18030_test.tbl_test-0 to position FetchPosition{offset=13, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[192.168.100.30:9092 (id: 0 rack: null)], epoch=0}}.
22/02/28 20:36:30 INFO SubscriptionState: [Consumer clientId=consumer-g1-3-3, groupId=g1-3] Resetting offset for partition test.db_gb18030_test.tbl_test-1 to position FetchPosition{offset=2, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[192.168.100.30:9092 (id: 0 rack: null)], epoch=0}}.
22/02/28 20:36:30 INFO JobScheduler: Added jobs for time 1646051790000 ms
22/02/28 20:36:30 INFO JobScheduler: Starting job streaming job 1646051790000 ms.0 from job set of time 1646051790000 ms
22/02/28 20:36:30 INFO Kafka2Iceberg: ----------------------------------------------------------------------------------
22/02/28 20:36:30 INFO Kafka2Iceberg: OffsetRanges: [OffsetRange(topic: 'test.db_gb18030_test.tbl_test', partition: 0, range: [0 -> 13]),OffsetRange(topic: 'test.db_gb18030_test.tbl_test', partition: 2, range: [0 -> 0]),OffsetRange(topic: 'test.db_gb18030_test.tbl_test', partition: 1, range: [0 -> 2])]
22/02/28 20:36:30 INFO Kafka2Iceberg: partitionOffsets: [test.db_gb18030_test.tbl_test:0 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test', partition: 0, range: [0 -> 13], curOffset: 0),test.db_gb18030_test.tbl_test:1 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test', partition: 1, range: [0 -> 2], curOffset: 0),test.db_gb18030_test.tbl_test:2 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test', partition: 2, range: [0 -> 0], curOffset: 0)]
22/02/28 20:36:30 INFO IcebergWriter: beginning write data into hive.db_gb18030_test.tbl_test ...
22/02/28 20:36:35 INFO DAGScheduler: Job 6 finished: sql at IcebergWriter.scala:60, took 0.777342 s
22/02/28 20:36:35 INFO ReplaceDataExec: Data source write support IcebergBatchWrite(table=hive.db_gb18030_test.tbl_test, format=PARQUET) is committing.
22/02/28 20:36:35 INFO SparkWrite: Committing overwrite of 0 data files with 1 new data files, scanSnapshotId: null, conflictDetectionFilter: true to table hive.db_gb18030_test.tbl_test
22/02/28 20:36:36 INFO BaseMetastoreTableOperations: Successfully committed to table hive.db_gb18030_test.tbl_test in 140 ms
22/02/28 20:36:36 INFO SnapshotProducer: Committed snapshot 8977969099846765012 (BaseOverwriteFiles)
22/02/28 20:36:36 INFO BaseMetastoreTableOperations: Refreshing table metadata from new version: hdfs://hadoop:8020/user/hive/warehouse/db_gb18030_test.db/tbl_test/metadata/00001-d4b5d27b-a4d6-46e8-a131-b7ea8a6b5eea.metadata.json
22/02/28 20:36:36 INFO SparkWrite: Committed in 839 ms
22/02/28 20:36:36 INFO ReplaceDataExec: Data source write support IcebergBatchWrite(table=hive.db_gb18030_test.tbl_test, format=PARQUET) committed.
22/02/28 20:36:36 INFO IcebergWriter: finished write data into hive.db_gb18030_test.tbl_test ...
22/02/28 20:36:36 INFO Kafka2Iceberg: commit offset rangers: [OffsetRange(topic: 'test.db_gb18030_test.tbl_test', partition: 0, range: [0 -> 12]),OffsetRange(topic: 'test.db_gb18030_test.tbl_test', partition: 1, range: [0 -> 0]),OffsetRange(topic: 'test.db_gb18030_test.tbl_test', partition: 2, range: [0 -> 0])]

```

## Schema 更新检测 与 Steaming 作业重启 
```text
22/02/28 20:36:36 INFO JobScheduler: Finished job streaming job 1646051790000 ms.0 from job set of time 1646051790000 ms
22/02/28 20:36:36 ERROR JobScheduler: Error running job streaming job 1646051790000 ms.0
org.apache.iceberg.streaming.exception.SchemaChangedException: detect schema version changed, need restart SparkStream job...
	at org.apache.iceberg.streaming.Kafka2Iceberg$.$anonfun$startStreamJob$1(Kafka2Iceberg.scala:210)
	at org.apache.iceberg.streaming.Kafka2Iceberg$.$anonfun$startStreamJob$1$adapted(Kafka2Iceberg.scala:182)
	at org.apache.spark.streaming.dstream.DStream.$anonfun$foreachRDD$2(DStream.scala:629)
	at org.apache.spark.streaming.dstream.DStream.$anonfun$foreachRDD$2$adapted(DStream.scala:629)
	at org.apache.spark.streaming.dstream.ForEachDStream.$anonfun$generateJob$2(ForEachDStream.scala:51)
	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
	at org.apache.spark.streaming.dstream.DStream.createRDDWithLocalProperties(DStream.scala:417)
	at org.apache.spark.streaming.dstream.ForEachDStream.$anonfun$generateJob$1(ForEachDStream.scala:51)
	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
	at scala.util.Try$.apply(Try.scala:213)
	at org.apache.spark.streaming.scheduler.Job.run(Job.scala:39)
	at org.apache.spark.streaming.scheduler.JobScheduler$JobHandler.$anonfun$run$1(JobScheduler.scala:256)
	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
	at scala.util.DynamicVariable.withValue(DynamicVariable.scala:62)
	at org.apache.spark.streaming.scheduler.JobScheduler$JobHandler.run(JobScheduler.scala:256)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
22/02/28 20:36:36 WARN Kafka2Iceberg: Detect Schema Version Changed, Restart SparkStream Job...
22/02/28 20:36:36 INFO ReceiverTracker: ReceiverTracker stopped
22/02/28 20:36:36 INFO JobGenerator: Stopping JobGenerator immediately
22/02/28 20:36:36 INFO RecurringTimer: Stopped timer for JobGenerator after time 1646051790000
22/02/28 20:36:36 INFO ConsumerCoordinator: [Consumer clientId=consumer-g1-3-3, groupId=g1-3] Revoke previously assigned partitions test.db_gb18030_test.tbl_test-2, test.db_gb18030_test.tbl_test-0, test.db_gb18030_test.tbl_test-1
22/02/28 20:36:36 INFO AbstractCoordinator: [Consumer clientId=consumer-g1-3-3, groupId=g1-3] Member consumer-g1-3-3-22fcf88b-3260-4d1e-819d-b7adb0c7c180 sending LeaveGroup request to coordinator 192.168.100.30:9092 (id: 2147483647 rack: null) due to the consumer is being closed
22/02/28 20:36:36 INFO Metrics: Metrics scheduler closed
22/02/28 20:36:36 INFO Metrics: Closing reporter org.apache.kafka.common.metrics.JmxReporter
22/02/28 20:36:36 INFO Metrics: Metrics reporters closed
22/02/28 20:36:36 INFO AppInfoParser: App info kafka.consumer for consumer-g1-3-3 unregistered
22/02/28 20:36:36 INFO JobGenerator: Stopped JobGenerator
22/02/28 20:36:36 INFO JobScheduler: Stopped JobScheduler
22/02/28 20:36:36 INFO StreamingContext: StreamingContext stopped successfully
22/02/28 20:36:39 INFO Kafka2Iceberg: commit kafka offset [test.db_gb18030_test.tbl_test-2 -> OffsetAndMetadata{offset=0, leaderEpoch=null, metadata=''},test.db_gb18030_test.tbl_test-0 -> OffsetAndMetadata{offset=12, leaderEpoch=null, metadata=''},test.db_gb18030_test.tbl_test-1 -> OffsetAndMetadata{offset=0, leaderEpoch=null, metadata=''}]
22/02/28 20:36:43 INFO BaseMetastoreTableOperations: Refreshing table metadata from new version: hdfs://hadoop:8020/user/hive/warehouse/db_gb18030_test.db/tbl_test/metadata/00001-d4b5d27b-a4d6-46e8-a131-b7ea8a6b5eea.metadata.json
22/02/28 20:36:43 INFO BaseMetastoreCatalog: Table loaded by catalog: hive.db_gb18030_test.tbl_test
22/02/28 20:36:43 INFO DDLHelper: table [hive.db_gb18030_test.tbl_test] schema changed, before [table {
  1: _src_name: optional string
  2: _src_db: optional string
  3: _src_table: optional string
  4: _src_ts_ms: optional long
  5: _src_server_id: optional long
  6: _src_file: optional string
  7: _src_pos: optional long
  8: _src_op: optional string
  9: _src_ts_ms_r: optional long
  10: _tsc_id: optional string
  11: _tsc_total_order: optional long
  12: _tsc_data_collection_order: optional long
  13: _kfk_topic: optional string
  14: _kfk_partition: optional int
  15: _kfk_offset: optional long
  16: _kfk_timestamp: optional long
  17: id: optional int
  18: c1: optional string
  19: c2: optional string
  20: c3: optional int
  21: c4: optional long
  22: create_time: optional long
  23: update_time: optional long
}]
22/02/28 20:36:43 INFO BaseMetastoreTableOperations: Successfully committed to table hive.db_gb18030_test.tbl_test in 179 ms
22/02/28 20:36:43 INFO BaseMetastoreTableOperations: Refreshing table metadata from new version: hdfs://hadoop:8020/user/hive/warehouse/db_gb18030_test.db/tbl_test/metadata/00002-751f9aac-1300-4185-b347-659be2063321.metadata.json
22/02/28 20:36:43 INFO BaseMetastoreCatalog: Table loaded by catalog: hive.db_gb18030_test.tbl_test
22/02/28 20:36:43 INFO DDLHelper: table [hive.db_gb18030_test.tbl_test] schema changed, after  [table {
  1: _src_name: optional string
  2: _src_db: optional string
  3: _src_table: optional string
  4: _src_ts_ms: optional long
  5: _src_server_id: optional long
  6: _src_file: optional string
  7: _src_pos: optional long
  8: _src_op: optional string
  9: _src_ts_ms_r: optional long
  10: _tsc_id: optional string
  11: _tsc_total_order: optional long
  12: _tsc_data_collection_order: optional long
  13: _kfk_topic: optional string
  14: _kfk_partition: optional int
  15: _kfk_offset: optional long
  16: _kfk_timestamp: optional long
  17: id: optional int
  18: c1: optional string
  19: c2: optional string
  20: c3: optional int
  21: c4: optional long
  24: c5: optional string
  22: create_time: optional long
  23: update_time: optional long
}]
22/02/28 20:36:43 INFO Kafka2Iceberg: After Init StatusAccumulator [Map(hive.db_gb18030_test.tbl_test -> StatusAccumulator(Map(test.db_gb18030_test.tbl_test:0 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test', partition: 0, range: [12 -> 0], curOffset: 12), test.db_gb18030_test.tbl_test:1 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test', partition: 1, range: [0 -> 0], curOffset: 0), test.db_gb18030_test.tbl_test:2 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test', partition: 2, range: [0 -> 0], curOffset: 0)), 2))]
22/02/28 20:36:43 INFO Kafka2Iceberg: After Init SchemaBroadcast   [List(SchemaBroadcast(hive.db_gb18030_test.tbl_test, SchemaBroadcast(Map({"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID","type":"int"},{"name":"C1","type":["null","string"],"default":null},{"name":"C2","type":{"type":"string","connect.default":"CV2"},"default":"CV2"},{"name":"C3","type":["null","int"],"default":null},{"name":"C4","type":["null","long"],"default":null},{"name":"C5","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test.Envelope"} -> 2, {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID","type":"int"},{"name":"C1","type":["null","string"],"default":null},{"name":"C2","type":{"type":"string","connect.default":"CV2"},"default":"CV2"},{"name":"C3","type":["null","int"],"default":null},{"name":"C4","type":["null","long"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test.Envelope"} -> 1), Map(1 -> {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID","type":"int"},{"name":"C1","type":["null","string"],"default":null},{"name":"C2","type":{"type":"string","connect.default":"CV2"},"default":"CV2"},{"name":"C3","type":["null","int"],"default":null},{"name":"C4","type":["null","long"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test.Envelope"}, 2 -> {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID","type":"int"},{"name":"C1","type":["null","string"],"default":null},{"name":"C2","type":{"type":"string","connect.default":"CV2"},"default":"CV2"},{"name":"C3","type":["null","int"],"default":null},{"name":"C4","type":["null","long"],"default":null},{"name":"C5","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test.Envelope"}))))]
22/02/28 20:36:43 INFO Kafka2Iceberg: Restarted StreamingContext ...

```




