
# Spark Streaming 读取 Kafka 存储 Iceberg 数据湖

基于 Spark Streaming / Kafka Connect / Confluent Schema Register Server / Apache Avro / Iceberg 等相关组建,将数据实时同步到 Iceberg 中。  
选择 Hive Catalog 进行数据存储。  
支持 Iceberg 表结构自适应变动 （添加列/ 删除列）- 修改列暂不支持

## 数据格式示列
丢弃 Key 域的值，仅保留 value 域的值
```shell
# 原始 Avro 数据
offset = 12, key={"ID": 1}, value={"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID","type":"int"},{"name":"C1","type":["null","string"],"default":null},{"name":"C2","type":{"type":"string","connect.default":"CV2"},"default":"CV2"},{"name":"C3","type":["null","int"],"default":null},{"name":"C4","type":["null","long"],"default":null},{"name":"C5","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test.Envelope"} 
offset = 0, key={"ID": 2}, value={"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID","type":"int"},{"name":"C1","type":["null","string"],"default":null},{"name":"C2","type":{"type":"string","connect.default":"CV2"},"default":"CV2"},{"name":"C3","type":["null","int"],"default":null},{"name":"C4","type":["null","long"],"default":null},{"name":"C5","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test.Envelope"} 

# 存储结构数据
+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+----------------------------------------+----------------+--------------------------+-----------------------------+--------------+-----------+--------------+---+---+---+---+---+----+-------------+-------------+
|_src_name|_src_db        |_src_table|_src_ts_ms   |_src_server_id|_src_file       |_src_pos|_src_op|_src_ts_ms_r |_tsc_id                                 |_tsc_total_order|_tsc_data_collection_order|_kfk_topic                   |_kfk_partition|_kfk_offset|_kfk_timestamp|ID |C1 |C2 |C3 |C4 |C5  |CREATE_TIME  |UPDATE_TIME  |
+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+----------------------------------------+----------------+--------------------------+-----------------------------+--------------+-----------+--------------+---+---+---+---+---+----+-------------+-------------+
|test     |db_gb18030_test|tbl_test  |1645690498000|1             |mysql-bin.000010|9164    |c      |1645690498874|e45b718e-906f-11ec-89e3-0242c0a8640a:220|1               |1                         |test.db_gb18030_test.tbl_test|1             |0          |1645690499261 |2  |A1 |A2 |1  |1  |null|1645157193000|1645690414000|
|test     |db_gb18030_test|tbl_test  |1645690414000|1             |mysql-bin.000010|8775    |u      |1645690414404|e45b718e-906f-11ec-89e3-0242c0a8640a:219|1               |1                         |test.db_gb18030_test.tbl_test|0             |12         |1645690414488 |1  |v1 |v2 |5  |12 |null|1645157193000|1645690414000|
+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+----------------------------------------+----------------+--------------------------+-----------------------------+--------------+-----------+--------------+---+---+---+---+---+----+-------------+-------------+
```

## Schema  管理说明
Schema 信息 维护 (使用 Avro Schema 作为 Schema 信息的综合转化节点)  

1、 Iceberg Schema 产生： Avro Schema -> Iceberg Schema -> Spark Schema  
2、 Schema 更新： Old Avro Schema -> New Avro Schema -> Generate Iceberg DDL SQL -> Apply to Iceberg Table  
3、 Schema 更新检测:   
 - Avro GenericRecord Schema -> Schema HashCode   
 - SchemaAccumulator -> Schema Version -> Schema -> Schema HashCode  

## 附加数据信息
用户可根据需要, 配置附加所需的额外信息, 支持的列参数配置入下所示  
```properties
record.metadata.source.columns = name, db, table, ts_ms, server_id, file, pos
record.metadata.source.prefix = _src_
record.metadata.transaction.columns = id, total_order, data_collection_order
record.metadata.transaction.prefix = _tsc_
record.metadata.kafka.columns = topic, partition, offset, timestamp
record.metadata.kafka.prefix = _kfk_
```

## Schema 版本更新处理逻辑
- 使用 Kafka Commit Offset 来记录数据处理消费的位点信息。
- 程序启动时从 Kafka Server 中，获取前次最后 Commit Offset, 然后根据一定的处理逻辑获取到当前正在处理的 Schema 版本，并作为作为初始化时的 Schema 版本。
- 使用 Avro Schema HashCode 来快速检测被处理数据的 Schema 版本是发生变动。
- 如果 Schema HashCode 相等，则解析该记录, 且保存并记录该已完成解析的 Offset 位点(记为 curOffset), 该位点用于微 batch 结束后, 指定 Kafka 的 Commit Offset 位点。   
- 如果 Schema HashCode 不相等，则丢弃该记录, 且不更新上述所提及的当前完成处理的 Offset 位点(记为 curOffset)
- 当一个微 batch 批结束后, 立即检测 Kafka Topic 各个 Partition 当前完成的消费处理位点(curOffset), 并与该微批数据的起始位点(fromOffset)以及结束位点(untilOffset)进行对比, 判断是否需要更新当前的 Schema 版本。  
- 如果所有的数据分区都存在丢弃记录的情况(即 0 <= fromOffset < curOffset < untilOffset),则立即更新当前的Schema 版本, 否则使用当前 Schema 版本继续处理下一批次数据
- Schema 版本升级后,需回溯处理被之前的微批丢弃的新 Schema 版本数据（即需重新消费已经消费的数据）
- 由于 Spark Streaming 会保存上批次读取的 Kafka 数据的 untilOffset 位点, 并不检测该位点是否成功 Commit, 直接将其作为下一批次的起始位点(fromOffset),因此需要重启 Spark Steaming 进程, 来回溯重新读取之前被丢弃的数据
- 由于 Spark 是分布式计算框架, 因此构建了一个能在各个计算节点直接共享信息（当前的 Schema Version）的 Accumulator 类
入下图所示  
![docs/images/status-accumulator.png](docs/images/status-accumulator.png)  






## 执行日志示范 
### Schema 更新示例   
```text
22/02/28 20:29:10 INFO BaseMetastoreCatalog: Table loaded by catalog: hive.db_gb18030_test.tbl_test
22/02/28 20:29:10 INFO DDLHelper: table [hive.db_gb18030_test.tbl_test] schema changed, before [table {
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
22/02/28 20:30:17 INFO BaseMetastoreTableOperations: Successfully committed to table hive.db_gb18030_test.tbl_test in 5597 ms
22/02/28 20:30:43 INFO BaseMetastoreTableOperations: Refreshing table metadata from new version: hdfs://hadoop:8020/user/hive/warehouse/db_gb18030_test.db/tbl_test/metadata/00002-e37a7898-aeb8-4fd0-87a4-a8e5df132367.metadata.json
22/02/28 20:30:44 INFO BaseMetastoreCatalog: Table loaded by catalog: hive.db_gb18030_test.tbl_test
22/02/28 20:30:44 INFO DDLHelper: table [hive.db_gb18030_test.tbl_test] schema changed, after  [table {
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

```



```properties
{"source":{"version":"1.8.0.Final","connector":"mysql","name":"test","ts_ms":1645895161361,"snapshot":{"string":"false"},"db":"","sequence":null,"table":{"string":""},"server_id":1,"gtid":{"string":"e45b718e-906f-11ec-89e3-0242c0a8640a:233"},"file":"mysql-bin.000014","pos":6244,"row":0,"thread":null,"query":null},"databaseName":{"string":""},"schemaName":null,"ddl":{"string":"/* ApplicationName=DBeaver 21.3.4 - Main */ ALTER TABLE db_gb18030_test.tbl_test MODIFY COLUMN C5 varchar(50) CHARACTER SET gb18030 COLLATE gb18030_bin NULL"},"tableChanges":[]}
{"source":{"version":"1.8.0.Final","connector":"mysql","name":"test","ts_ms":1645895161354,"snapshot":{"string":"false"},"db":"","sequence":null,"table":{"string":""},"server_id":1,"gtid":{"string":"e45b718e-906f-11ec-89e3-0242c0a8640a:233"},"file":"mysql-bin.000014","pos":6244,"row":0,"thread":null,"query":null},"databaseName":{"string":""},"schemaName":null,"ddl":{"string":"/* ApplicationName=DBeaver 21.3.4 - Main */ ALTER TABLE db_gb18030_test.tbl_test MODIFY COLUMN C5 varchar(50) CHARACTER SET gb18030 COLLATE gb18030_bin NULL"},"tableChanges":[]}
{"source":{"version":"1.8.0.Final","connector":"mysql","name":"test","ts_ms":1645895161359,"snapshot":{"string":"false"},"db":"db_gb18030_test","sequence":null,"table":{"string":"tbl_test"},"server_id":1,"gtid":{"string":"e45b718e-906f-11ec-89e3-0242c0a8640a:233"},"file":"mysql-bin.000014","pos":6244,"row":0,"thread":null,"query":null},"databaseName":{"string":"db_gb18030_test"},"schemaName":null,"ddl":{"string":"ALTER TABLE db_gb18030_test.tbl_test MODIFY COLUMN C5 varchar(50) CHARACTER SET gb18030 COLLATE gb18030_bin NULL"},"tableChanges":[{"type":"ALTER","id":"\"db_gb18030_test\".\"tbl_test\"","table":{"defaultCharsetName":{"string":"gb18030"},"primaryKeyColumnNames":{"array":["ID"]},"columns":[{"name":"ID","jdbcType":4,"nativeType":null,"typeName":"INT","typeExpression":{"string":"INT"},"charsetName":null,"length":null,"scale":null,"position":1,"optional":{"boolean":false},"autoIncremented":{"boolean":true},"generated":{"boolean":true},"comment":null},{"name":"C1","jdbcType":12,"nativeType":null,"typeName":"VARCHAR","typeExpression":{"string":"VARCHAR"},"charsetName":{"string":"gb18030"},"length":{"int":45},"scale":null,"position":2,"optional":{"boolean":true},"autoIncremented":{"boolean":false},"generated":{"boolean":false},"comment":null},{"name":"C2","jdbcType":12,"nativeType":null,"typeName":"VARCHAR","typeExpression":{"string":"VARCHAR"},"charsetName":{"string":"gb18030"},"length":{"int":45},"scale":null,"position":3,"optional":{"boolean":false},"autoIncremented":{"boolean":false},"generated":{"boolean":false},"comment":null},{"name":"C3","jdbcType":4,"nativeType":null,"typeName":"INT","typeExpression":{"string":"INT"},"charsetName":null,"length":null,"scale":null,"position":4,"optional":{"boolean":true},"autoIncremented":{"boolean":false},"generated":{"boolean":false},"comment":null},{"name":"C4","jdbcType":-5,"nativeType":null,"typeName":"BIGINT","typeExpression":{"string":"BIGINT"},"charsetName":null,"length":null,"scale":null,"position":5,"optional":{"boolean":true},"autoIncremented":{"boolean":false},"generated":{"boolean":false},"comment":null},{"name":"C5","jdbcType":12,"nativeType":null,"typeName":"VARCHAR","typeExpression":{"string":"VARCHAR"},"charsetName":{"string":"gb18030"},"length":{"int":50},"scale":null,"position":6,"optional":{"boolean":true},"autoIncremented":{"boolean":false},"generated":{"boolean":false},"comment":null},{"name":"CREATE_TIME","jdbcType":93,"nativeType":null,"typeName":"DATETIME","typeExpression":{"string":"DATETIME"},"charsetName":null,"length":null,"scale":null,"position":7,"optional":{"boolean":false},"autoIncremented":{"boolean":false},"generated":{"boolean":false},"comment":null},{"name":"UPDATE_TIME","jdbcType":93,"nativeType":null,"typeName":"DATETIME","typeExpression":{"string":"DATETIME"},"charsetName":null,"length":null,"scale":null,"position":8,"optional":{"boolean":false},"autoIncremented":{"boolean":false},"generated":{"boolean":false},"comment":null}],"comment":null}}]}
```


before struct<6: ID: required int, 7: C1: optional string, 8: C2: required string, 9: C3: optional int, 10: C4: optional long, 11: C5: optional string, 12: CREATE_TIME: required long, 13: UPDATE_TIME: required long>, 
after struct<14: ID: required int, 15: C1: optional string, 16: C2: required string, 17: C3: optional int, 18: C4: optional long, 19: C5: optional string, 20: CREATE_TIME: required long, 21: UPDATE_TIME: required long>,
source struct<22: version: required string, 23: connector: required string, 24: name: required string, 25: ts_ms: required long, 26: snapshot: optional string, 27: db: required string, 28: sequence: optional string, 29: table: optional string, 30: server_id: required long, 31: gtid: optional string, 32: file: required string, 33: pos: required long, 34: row: required int, 35: thread: optional long, 36: query: optional string>, op string, ts_ms long, transaction struct<37: id: required string, 38: total_order: required long, 39: data_collection_order: required long>)




use db_gb18030_test;
drop table tbl_test;
drop database  db_gb18030_test



show databases;
use db_gb18030_test;
show tables;
drop table tbl_test;
drop database db_gb18030_test;

spark.sql("show databases").show
spark.sql("use db_gb18030_test")
spark.sql("show tables").show



CREATE TABLE hive.db_gb18030_test.tbl_test (
_src_name string, _src_db string, _src_table string, _src_ts_ms long, _src_server_id long, _src_file string, _src_pos long, _src_op string, _src_ts_ms_r long, _tsc_id string, _tsc_total_order long, _tsc_data_collection_order long, _kfk_topic string, _kfk_partition int, _kfk_offset long, _kfk_timestamp long, ID int, C1 string, C2 string, C3 int, C4 long, CREATE_TIME long, UPDATE_TIME long)
USING iceberg
PARTITIONED BY (c1)
LOCATION 'hdfs://hadoop:8020/user/hive/warehouse/hive.db_gb18030_test.tbl_test'
COMMENT 'db_gb18030_test tbl_test'
TBLPROPERTIES ('read.split.target-size'='268435456')



