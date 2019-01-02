# Table Source and Sink

## Integer source

yuanyangwu.flink.training.table.IntegerTableSourceApp

Schema

```text
root
 |-- value: Integer
```

Output

```text
12:04:07,984 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - Source: Custom Source -> (Sink: Unnamed, from: (value) -> to: Integer -> Sink: Unnamed) (1/1) (e49240450161c1fcdff5e53669914507) switched from DEPLOYING to RUNNING.
12:04:07,988 INFO  org.apache.flink.streaming.runtime.tasks.StreamTask           - No state backend has been configured, using default (Memory / JobManager) MemoryStateBackend (data in heap memory / checkpoints to JobManager) (checkpoints: 'null', savepoints: 'null', asynchronous: TRUE, maxStateSize: 5242880)
12:04:08,107 INFO  yuanyangwu.flink.training.util.LogSink                        - source timestamp=null watermark=null value=1
12:04:08,108 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value=1
12:04:08,209 INFO  yuanyangwu.flink.training.util.LogSink                        - source timestamp=null watermark=null value=2
12:04:08,209 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value=2
...

12:04:18,161 INFO  yuanyangwu.flink.training.util.LogSink                        - source timestamp=null watermark=null value=99
12:04:18,161 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value=99
12:04:18,262 INFO  yuanyangwu.flink.training.util.LogSink                        - source timestamp=null watermark=null value=0
12:04:18,262 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value=0
12:04:18,362 INFO  yuanyangwu.flink.training.util.LogSink                        - source timestamp=null watermark=null value=1
12:04:18,363 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value=1
```

## Timestamped source

yuanyangwu.flink.training.table.TimestampedTableSourceApp

Use "TypeInformation.of(new TypeHint<T>(){})" to get tuple TypeInformation.

```text
final DataStream<Tuple2<LocalDateTime, Integer>> output = tEnv.toAppendStream(table, TypeInformation.of(new TypeHint<Tuple2<LocalDateTime, Integer>>(){}));
```

Schema

```text
root
 |-- timestamp: GenericType<java.time.LocalDateTime>
 |-- value: Integer
```

Output

```text
21:46:28,267 INFO  yuanyangwu.flink.training.util.LogSink                        - source timestamp=null watermark=null value=(2019-01-01T13:46:27.956,0)
21:46:28,292 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value=(2019-01-01T13:46:27.956,0)
21:46:28,406 INFO  yuanyangwu.flink.training.util.LogSink                        - source timestamp=null watermark=null value=(2019-01-01T13:46:28.404,1)
21:46:28,407 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value=(2019-01-01T13:46:28.404,1)
...
21:46:39,176 INFO  yuanyangwu.flink.training.util.LogSink                        - source timestamp=null watermark=null value=(2019-01-01T13:46:39.176,99)
21:46:39,176 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value=(2019-01-01T13:46:39.176,99)
21:46:39,285 INFO  yuanyangwu.flink.training.util.LogSink                        - source timestamp=null watermark=null value=(2019-01-01T13:46:39.285,0)
21:46:39,285 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value=(2019-01-01T13:46:39.285,0)
```

## Pojo source

yuanyangwu.flink.training.table.PojoTableSourceApp

Schema

```text
root
 |-- incoming: Integer
 |-- person: String
```

Output

```text
21:52:32,777 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - Source: Custom Source -> (Sink: Unnamed, from: (incoming, person) -> to: PersonIncoming -> Sink: Unnamed) (1/1) (d5ae18a53a7bbfaf797eedbf22f2123f) switched from DEPLOYING to RUNNING.
21:52:32,781 INFO  org.apache.flink.streaming.runtime.tasks.StreamTask           - No state backend has been configured, using default (Memory / JobManager) MemoryStateBackend (data in heap memory / checkpoints to JobManager) (checkpoints: 'null', savepoints: 'null', asynchronous: TRUE, maxStateSize: 5242880)
21:52:33,095 INFO  yuanyangwu.flink.training.util.LogSink                        - source timestamp=null watermark=null value={person=Tom, incoming=0}
21:52:33,102 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value={person=Tom, incoming=0}
21:52:33,211 INFO  yuanyangwu.flink.training.util.LogSink                        - source timestamp=null watermark=null value={person=John, incoming=1}
21:52:33,211 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value={person=John, incoming=1}
21:52:33,325 INFO  yuanyangwu.flink.training.util.LogSink                        - source timestamp=null watermark=null value={person=Alice, incoming=2}
21:52:33,325 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value={person=Alice, incoming=2}
21:52:33,430 INFO  yuanyangwu.flink.training.util.LogSink                        - source timestamp=null watermark=null value={person=Mary, incoming=3}
21:52:33,431 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value={person=Mary, incoming=3}
```

## CSV source

yuanyangwu.flink.training.table.CsvTableSourceApp

Schema

```text
root
 |-- Timestamp: String
 |-- Person: String
 |-- Incoming: Integer
```

Output

```text
07:48:31,159 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value=2018-11-08T13:00:00.100,John,20
07:48:31,159 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value=2018-11-08T13:00:00.200,Mary,30
07:48:31,159 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value=2018-11-08T13:00:00.400,Mary,12
07:48:31,159 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value=2018-11-08T13:00:00.500,Jane,50
07:48:31,159 INFO  org.apache.flink.api.common.io.GenericCsvInputFormat          - In file "file:/C:/dev/work/flink-training/src/main/resources/person_incoming.csv" (split start: 0) 3 comment line(s) were skipped.
07:48:31,159 INFO  org.apache.flink.runtime.taskmanager.Task                     - CsvTableSource(read fields: Timestamp, Person, Incoming) -> Map -> where: (<>(Person, _UTF-16LE'Mike')), select: (Timestamp, Person, Incoming) -> to: Row -> Sink: Unnamed (1/1) (769344559df0ad395b2e6ec7a64f3c6c) switched from RUNNING to FINISHED.
```
