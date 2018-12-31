# Simple Table Application

## Batch table application

yuanyangwu.flink.training.table.BatchTableApp

## Stream table application

yuanyangwu.flink.training.table.StreamTableApp

Specify table field names.

```text
final DataStreamSource<Integer> source = env.fromElements(1, 2, 3);
final Table table = tEnv.fromDataStream(source, "value");

root
 |-- value: Integer
```

Output

```text
21:20:00,615 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value=1
21:20:00,615 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value=2
21:20:00,616 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value=3
```

yuanyangwu.flink.training.table.CsvTableApp

Pojo field names.

```text
final SingleOutputStreamOperator<PersonIncoming> stream = source
        .map(new MapFunction<String, PersonIncoming>() {
            ...
        });

final Table table = tEnv.fromDataStream(stream);
table.printSchema();

root
 |-- incoming: Integer
 |-- person: String
```

Output does not pass timestamp and watermark to table output.
"csv" log is the source stream. "table" log is the output table stream.

```text
21:23:08,156 INFO  yuanyangwu.flink.training.util.LogSink                        - csv timestamp=2018-11-08T13:00 watermark=-9223372036854775808 value=Mike,10
21:23:08,157 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value={person=Mike, incoming=10}
21:23:08,157 INFO  yuanyangwu.flink.training.util.LogSink                        - csv timestamp=2018-11-08T13:00:00.100 watermark=-9223372036854775808 value=John,20
21:23:08,158 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value={person=John, incoming=20}
21:23:08,158 INFO  yuanyangwu.flink.training.util.LogSink                        - csv timestamp=2018-11-08T13:00:00.200 watermark=2018-11-08T13:00:00.120 value=Mary,30
21:23:08,158 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value={person=Mary, incoming=30}
21:23:08,159 INFO  yuanyangwu.flink.training.util.LogSink                        - csv timestamp=2018-11-08T13:00:00.300 watermark=2018-11-08T13:00:00.120 value=Mike,25
21:23:08,159 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value={person=Mike, incoming=25}
21:23:08,159 INFO  yuanyangwu.flink.training.util.LogSink                        - csv timestamp=2018-11-08T13:00:00.400 watermark=2018-11-08T13:00:00.300 value=Mary,12
21:23:08,160 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value={person=Mary, incoming=12}
21:23:08,160 INFO  yuanyangwu.flink.training.util.LogSink                        - csv timestamp=2018-11-08T13:00:00.500 watermark=2018-11-08T13:00:00.300 value=Jane,50
21:23:08,161 INFO  yuanyangwu.flink.training.util.LogSink                        - table timestamp=null watermark=null value={person=Jane, incoming=50}
```