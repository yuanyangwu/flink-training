# Table Source and Sink

- CSV source

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
