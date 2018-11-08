# Source generating events from a CSV file

We will generate events from a CSV file.

class: yuanyangwu.flink.training.streaming.source.CsvSourceApp

## CSV file

```text
2018-11-08T13:00:00.000,Mike,10,
2018-11-08T13:00:00.100,John,20,
2018-11-08T13:00:00.200,Mary,30,
2018-11-08T13:00:00.300,Mike,25,
2018-11-08T13:00:00.400,Mary,12,
2018-11-08T13:00:00.500,Jane,50,
```

## Consume events

- Call ```readTextFile``` to load CSV content as String-type stream
- Call ```assignTimestampsAndWatermarks``` to assign timestamp and watermark by parsing CSV line's first field
- Call ```map``` to convert each line to a ```Person``` instance

```java
public class CsvSourceApp {
    public static void main(String[] args) {
        try {
            final String PATH = "file:///C:/dev/work/flink-training/src/main/resources/person_incoming.csv";

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env
                    .setBufferTimeout(1000)
                    .setParallelism(1)
                    .setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            DataStreamSource<String> source = env.readTextFile(PATH);
            source
                    .assignTimestampsAndWatermarks(new StringAssignerWithPunctuatedWatermarks())
                    .map(new MapFunction<String, Person>() {
                        @Override
                        public Person map(String value) throws Exception {
                            String[] fields = value.split(",", 4);
                            return new Person(fields[1], Integer.parseInt(fields[2]));
                        }
                    })
                    .addSink(new LogSink<>("csv"));

            env.execute("CsvSourceApp");
        } catch (Exception e) {
            LOG.error("main", e);
        }
    }
}
```

Parse CSV first field to assign timestamp and watermark

```java
private static class StringAssignerWithPunctuatedWatermarks implements AssignerWithPunctuatedWatermarks<String> {
    private static final long serialVersionUID = 1L;
    private long timestamp = FlinkTimestamp.fromLocalDateTime(LocalDateTime.now(ZoneOffset.UTC));

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
        return new Watermark(extractedTimestamp - 1);
    }

    @Override
    public long extractTimestamp(String element, long previousElementTimestamp) {
        try {
            String [] fields = element.split(",", 2);
            LocalDateTime dateTime = LocalDateTime.parse(fields[0]);
            timestamp = FlinkTimestamp.fromLocalDateTime(dateTime);
        } catch (DateTimeException e) {
            // increase by 1 if first field is invalid
            timestamp++;
        }
        return timestamp;
    }
}
```

Test result shows

- "timestamp=<value>" is same as CSV first field
- first watermark is invalid and others are valid
- CSV line is mapped to ```Person```

```console
22:20:55,743 INFO  yuanyangwu.flink.training.util.LogSink                        - csv timestamp=2018-11-08T13:00 watermark=-9223372036854775808 value=Person{name=Mike, incoming=10}
22:20:55,744 INFO  yuanyangwu.flink.training.util.LogSink                        - csv timestamp=2018-11-08T13:00:00.100 watermark=2018-11-08T12:59:59.999 value=Person{name=John, incoming=20}
22:20:55,744 INFO  yuanyangwu.flink.training.util.LogSink                        - csv timestamp=2018-11-08T13:00:00.200 watermark=2018-11-08T13:00:00.099 value=Person{name=Mary, incoming=30}
22:20:55,744 INFO  yuanyangwu.flink.training.util.LogSink                        - csv timestamp=2018-11-08T13:00:00.300 watermark=2018-11-08T13:00:00.199 value=Person{name=Mike, incoming=25}
22:20:55,745 INFO  yuanyangwu.flink.training.util.LogSink                        - csv timestamp=2018-11-08T13:00:00.400 watermark=2018-11-08T13:00:00.299 value=Person{name=Mary, incoming=12}
22:20:55,745 INFO  yuanyangwu.flink.training.util.LogSink                        - csv timestamp=2018-11-08T13:00:00.500 watermark=2018-11-08T13:00:00.399 value=Person{name=Jane, incoming=50}
```
