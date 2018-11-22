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

- entrance
  - fromFile(), read CSV file and translate CSV into events and watermark
  - fromCollection(), read CSV from string collection, each string is a CSV row and translate CSV into events and watermark
- fromStream() do followings for CSV row
  - event row, shorten string and attach event timestamp
  - watermark row, call StringAssignerWithPunctuatedWatermarks() to extract watermark timestamp

```java
public class TimestampedCsvSource {
    private static Logger LOG = LoggerFactory.getLogger(TimestampedCsvSource.class);
    private static final String WATERMARK_PREFIX = "WATERMARK.";

    private static class StringAssignerWithPunctuatedWatermarks implements AssignerWithPunctuatedWatermarks<String> {
        private static final long serialVersionUID = 1L;
        private long timestamp = FlinkTimestamp.fromLocalDateTime(LocalDateTime.now(ZoneOffset.UTC));

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
            try {
                String[] fields = lastElement.split(",", 2);
                if (!fields[0].startsWith(WATERMARK_PREFIX)) {
                    return null;
                }
                String watermark = fields[0].substring(WATERMARK_PREFIX.length());
                LocalDateTime dateTime = LocalDateTime.parse(watermark);
                return new Watermark(FlinkTimestamp.fromLocalDateTime(dateTime));
            } catch (Exception e) {
                LOG.error("Parse watermark failure. row={}", lastElement, e);
            }
            return null;
        }

        @Override
        public long extractTimestamp(String element, long previousElementTimestamp) {
            try {
                String [] fields = element.split(",", 2);
                if (fields[0].startsWith(WATERMARK_PREFIX)) {
                    return timestamp;
                }
                LocalDateTime dateTime = LocalDateTime.parse(fields[0]);
                timestamp = FlinkTimestamp.fromLocalDateTime(dateTime);
            } catch (DateTimeException e) {
                LOG.error("Parse timestamp failure. row={}", element, e);
                // increase by 1 if first field is invalid
                timestamp++;
            }
            return timestamp;
        }
    }

    private static SingleOutputStreamOperator<String> fromStream(SingleOutputStreamOperator<String> stream) {
        return stream
                .assignTimestampsAndWatermarks(new StringAssignerWithPunctuatedWatermarks())
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String [] fields = value.split(",", 2);
                        if (fields.length < 2 || fields[0].startsWith(WATERMARK_PREFIX)) {
                            return;
                        }

                        out.collect(fields[1]);
                    }
                });
    }

    public static SingleOutputStreamOperator<String> fromCollection(StreamExecutionEnvironment env, Collection<String> input) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return fromStream(env.setParallelism(1).fromCollection(input));
    }

    public static SingleOutputStreamOperator<String> fromFile(StreamExecutionEnvironment env, String path) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return fromStream(env.setParallelism(1).readTextFile(path));
    }
}
```

## Call consumer in CsvSourceApp

- Call TimestampedCsvSource.fromFile() to generate string-type events
- Map each string event to PersonIncoming

```java
public class CsvSourceApp {
    private static Logger LOG = LoggerFactory.getLogger(CsvSourceApp.class);

    public static void main(String[] args) {
        try {
            final String PATH = "file:///C:/dev/work/flink-training/src/main/resources/person_incoming.csv";

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            SingleOutputStreamOperator<String> source = TimestampedCsvSource.fromFile(env, PATH);
            source
                    .map(new MapFunction<String, PersonIncoming>() {
                        @Override
                        public PersonIncoming map(String value) throws Exception {
                            String[] fields = value.split(",", 2);
                            return new PersonIncoming(fields[0], Integer.parseInt(fields[1]));
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

Test result shows

- 6 "csv" logs for CSV event rows
- "timestamp=<value>" is same as CSV event row's first field
- Because watermark row is after 2 event rows, "watermark=<value>" of first 2 events is invalid
- Event's "watermark=<value>" is same as previous watermark row

```console
10:11:19,543 INFO  yuanyangwu.flink.training.util.LogSink                        - csv timestamp=2018-11-08T13:00 watermark=-9223372036854775808 value={person=Mike, incoming=10}
10:11:19,544 INFO  yuanyangwu.flink.training.util.LogSink                        - csv timestamp=2018-11-08T13:00:00.100 watermark=-9223372036854775808 value={person=John, incoming=20}
10:11:19,545 INFO  yuanyangwu.flink.training.util.LogSink                        - csv timestamp=2018-11-08T13:00:00.200 watermark=2018-11-08T13:00:00.120 value={person=Mary, incoming=30}
10:11:19,545 INFO  yuanyangwu.flink.training.util.LogSink                        - csv timestamp=2018-11-08T13:00:00.300 watermark=2018-11-08T13:00:00.120 value={person=Mike, incoming=25}
10:11:19,545 INFO  yuanyangwu.flink.training.util.LogSink                        - csv timestamp=2018-11-08T13:00:00.400 watermark=2018-11-08T13:00:00.300 value={person=Mary, incoming=12}
10:11:19,546 INFO  yuanyangwu.flink.training.util.LogSink                        - csv timestamp=2018-11-08T13:00:00.500 watermark=2018-11-08T13:00:00.300 value={person=Jane, incoming=50}
```
