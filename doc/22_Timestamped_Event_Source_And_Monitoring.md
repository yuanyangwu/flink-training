# Timestamped Event Source and Monitoring

Apache Flink supports 3 kinds of time.

- processing time, time when a Flink cluster machine processes an event.
- event time, time when an event is produced.
- ingestion time, time when Flink cluster receives an event.

Processing and ingestion time varies with different Flink and network to Flink. Event time is the best although it may be hard to generate precise timestamp for events. To adopt event time, events should include timestamp in event data. Then Flink can extract related timestamp.

Flink Java API document [AssignerWithPunctuatedWatermarks](https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/streaming/api/functions/AssignerWithPunctuatedWatermarks.html) says, "Timestamps and watermarks are defined as longs that represent the **milliseconds** since the Epoch (midnight, January 1, 1970 UTC). A watermark with a certain value t indicates that no elements with event timestamps x, where x is lower or equal to t, will occur any more."

We will create a source with events including millisecond epoch timestamp and periodic watermark.

classes:

- yuanyangwu.flink.training.streaming.source.TimestampedSourceApp
- yuanyangwu.flink.training.util.FlinkTimestamp, utility functions to convert between LocalDateTime and long-type millisecond epoch timestamp
- yuanyangwu.flink.training.util.LogSink, Flink sink which logs events and related timestamp/watermark

## Timestamped event source

- Send an event every 100 ms
- Send a watermark every 10 events
- Event a tuple of LocalDateTime and an integer. LocalDateTime is event timestamp. Use LocalDateTime instead of Long because Tuple2.toString() can print LocalDateTime in the human friend format.

```java
public static class TimestampedIntegerSource implements SourceFunction<Tuple2<LocalDateTime, Integer>> {
    private static final long serialVersionUID = 1L;
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Tuple2<LocalDateTime, Integer>> ctx) throws Exception {
        int counter = 0;
        while (isRunning) {
            LocalDateTime current = LocalDateTime.now(ZoneOffset.UTC);
            long epoch = FlinkTimestamp.fromLocalDateTime(current);
            ctx.collectWithTimestamp(Tuple2.of(current, counter), epoch);
            if (counter % 10 == 0) {
                ctx.emitWatermark(new Watermark(epoch - 1));
            }
            counter = (counter + 1) % 100;
            Thread.sleep(100L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
```

## Consume events

### Print event via print()

- Use ```env.setParallelism(1)``` to print unordered events for multi-threading
- Use ```env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)``` to ask Flink to use event time.
- Use ```print()``` as sink to print events to console

```java
public class TimestampedSourceApp {
    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env
                    .setBufferTimeout(1000)
                    .setParallelism(1)
                    .setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            DataStreamSource<Tuple2<LocalDateTime, Integer>> source = env.addSource(new TimestampedIntegerSource());
            source.print();

            env.execute("TimestampedSourceApp");
        } catch (Exception e) {
            LOG.error("main", e);
        }
    }
}
```

Run ```TimestampedSourceApp.main()```

```console
10:59:11,471 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - Job TimestampedSourceApp (640c19e3d4b0b04e98bf12cdbcd43aa9) switched from state CREATED to RUNNING.

10:59:11,677 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - Source: Custom Source -> Sink: Print to Std. Out (1/1) (14bc53c179eb321803fa854138cfecfe) switched from DEPLOYING to RUNNING.
10:59:11,687 INFO  org.apache.flink.streaming.runtime.tasks.StreamTask           - No state backend has been configured, using default (Memory / JobManager) MemoryStateBackend (data in heap memory / checkpoints to JobManager) (checkpoints: 'null', savepoints: 'null', asynchronous: TRUE, maxStateSize: 5242880)
(2018-11-07T02:59:11.798,0)
(2018-11-07T02:59:12.393,1)
(2018-11-07T02:59:12.494,2)
(2018-11-07T02:59:12.594,3)
```

```print()``` has following limitation.

- Only for development. Do not work in production cluster.
- Only prints event tuple content
- Do not print event timestamp if event does not include event timestamp
- Do not print watermark 

### Print event and timestamp via process()

[Stackover](https://stackoverflow.com/questions/49107932/apache-flink-how-to-get-timestamp-of-events-in-ingestion-time-mode) recommends ```ProcessFunction``` to retrieve event timestamp. So I insert ```process()``` before sink.

```java
public class TimestampedSourceApp {
    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env
                    .setBufferTimeout(1000)
                    .setParallelism(1)
                    .setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            DataStreamSource<Tuple2<LocalDateTime, Integer>> source = env.addSource(new TimestampedIntegerSource());
            source
                    .process(new ProcessFunction<Tuple2<LocalDateTime, Integer>, Tuple2<LocalDateTime, Integer>>() {
                        @Override
                        public void processElement(Tuple2<LocalDateTime, Integer> value, Context ctx, Collector<Tuple2<LocalDateTime, Integer>> out) throws Exception {
                            LOG.info("timestamp={} value={}", FlinkTimestamp.toLocalDateTime(ctx.timestamp()), value);
                            out.collect(value);
                        }
                    })
                    .print();

            env.execute("TimestampedSourceApp");
        } catch (Exception e) {
            LOG.error("main", e);
        }
    }
}
```

Test result shows

- Event timestamp is retrieved by ```ProcessFunction.Context.timestamp()```
- Event timestamp is same as timestamp of event tuple. It proves that Flink timestamp takes effect.
- Event watermark is missing because ```ProcessFunction.Context``` has no watermark
- Both process() and print() print events. That makes console output messy

```console
11:23:01,132 INFO  yuanyangwu.flink.training.streaming.source.TimestampedSourceApp  - timestamp=2018-11-07T03:23:00.762 value=(2018-11-07T03:23:00.762,0)
(2018-11-07T03:23:00.762,0)
11:23:01,240 INFO  yuanyangwu.flink.training.streaming.source.TimestampedSourceApp  - timestamp=2018-11-07T03:23:01.240 value=(2018-11-07T03:23:01.240,1)
(2018-11-07T03:23:01.240,1)
11:23:01,341 INFO  yuanyangwu.flink.training.streaming.source.TimestampedSourceApp  - timestamp=2018-11-07T03:23:01.341 value=(2018-11-07T03:23:01.341,2)
(2018-11-07T03:23:01.341,2)
```

### Print event, timestamp and watermark via LogSink

To resolve process() shortcomings, I add a new sink class LogSink.

- LogSink.invoke() logs event, timestamp and watermark because SinkFunction.Context can access both event timestamp and watermark
- LogSink has a template class. The type argument is to support different event types.
- LogSink constructor requires "prefix", which is logged with event. That makes it easy to filter a sink related logs.

```java
public class LogSink<IN> implements SinkFunction<IN> {
    private static Logger LOG = LoggerFactory.getLogger(LogSink.class);

    private static final long serialVersionUID = 1L;
    private String prefix;

    public LogSink(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        if (context.timestamp() == null) {
            LOG.info("{} timestamp=null watermark=null value={}",
                    prefix,
                    value);
        } else if (context.currentWatermark() < 0) {
            LOG.info("{} timestamp={} watermark={} value={}",
                    prefix,
                    FlinkTimestamp.toLocalDateTime(context.timestamp()),
                    context.currentWatermark(),
                    value);
        } else {
            LOG.info("{} timestamp={} watermark={} value={}",
                    prefix,
                    FlinkTimestamp.toLocalDateTime(context.timestamp()),
                    FlinkTimestamp.toLocalDateTime(context.currentWatermark()),
                    value);
        }
    }
}
```

Replace print() with LogSink.

```java
public class TimestampedSourceApp {
    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env
                    .setBufferTimeout(1000)
                    .setParallelism(1)
                    .setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            DataStreamSource<Tuple2<LocalDateTime, Integer>> source = env.addSource(new TimestampedIntegerSource());
            source.addSink(new LogSink<Tuple2<LocalDateTime, Integer>>("sink1"));

            env.execute("TimestampedSourceApp");
        } catch (Exception e) {
            LOG.error("main", e);
        }
    }
}
```

Test result shows

- Event timestamp and watermark is logged.
- Event timestamp is same as timestamp of event tuple. It proves that Flink timestamp takes effect.
- First event watermark is invalid and other event watermark is valid.
- Watermark is increased every 10 events. It follows the code of "Send a watermark every 10 events"

```console
16:26:12,844 INFO  yuanyangwu.flink.training.util.LogSink                        - sink1 timestamp=2018-11-07T08:26:12.346 watermark=-9223372036854775808 value=(2018-11-07T08:26:12.346,0)
16:26:12,946 INFO  yuanyangwu.flink.training.util.LogSink                        - sink1 timestamp=2018-11-07T08:26:12.946 watermark=2018-11-07T08:26:12.345 value=(2018-11-07T08:26:12.946,1)
16:26:13,046 INFO  yuanyangwu.flink.training.util.LogSink                        - sink1 timestamp=2018-11-07T08:26:13.046 watermark=2018-11-07T08:26:12.345 value=(2018-11-07T08:26:13.046,2)
...
16:26:13,855 INFO  yuanyangwu.flink.training.util.LogSink                        - sink1 timestamp=2018-11-07T08:26:13.855 watermark=2018-11-07T08:26:12.345 value=(2018-11-07T08:26:13.855,10)
16:26:13,956 INFO  yuanyangwu.flink.training.util.LogSink                        - sink1 timestamp=2018-11-07T08:26:13.956 watermark=2018-11-07T08:26:13.854 value=(2018-11-07T08:26:13.956,11)
```

LogSink is an useful utility. Unlike print(), LogSink can be used in Flink production cluster. We can monitor every source and operator by appending LogSink to sources and operators. Each LogSink instance can set unique prefix to filter events later.
