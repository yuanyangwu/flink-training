# POJO-type event source

For event having multiple fields, we can have 2 choices

- Use TupleN to wrap fields. For example, ``Tuple2<LocalDateTime, Integer>``` in [Timestamped event source and monitoring](22_Timestamped_Event_Source_And_Monitoring.md)
- Create a POJO-type event class to wrap fields

We will create a source which generates POJO-type event.

class: yuanyangwu.flink.training.streaming.source.PojoSourceApp

## POJO-type

yuanyangwu.flink.training.element.PersonIncoming

## POJO-type event source

```java
public static class PersonSource implements SourceFunction<PersonIncoming> {
    private static final long serialVersionUID = 1L;
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<PersonIncoming> ctx) throws Exception {
        final String [] names = {"Tom", "John", "Alice", "Mary"};
        int counter = 0;
        while (isRunning) {
            LocalDateTime current = LocalDateTime.now(ZoneOffset.UTC);
            long epoch = FlinkTimestamp.fromLocalDateTime(current);
            ctx.collectWithTimestamp(new PersonIncoming(names[counter % names.length], counter), epoch);
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

```java
public class PojoSourceApp {
    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env
                    .setBufferTimeout(1000)
                    .setParallelism(1)
                    .setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            DataStreamSource<PersonIncoming> source = env.addSource(new PersonSource());
            source.addSink(new LogSink<>("PersonSink"));

            env.execute("PojoSourceApp");
        } catch (Exception e) {
            LOG.error("main", e);
        }
    }
}
```

Test result shows Person events are logged.

```console
21:56:26,258 INFO  yuanyangwu.flink.training.util.LogSink                        - PersonSink timestamp=2019-01-01T13:56:26.199 watermark=-9223372036854775808 value={person=Tom, incoming=0}
21:56:26,374 INFO  yuanyangwu.flink.training.util.LogSink                        - PersonSink timestamp=2019-01-01T13:56:26.374 watermark=2019-01-01T13:56:26.198 value={person=John, incoming=1}
21:56:26,487 INFO  yuanyangwu.flink.training.util.LogSink                        - PersonSink timestamp=2019-01-01T13:56:26.487 watermark=2019-01-01T13:56:26.198 value={person=Alice, incoming=2}
21:56:26,592 INFO  yuanyangwu.flink.training.util.LogSink                        - PersonSink timestamp=2019-01-01T13:56:26.592 watermark=2019-01-01T13:56:26.198 value={person=Mary, incoming=3}
21:56:26,703 INFO  yuanyangwu.flink.training.util.LogSink                        - PersonSink timestamp=2019-01-01T13:56:26.703 watermark=2019-01-01T13:56:26.198 value={person=Tom, incoming=4}
21:56:26,817 INFO  yuanyangwu.flink.training.util.LogSink                        - PersonSink timestamp=2019-01-01T13:56:26.817 watermark=2019-01-01T13:56:26.198 value={person=John, incoming=5}
21:56:26,929 INFO  yuanyangwu.flink.training.util.LogSink                        - PersonSink timestamp=2019-01-01T13:56:26.929 watermark=2019-01-01T13:56:26.198 value={person=Alice, incoming=6}
```
