# POJO-type event source

For event having multiple fields, we can have 2 choices

- Use TupleN to wrap fields. For example, ``Tuple2<LocalDateTime, Integer>``` in [Timestamped event source and monitoring](22_Timestamped_Event_Source_And_Monitoring.md)
- Create a POJO-type event class to wrap fields

We will create a source which generates POJO-type event.

class: yuanyangwu.flink.training.streaming.source.PojoSourceApp

## POJO-type

```java
public static class Person  {
    public String name;
    public Integer incoming;

    public Person(String name, Integer incoming) {
        this.name = name;
        this.incoming = incoming;
    }

    @Override
    public String toString() {
        return "Person{name=" + name + ", incoming=" + incoming + "}";
    }
}
```

## POJO-type event source

```java
public static class PersonSource implements SourceFunction<Person> {
    private static final long serialVersionUID = 1L;
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Person> ctx) throws Exception {
        final String [] names = {"Tom", "John", "Alice", "Mary"};
        int counter = 0;
        while (isRunning) {
            LocalDateTime current = LocalDateTime.now(ZoneOffset.UTC);
            long epoch = FlinkTimestamp.fromLocalDateTime(current);
            ctx.collectWithTimestamp(new Person(names[counter % names.length], counter), epoch);
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

            DataStreamSource<Person> source = env.addSource(new PersonSource());
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
18:03:08,452 INFO  yuanyangwu.flink.training.util.LogSink                        - PersonSink timestamp=2018-11-07T10:03:07.833 watermark=-9223372036854775808 value=Person{name=Tom, incoming=0}
18:03:08,553 INFO  yuanyangwu.flink.training.util.LogSink                        - PersonSink timestamp=2018-11-07T10:03:08.553 watermark=2018-11-07T10:03:07.832 value=Person{name=John, incoming=1}
18:03:08,654 INFO  yuanyangwu.flink.training.util.LogSink                        - PersonSink timestamp=2018-11-07T10:03:08.654 watermark=2018-11-07T10:03:07.832 value=Person{name=Alice, incoming=2}
18:03:08,755 INFO  yuanyangwu.flink.training.util.LogSink                        - PersonSink timestamp=2018-11-07T10:03:08.755 watermark=2018-11-07T10:03:07.832 value=Person{name=Mary, incoming=3}
18:03:08,855 INFO  yuanyangwu.flink.training.util.LogSink                        - PersonSink timestamp=2018-11-07T10:03:08.855 watermark=2018-11-07T10:03:07.832 value=Person{name=Tom, incoming=4}
```