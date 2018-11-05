# Simple-type event source

We will create a Flink source, which generates integer event every 100ms.

class: yuanyangwu.flink.training.streaming.source.IntegerSourceApp

## Integer Source

```java
    public static class IntegerSource implements SourceFunction<Integer> {
        private static final long serialVersionUID = 1L;
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            int counter = 0;
            while (isRunning) {
                counter = (counter + 1) % 100;
                ctx.collect(counter);
                Thread.sleep(100L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
```

## Consume Source in an Application

```java
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env
            .setBufferTimeout(1000)
            .setParallelism(1);

    DataStreamSource<Integer> source = env.addSource(new IntegerSource());
    source.print();

    env.execute("IntegerSourceApp");
```

## Run application in IntelliJ IDEA

```console
20:25:15,917 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - Job IntegerSourceApp (cd1fc5b7abae032852d9de419c5be6df) switched from state CREATED to RUNNING.

20:25:16,052 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - Source: Custom Source -> Sink: Print to Std. Out (1/1) (e08007ae72d28514a476a9f2a79f60b3) switched from DEPLOYING to RUNNING.

1
2
3
```
