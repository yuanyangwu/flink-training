# Test operator map

We will test DataStream's ```public <R> SingleOutputStreamOperator<R> map(MapFunction<T, R> mapper)```

MapFunction class

```java
private static class LongStringMapFunction implements MapFunction<Long, String> {
    @Override
    public String map(Long value) throws Exception {
        return Long.toString(value * 2);
    }
}
```

Unit test only tests MapFunction class.

```java
public class MapTest {
    @Test
    public void mapUnitTest() throws Exception {
        assertEquals("24", new LongStringMapFunction().map(12L));
    }
}
```

End-to-end integration test tests MapFunction with Flink runtime.

```java
public class MapTest {
    @Test
    public void mapIntegrationTest() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final SingleOutputStreamOperator<String> stream = env
                .setParallelism(1) // set parallelism = 1 to avoid orderless by multi-thread
                .fromElements(1L, 21L, 22L)
                .map(new LongStringMapFunction());

        // convert stream to list
        final Iterator<String> iterator = DataStreamUtils.collect(stream);
        List<String> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);
        
        assertEquals(Arrays.asList("2", "42", "44"), result);
    }
}
```

"mapUnitTest" costs 24ms. But "mapIntegrationTest" costs 6.5s for the Flink run-time cost.
