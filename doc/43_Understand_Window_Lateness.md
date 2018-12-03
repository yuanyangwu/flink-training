# Understand Apache Flink Window Lateness

Events may not arrive at time order for various reasons. Events may arrive after a window ends. Apache Flink introduces "lateness" option to control when events are added to a window or dropped. A window stream can control "lateness" by calling allowedLateness().

```text
stream
       .keyBy(...)               <-  keyed versus non-keyed windows
       .window(...)              <-  required: "assigner"
      [.allowedLateness(...)]    <-  optional: "lateness" (else zero)
```

note: "lateness" only makes sense for event time. Processing time and ingestion time is related to stream processing application and has no "lateness" concept.

Open questions:

If window stream does not call allowedLateness(), it means lateness is 0. If any window related event arrives after window ends, it is dropped.

Q1: What does "window ends" mean?

If window stream call allowedLateness() with positive lateness value, a windows can accept "late" events instead of dropping them.

Q2: How does Flink decide whether a window can accept an event?

We will test to find out the answers to above questions. 

## Test lateness

### Test setup

I use TimestampedCsvSource to generate an event stream with injected watermarks.

```java
public class WindowAllowedLatenessTest {
    private SingleOutputStreamOperator<Tuple2<String, Long>> orig;

    @Before
    public void setup() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        orig = TimestampedCsvSource.fromCollection(
                env,
                Arrays.asList(
                        "2018-11-08T13:00:00.000,Mike,0",
                        "2018-11-08T13:00:00.010,Mike,10",
                        "2018-11-08T13:00:00.090,Mike,9000000000",
                        "2018-11-08T13:00:00.100,Mike,10000000000",
                        "WATERMARK.2018-11-08T13:00:00.099",
                        "2018-11-08T13:00:00.020,Mike,200",
                        "2018-11-08T13:00:00.030,Mike,3000",
                        "WATERMARK.2018-11-08T13:00:00.108",
                        "2018-11-08T13:00:00.040,Mike,40000",
                        "WATERMARK.2018-11-08T13:00:00.109",
                        "2018-11-08T13:00:00.050,Mike,500000",
                        "2018-11-08T13:00:00.060,Mike,6000000",
                        "2018-11-08T13:00:00.070,Mike,70000000",
                        "2018-11-08T13:00:00.080,Mike,800000000",
                        "2018-11-08T13:00:00.110,Mike,10000000010",
                        "2018-11-08T13:00:00.200,Mike,200000000000",
                        "2018-11-08T13:00:00.190,Mike,19000000000",
                        "WATERMARK.2018-11-08T13:00:00.199",
                        "2018-11-08T13:00:00.120,Mike,10000000200",
                        "2018-11-08T13:00:00.130,Mike,10000003000",
                        "WATERMARK.2018-11-08T13:00:00.208",
                        "2018-11-08T13:00:00.140,Mike,10000040000",
                        "WATERMARK.2018-11-08T13:00:00.209",
                        "2018-11-08T13:00:00.150,Mike,10000500000",
                        "2018-11-08T13:00:00.160,Mike,10006000000",
                        "2018-11-08T13:00:00.170,Mike,10070000000",
                        "2018-11-08T13:00:00.180,Mike,10800000000"
                ))
                .map(new CsvStringTuple2MapFunction())
                .map(new MapFunction<Tuple2<String, String>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Tuple2<String, String> value) throws Exception {
                        return Tuple2.of(value.f0, Long.valueOf(value.f1));
                    }
                });
    }
}
```

Here is the generated stream. The 2nd part of event tuple value is Long type. Event timestamp and the Long value is correlated. For example, 2018-11-08T13:00:00.020 related value is "200". Such correlation makes it easy for us to check what event is accepted by window later.

```text
orig   timestamp=2018-11-08T13:00 watermark=-9223372036854775808 value=(Mike,0)
orig   timestamp=2018-11-08T13:00:00.010 watermark=-9223372036854775808 value=(Mike,10)
orig   timestamp=2018-11-08T13:00:00.090 watermark=-9223372036854775808 value=(Mike,9000000000)
orig   timestamp=2018-11-08T13:00:00.100 watermark=-9223372036854775808 value=(Mike,10000000000)
orig   timestamp=2018-11-08T13:00:00.020 watermark=2018-11-08T13:00:00.099 value=(Mike,200)
orig   timestamp=2018-11-08T13:00:00.030 watermark=2018-11-08T13:00:00.099 value=(Mike,3000)
orig   timestamp=2018-11-08T13:00:00.040 watermark=2018-11-08T13:00:00.108 value=(Mike,40000)
orig   timestamp=2018-11-08T13:00:00.050 watermark=2018-11-08T13:00:00.109 value=(Mike,500000)
orig   timestamp=2018-11-08T13:00:00.060 watermark=2018-11-08T13:00:00.109 value=(Mike,6000000)
orig   timestamp=2018-11-08T13:00:00.070 watermark=2018-11-08T13:00:00.109 value=(Mike,70000000)
orig   timestamp=2018-11-08T13:00:00.080 watermark=2018-11-08T13:00:00.109 value=(Mike,800000000)
orig   timestamp=2018-11-08T13:00:00.110 watermark=2018-11-08T13:00:00.109 value=(Mike,10000000010)
orig   timestamp=2018-11-08T13:00:00.200 watermark=2018-11-08T13:00:00.109 value=(Mike,200000000000)
orig   timestamp=2018-11-08T13:00:00.190 watermark=2018-11-08T13:00:00.109 value=(Mike,19000000000)
orig   timestamp=2018-11-08T13:00:00.120 watermark=2018-11-08T13:00:00.199 value=(Mike,10000000200)
orig   timestamp=2018-11-08T13:00:00.130 watermark=2018-11-08T13:00:00.199 value=(Mike,10000003000)
orig   timestamp=2018-11-08T13:00:00.140 watermark=2018-11-08T13:00:00.208 value=(Mike,10000040000)
orig   timestamp=2018-11-08T13:00:00.150 watermark=2018-11-08T13:00:00.209 value=(Mike,10000500000)
orig   timestamp=2018-11-08T13:00:00.160 watermark=2018-11-08T13:00:00.209 value=(Mike,10006000000)
orig   timestamp=2018-11-08T13:00:00.170 watermark=2018-11-08T13:00:00.209 value=(Mike,10070000000)
orig   timestamp=2018-11-08T13:00:00.180 watermark=2018-11-08T13:00:00.209 value=(Mike,10800000000)
```

### Test default window lateness (lateness=0)

Use 100ms tumbling window and apply sum operator to sum up the Long value 

```java
public class WindowAllowedLatenessTest {
    @Test
    public void noLatenessTest() throws Exception {
        final SingleOutputStreamOperator<Tuple2<String, Long>> stream = orig
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(100L)))
                //.allowedLateness(Time.milliseconds(0L))
                .sum(1);

        orig.addSink(new LogSink<>("orig  "));
        stream.addSink(new LogSink<>("window"));

        assertEquals(3, TestUtil.streamToCollection(stream).size());
    }
}
```

It has 3 tumbling windows.

- <span style="background-color:lightblue">window000</span> is tumbling window [2018-11-08T13:00:00.000, 2018-11-08T13:00:00.099]
- <span style="background-color:yellow">window100</span> is tumbling window [2018-11-08T13:00:00.100, 2018-11-08T13:00:00.199]
- <span style="background-color:pink">window200</span> is tumbling window [2018-11-08T13:00:00.200, 2018-11-08T13:00:00.299]

The "window" log is the window sum resulting stream.

```text
window timestamp=2018-11-08T13:00:00.099 watermark=-9223372036854775808 value=(Mike,9000000010)
window timestamp=2018-11-08T13:00:00.199 watermark=2018-11-08T13:00:00.109 value=(Mike,39000000010)
window timestamp=2018-11-08T13:00:00.299 watermark=2018-11-08T13:00:00.209 value=(Mike,200000000000)
```

With the Long value, we can label each event with related window accepted/dropped.

- [<span style="background-color:lightblue">window000</span>, <span style="background-color:lightgreen">accepted</span>] orig   timestamp=2018-11-08T13:00 watermark=-9223372036854775808 value=(Mike,0)
- [<span style="background-color:lightblue">window000</span>, <span style="background-color:lightgreen">accepted</span>] orig   timestamp=2018-11-08T13:00:00.010 watermark=-9223372036854775808 value=(Mike,10)
- [<span style="background-color:lightblue">window000</span>, <span style="background-color:lightgreen">accepted</span>] orig   timestamp=2018-11-08T13:00:00.090 watermark=-9223372036854775808 value=(Mike,9000000000)
- [<span style="background-color:yellow">window100</span>, <span style="background-color:lightgreen">accepted</span>] orig   timestamp=2018-11-08T13:00:00.100 watermark=-9223372036854775808 value=(Mike,10000000000)
- [<span style="background-color:lightblue">window000</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.020 watermark=2018-11-08T13:00:00.099 value=(Mike,200)
- [<span style="background-color:lightblue">window000</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.030 watermark=2018-11-08T13:00:00.099 value=(Mike,3000)
- [<span style="background-color:lightblue">window000</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.040 watermark=2018-11-08T13:00:00.108 value=(Mike,40000)
- [<span style="background-color:lightblue">window000</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.050 watermark=2018-11-08T13:00:00.109 value=(Mike,500000)
- [<span style="background-color:lightblue">window000</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.060 watermark=2018-11-08T13:00:00.109 value=(Mike,6000000)
- [<span style="background-color:lightblue">window000</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.070 watermark=2018-11-08T13:00:00.109 value=(Mike,70000000)
- [<span style="background-color:lightblue">window000</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.080 watermark=2018-11-08T13:00:00.109 value=(Mike,800000000)
- [<span style="background-color:yellow">window100</span>, <span style="background-color:lightgreen">accepted</span>] orig   timestamp=2018-11-08T13:00:00.110 watermark=2018-11-08T13:00:00.109 value=(Mike,10000000010)
- [<span style="background-color:pink">window200</span>, <span style="background-color:lightgreen">accepted</span>] orig   timestamp=2018-11-08T13:00:00.200 watermark=2018-11-08T13:00:00.109 value=(Mike,200000000000)
- [<span style="background-color:yellow">window100</span>, <span style="background-color:lightgreen">accepted</span>] orig   timestamp=2018-11-08T13:00:00.190 watermark=2018-11-08T13:00:00.109 value=(Mike,19000000000)
- [<span style="background-color:yellow">window100</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.120 watermark=2018-11-08T13:00:00.199 value=(Mike,10000000200)
- [<span style="background-color:yellow">window100</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.130 watermark=2018-11-08T13:00:00.199 value=(Mike,10000003000)
- [<span style="background-color:yellow">window100</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.140 watermark=2018-11-08T13:00:00.208 value=(Mike,10000040000)
- [<span style="background-color:yellow">window100</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.150 watermark=2018-11-08T13:00:00.209 value=(Mike,10000500000)
- [<span style="background-color:yellow">window100</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.160 watermark=2018-11-08T13:00:00.209 value=(Mike,10006000000)
- [<span style="background-color:yellow">window100</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.170 watermark=2018-11-08T13:00:00.209 value=(Mike,10070000000)
- [<span style="background-color:yellow">window100</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.180 watermark=2018-11-08T13:00:00.209 value=(Mike,10800000000)

The labeled resulting stream log answers Q1 when lateness=0.

Q1: What does "window ends" mean?

A: A window ends when watermark is equal to or larger than window ending timestamp.

Q2: How does Flink decide whether a window can accept an event?

A: If an event arrives before window ends, it is accepted. Otherwise, it is dropped.

window000 ending timestamp + lateness is 2018-11-08T13:00:00.099. "(Mike,200)" is dropped because it arrives after watermark 2018-11-08T13:00:00.099, which means it arrives after window000 ends.

```text
"WATERMARK.2018-11-08T13:00:00.099",
"2018-11-08T13:00:00.020,Mike,200",
```

For same reason, window100 accepts "(Mike,19000000000)" and drops "(Mike,10000000200)".

```text
"2018-11-08T13:00:00.190,Mike,19000000000",
"WATERMARK.2018-11-08T13:00:00.199",
"2018-11-08T13:00:00.120,Mike,10000000200",
```

### Test customized window lateness (lateness>0)

Use 100ms tumbling window, specify 10ms lateness and apply sum operator to sum up the Long value.

```java
public class WindowAllowedLatenessTest {
    @Test
    public void allowLatenessTest() throws Exception {
        final SingleOutputStreamOperator<Tuple2<String, Long>> stream = orig
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(100L)))
                .allowedLateness(Time.milliseconds(10L))
                .sum(1);

        orig.addSink(new LogSink<>("orig  "));
        stream.addSink(new LogSink<>("window"));

        assertEquals(9, TestUtil.streamToCollection(stream).size());
    }
}
```

The "window" log is the window sum resulting stream.

```text
window timestamp=2018-11-08T13:00:00.099 watermark=-9223372036854775808 value=(Mike,9000000010)
window timestamp=2018-11-08T13:00:00.099 watermark=2018-11-08T13:00:00.099 value=(Mike,9000000210)
window timestamp=2018-11-08T13:00:00.099 watermark=2018-11-08T13:00:00.099 value=(Mike,9000003210)
window timestamp=2018-11-08T13:00:00.099 watermark=2018-11-08T13:00:00.108 value=(Mike,9000043210)
window timestamp=2018-11-08T13:00:00.199 watermark=2018-11-08T13:00:00.109 value=(Mike,39000000010)
window timestamp=2018-11-08T13:00:00.199 watermark=2018-11-08T13:00:00.199 value=(Mike,49000000210)
window timestamp=2018-11-08T13:00:00.199 watermark=2018-11-08T13:00:00.199 value=(Mike,59000003210)
window timestamp=2018-11-08T13:00:00.199 watermark=2018-11-08T13:00:00.208 value=(Mike,69000043210)
window timestamp=2018-11-08T13:00:00.299 watermark=2018-11-08T13:00:00.209 value=(Mike,200000000000)
```

With the Long value, we can label each event with related window accepted/dropped.

- [<span style="background-color:lightblue">window000</span>, <span style="background-color:lightgreen"><span style="background-color:lightgreen">accepted</span></span>] orig   timestamp=2018-11-08T13:00 watermark=-9223372036854775808 value=(Mike,0)
- [<span style="background-color:lightblue">window000</span>, <span style="background-color:lightgreen">accepted</span>] orig   timestamp=2018-11-08T13:00:00.010 watermark=-9223372036854775808 value=(Mike,10)
- [<span style="background-color:lightblue">window000</span>, <span style="background-color:lightgreen">accepted</span>] orig   timestamp=2018-11-08T13:00:00.090 watermark=-9223372036854775808 value=(Mike,9000000000)
- [<span style="background-color:yellow">window100</span>, <span style="background-color:lightgreen">accepted</span>] orig   timestamp=2018-11-08T13:00:00.100 watermark=-9223372036854775808 value=(Mike,10000000000)
- [<span style="background-color:lightblue">window000</span>, <span style="background-color:lightgreen">accepted</span>] orig   timestamp=2018-11-08T13:00:00.020 watermark=2018-11-08T13:00:00.099 value=(Mike,200)
- [<span style="background-color:lightblue">window000</span>, <span style="background-color:lightgreen">accepted</span>] orig   timestamp=2018-11-08T13:00:00.030 watermark=2018-11-08T13:00:00.099 value=(Mike,3000)
- [<span style="background-color:lightblue">window000</span>, <span style="background-color:lightgreen">accepted</span>] orig   timestamp=2018-11-08T13:00:00.040 watermark=2018-11-08T13:00:00.108 value=(Mike,40000)
- [<span style="background-color:lightblue">window000</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.050 watermark=2018-11-08T13:00:00.109 value=(Mike,500000)
- [<span style="background-color:lightblue">window000</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.060 watermark=2018-11-08T13:00:00.109 value=(Mike,6000000)
- [<span style="background-color:lightblue">window000</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.070 watermark=2018-11-08T13:00:00.109 value=(Mike,70000000)
- [<span style="background-color:lightblue">window000</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.080 watermark=2018-11-08T13:00:00.109 value=(Mike,800000000)
- [<span style="background-color:yellow">window100</span>, <span style="background-color:lightgreen">accepted</span>] orig   timestamp=2018-11-08T13:00:00.110 watermark=2018-11-08T13:00:00.109 value=(Mike,10000000010)
- [<span style="background-color:pink">window200</span>, <span style="background-color:lightgreen">accepted</span>] orig   timestamp=2018-11-08T13:00:00.200 watermark=2018-11-08T13:00:00.109 value=(Mike,200000000000)
- [<span style="background-color:yellow">window100</span>, <span style="background-color:lightgreen">accepted</span>] orig   timestamp=2018-11-08T13:00:00.190 watermark=2018-11-08T13:00:00.109 value=(Mike,19000000000)
- [<span style="background-color:yellow">window100</span>, <span style="background-color:lightgreen">accepted</span>] orig   timestamp=2018-11-08T13:00:00.120 watermark=2018-11-08T13:00:00.199 value=(Mike,10000000200)
- [<span style="background-color:yellow">window100</span>, <span style="background-color:lightgreen">accepted</span>] orig   timestamp=2018-11-08T13:00:00.130 watermark=2018-11-08T13:00:00.199 value=(Mike,10000003000)
- [<span style="background-color:yellow">window100</span>, <span style="background-color:lightgreen">accepted</span>] orig   timestamp=2018-11-08T13:00:00.140 watermark=2018-11-08T13:00:00.208 value=(Mike,10000040000)
- [<span style="background-color:yellow">window100</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.150 watermark=2018-11-08T13:00:00.209 value=(Mike,10000500000)
- [<span style="background-color:yellow">window100</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.160 watermark=2018-11-08T13:00:00.209 value=(Mike,10006000000)
- [<span style="background-color:yellow">window100</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.170 watermark=2018-11-08T13:00:00.209 value=(Mike,10070000000)
- [<span style="background-color:yellow">window100</span>, <span style="background-color:grey">dropped </span>] orig   timestamp=2018-11-08T13:00:00.180 watermark=2018-11-08T13:00:00.209 value=(Mike,10800000000)

The labeled resulting stream log answers Q1 when lateness>0.

Q1: What does "window ends" mean?

A: A window ends when watermark is equal to or larger than "window max timestamp + lateness".

Q2: How does Flink decide whether a window can accept an event?

A: If an event arrives before window ends, it is accepted. Otherwise, it is dropped. Window fires a result if window accepts a event between [window max timestamp, window max timestamp + lateness).

window000 max timestamp + lateness is 2018-11-08T13:00:00.099 + 0.010 = 2018-11-08T13:00:00.109. "(Mike,40000)" is accepted because it happens before watermark "WATERMARK.2018-11-08T13:00:00.109". "(Mike,500000)" is dropped because it happens after the watermark.

```text
"2018-11-08T13:00:00.030,Mike,3000",
"WATERMARK.2018-11-08T13:00:00.108",
"2018-11-08T13:00:00.040,Mike,40000",
"WATERMARK.2018-11-08T13:00:00.109",
"2018-11-08T13:00:00.050,Mike,500000",
```

## Lateness Code

Apache Flink code double confirms the above answers.

Here is "lateness" related code in class org.apache.flink.streaming.runtime.operators.windowing.WindowOperator.

- In processElement(), window stops accepting events if isWindowLate is true
- isWindowLate() and cleanupTime() decides a window is late only if
  - window is event time
  - current stream watermark >= window max timestamp

```java
public class WindowOperator<K, IN, ACC, OUT, W extends Window>
    extends AbstractUdfStreamOperator<OUT, InternalWindowFunction<ACC, OUT, K, W>>
	implements OneInputStreamOperator<IN, OUT>, Triggerable<K, W> {

	public void processElement(StreamRecord<IN> element) throws Exception {
        for (W window: elementWindows) {
            // drop if the window is already late
            if (isWindowLate(window)) {
                continue;
            }
            isSkippedElement = false;
        }
    }

    protected boolean isWindowLate(W window) {
        return (windowAssigner.isEventTime() && (cleanupTime(window) <= internalTimerService.currentWatermark()));
    }

    private long cleanupTime(W window) {
        if (windowAssigner.isEventTime()) {
            long cleanupTime = window.maxTimestamp() + allowedLateness;
            return cleanupTime >= window.maxTimestamp() ? cleanupTime : Long.MAX_VALUE;
        } else {
            return window.maxTimestamp();
        }
    }
}
```

## Conclusion

Both test and code helps reach out the conclusion.

In Apache Flink, "lateness" is for window stream using event time. It decides how "late" a window can accept events. A window is "late" when watermark is equal to or larger than "window max timestamp + lateness". For an event whose event timestamp is in [window min timestamp, window max timestamp], if it arrives before window is "late", it is accepted. Otherwise, it is dropped. With positive lateness, same window may fire the result for multiple times. Window fires a result if window accepts a event when watermark is between [window max timestamp, window max timestamp + lateness).

My test code is at [WindowAllowedLatenessTest.java](https://github.com/yuanyangwu/flink-training/blob/master/src/test/java/yuanyangwu/flink/training/streaming/operator/WindowAllowedLatenessTest.java)