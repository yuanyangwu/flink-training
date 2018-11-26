package yuanyangwu.flink.training.streaming.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yuanyangwu.flink.training.streaming.source.csv.CsvStringTuple2MapFunction;
import yuanyangwu.flink.training.streaming.source.csv.TimestampedCsvSource;
import yuanyangwu.flink.training.util.FlinkTimestamp;
import yuanyangwu.flink.training.util.LogSink;

import java.time.LocalDateTime;
import java.util.Arrays;

import static yuanyangwu.flink.training.TestUtil.assertStreamEquals;

public class ProcessWindowFunctionTest {
    private static Logger LOG = LoggerFactory.getLogger(ProcessWindowFunctionTest.class);
    private SingleOutputStreamOperator<Tuple2<String, Integer>> orig;

    @Before
    public void setup() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        orig = TimestampedCsvSource.fromCollection(
                env,
                Arrays.asList(
                        "2018-11-08T13:00:00.000,Mike,10",
                        "2018-11-08T13:00:00.100,John,20",
                        "WATERMARK.2018-11-08T13:00:00.120",
                        "2018-11-08T13:00:00.200,Mike,30",
                        "2018-11-08T13:00:00.300,Mike,25",
                        "WATERMARK.2018-11-08T13:00:00.300",
                        "2018-11-08T13:00:00.400,John,12",
                        "2018-11-08T13:00:00.500,John,50",
                        "WATERMARK.2018-11-08T13:00:00.500"
                ))
                .map(new CsvStringTuple2MapFunction())
                .map(new MapFunction<Tuple2<String, String>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, String> value) throws Exception {
                        return Tuple2.of(value.f0, Integer.valueOf(value.f1));
                    }
                });
    }

    //    orig   timestamp=2018-11-08T13:00 watermark=-9223372036854775808 value=(Mike,10)
    //    orig   timestamp=2018-11-08T13:00:00.100 watermark=-9223372036854775808 value=(John,20)
    //    orig   timestamp=2018-11-08T13:00:00.200 watermark=2018-11-08T13:00:00.120 value=(Mike,30)
    //    orig   timestamp=2018-11-08T13:00:00.300 watermark=2018-11-08T13:00:00.120 value=(Mike,25)
    //    orig   timestamp=2018-11-08T13:00:00.400 watermark=2018-11-08T13:00:00.300 value=(John,12)
    //    orig   timestamp=2018-11-08T13:00:00.500 watermark=2018-11-08T13:00:00.300 value=(John,50)
    //    window timestamp=2018-11-08T13:00:00.299 watermark=2018-11-08T13:00:00.120 value=(Mike,30)
    //    window timestamp=2018-11-08T13:00:00.299 watermark=2018-11-08T13:00:00.120 value=(John,20)
    //    window timestamp=2018-11-08T13:00:00.599 watermark=2018-11-08T13:00:00.500 value=(Mike,25)
    //    window timestamp=2018-11-08T13:00:00.599 watermark=2018-11-08T13:00:00.500 value=(John,50)
    @Test
    public void processTest() throws Exception {
        final SingleOutputStreamOperator<Tuple2<String, Integer>> stream = orig
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(300L)))
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String person = "";
                        int incoming = Integer.MIN_VALUE;
                        for (Tuple2<String, Integer> element : elements) {
                            person = element.f0;
                            incoming = Math.max(incoming, element.f1);
                        }
                        if (!person.isEmpty()) {
                            out.collect(new Tuple2<>(person, incoming));
                        }
                    }
                });

        orig.addSink(new LogSink<>("orig  "));
        stream.addSink(new LogSink<>("window"));

        assertStreamEquals(Arrays.asList(
                new Tuple2<>("Mike", 30),
                new Tuple2<>("John", 20),
                new Tuple2<>("Mike", 25),
                new Tuple2<>("John", 50)),
                stream);
    }

    //    orig   timestamp=2018-11-08T13:00 watermark=-9223372036854775808 value=(Mike,10)
    //    orig   timestamp=2018-11-08T13:00:00.100 watermark=-9223372036854775808 value=(John,20)
    //    orig   timestamp=2018-11-08T13:00:00.200 watermark=2018-11-08T13:00:00.120 value=(Mike,30)
    //    orig   timestamp=2018-11-08T13:00:00.300 watermark=2018-11-08T13:00:00.120 value=(Mike,25)
    //    orig   timestamp=2018-11-08T13:00:00.400 watermark=2018-11-08T13:00:00.300 value=(John,12)
    //    orig   timestamp=2018-11-08T13:00:00.500 watermark=2018-11-08T13:00:00.300 value=(John,50)
    //
    //    start=2018-11-08T13:00 max=2018-11-08T13:00:00.299 (Mike,30)
    //    window timestamp=2018-11-08T13:00:00.299 watermark=2018-11-08T13:00:00.120 value=(1541682000000,Mike,30)
    //    start=2018-11-08T13:00 max=2018-11-08T13:00:00.299 (John,20)
    //    window timestamp=2018-11-08T13:00:00.299 watermark=2018-11-08T13:00:00.120 value=(1541682000000,John,20)
    //    start=2018-11-08T13:00:00.300 max=2018-11-08T13:00:00.599 (Mike,25)
    //    window timestamp=2018-11-08T13:00:00.599 watermark=2018-11-08T13:00:00.500 value=(1541682000300,Mike,25)
    //    start=2018-11-08T13:00:00.300 max=2018-11-08T13:00:00.599 (John,50)
    //    window timestamp=2018-11-08T13:00:00.599 watermark=2018-11-08T13:00:00.500 value=(1541682000300,John,50)
    @Test
    public void reduceProcessTest() throws Exception {
        final SingleOutputStreamOperator<Tuple3<Long, String, Integer>> stream = orig
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(300L)))
                .reduce(
                        new ReduceFunction<Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                                return new Tuple2<>(value1.f0, Math.max(value1.f1, value2.f1));
                            }
                        },
                        new ProcessWindowFunction<Tuple2<String, Integer>, Tuple3<Long, String, Integer>, Tuple, TimeWindow>() {
                            @Override
                            public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple3<Long, String, Integer>> out) throws Exception {
                                Tuple2<String, Integer> element = elements.iterator().next();

                                LOG.info("start={} max={} {}",
                                        FlinkTimestamp.toLocalDateTime(context.window().getStart()),
                                        FlinkTimestamp.toLocalDateTime(context.window().maxTimestamp()),
                                        element);

                                out.collect(new Tuple3<>(context.window().getStart(), element.f0, element.f1));
                            }
                        });

        orig.addSink(new LogSink<>("orig  "));
        stream.addSink(new LogSink<>("window"));

        assertStreamEquals(Arrays.asList(
                new Tuple3<>(FlinkTimestamp.fromLocalDateTime(LocalDateTime.parse("2018-11-08T13:00:00.000")), "Mike", 30),
                new Tuple3<>(FlinkTimestamp.fromLocalDateTime(LocalDateTime.parse("2018-11-08T13:00:00.000")), "John", 20),
                new Tuple3<>(FlinkTimestamp.fromLocalDateTime(LocalDateTime.parse("2018-11-08T13:00:00.300")), "Mike", 25),
                new Tuple3<>(FlinkTimestamp.fromLocalDateTime(LocalDateTime.parse("2018-11-08T13:00:00.300")), "John", 50)),
                stream);
    }
}
