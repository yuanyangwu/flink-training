package yuanyangwu.flink.training.streaming.operator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Before;
import org.junit.Test;
import yuanyangwu.flink.training.streaming.source.csv.CsvStringTuple2MapFunction;
import yuanyangwu.flink.training.streaming.source.csv.TimestampedCsvSource;
import yuanyangwu.flink.training.util.LogSink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static yuanyangwu.flink.training.Assert.assertStreamEquals;

public class WindowTest {
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
    //    window timestamp=2018-11-08T13:00:00.299 watermark=2018-11-08T13:00:00.120 value=(Mike,40)
    //    window timestamp=2018-11-08T13:00:00.299 watermark=2018-11-08T13:00:00.120 value=(John,20)
    //    window timestamp=2018-11-08T13:00:00.599 watermark=2018-11-08T13:00:00.500 value=(Mike,25)
    //    window timestamp=2018-11-08T13:00:00.599 watermark=2018-11-08T13:00:00.500 value=(John,62)
    @Test
    public void tumblingWindowTest() throws Exception {
        final SingleOutputStreamOperator<Tuple2<String, Integer>> stream = orig
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(300L)))
                .sum(1);

        orig.addSink(new LogSink<>("orig  "));
        stream.addSink(new LogSink<>("window"));

        assertStreamEquals(Arrays.asList(
                new Tuple2<>("Mike", 40),
                new Tuple2<>("John", 20),
                new Tuple2<>("Mike", 25),
                new Tuple2<>("John", 62)),
                stream);
    }

    //    orig   timestamp=2018-11-08T13:00 watermark=-9223372036854775808 value=(Mike,10)
    //    orig   timestamp=2018-11-08T13:00:00.200 watermark=2018-11-08T13:00:00.120 value=(Mike,30)
    //    orig   timestamp=2018-11-08T13:00:00.300 watermark=2018-11-08T13:00:00.120 value=(Mike,25)
    //    window timestamp=2018-11-08T13:00:00.099 watermark=-9223372036854775808 value=(Mike,10)
    //    window timestamp=2018-11-08T13:00:00.199 watermark=2018-11-08T13:00:00.120 value=(Mike,10)
    //    window timestamp=2018-11-08T13:00:00.299 watermark=2018-11-08T13:00:00.120 value=(Mike,40)
    //    window timestamp=2018-11-08T13:00:00.399 watermark=2018-11-08T13:00:00.300 value=(Mike,55)
    //    window timestamp=2018-11-08T13:00:00.499 watermark=2018-11-08T13:00:00.300 value=(Mike,55)
    //    window timestamp=2018-11-08T13:00:00.599 watermark=2018-11-08T13:00:00.500 value=(Mike,25)
    //
    //    orig   timestamp=2018-11-08T13:00:00.100 watermark=-9223372036854775808 value=(John,20)
    //    orig   timestamp=2018-11-08T13:00:00.400 watermark=2018-11-08T13:00:00.300 value=(John,12)
    //    orig   timestamp=2018-11-08T13:00:00.500 watermark=2018-11-08T13:00:00.300 value=(John,50)
    //    window timestamp=2018-11-08T13:00:00.199 watermark=2018-11-08T13:00:00.120 value=(John,20)
    //    window timestamp=2018-11-08T13:00:00.299 watermark=2018-11-08T13:00:00.120 value=(John,20)
    //    window timestamp=2018-11-08T13:00:00.399 watermark=2018-11-08T13:00:00.300 value=(John,20)
    //    window timestamp=2018-11-08T13:00:00.499 watermark=2018-11-08T13:00:00.300 value=(John,12)
    //    window timestamp=2018-11-08T13:00:00.599 watermark=2018-11-08T13:00:00.500 value=(John,62)
    //    window timestamp=2018-11-08T13:00:00.699 watermark=2018-11-08T13:00:00.500 value=(John,62)
    //    window timestamp=2018-11-08T13:00:00.799 watermark=2018-11-08T13:00:00.500 value=(John,50)
    @Test
    public void slidingWindowTest() throws Exception {
        final SingleOutputStreamOperator<Tuple2<String, Integer>> stream = orig
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.milliseconds(300L), Time.milliseconds(100L)))
                .sum(1);

        orig.addSink(new LogSink<>("orig  "));
        stream.addSink(new LogSink<>("window"));

        assertStreamEquals(Arrays.asList(
                new Tuple2<>("Mike", 10),
                new Tuple2<>("Mike", 10),
                new Tuple2<>("Mike", 40),
                new Tuple2<>("Mike", 55),
                new Tuple2<>("Mike", 55),
                new Tuple2<>("Mike", 25)),
                stream.filter(new FilterFunction<Tuple2<String, Integer>>() {
                    @Override
                    public boolean filter(Tuple2<String, Integer> value) throws Exception {
                        return value.f0.equals("Mike");
                    }
                })
        );
    }
}
