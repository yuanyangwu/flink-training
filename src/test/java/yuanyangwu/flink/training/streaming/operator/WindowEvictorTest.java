package yuanyangwu.flink.training.streaming.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.DeltaEvictor;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.Before;
import org.junit.Test;
import yuanyangwu.flink.training.streaming.source.csv.CsvStringTuple2MapFunction;
import yuanyangwu.flink.training.streaming.source.csv.TimestampedCsvSource;
import yuanyangwu.flink.training.util.LogSink;

import java.util.Arrays;

import static yuanyangwu.flink.training.TestUtil.assertStreamEquals;

public class WindowEvictorTest {
    private SingleOutputStreamOperator<Tuple2<String, Integer>> orig;

    @Before
    public void setup() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        orig = TimestampedCsvSource.fromCollection(
                env,
                Arrays.asList(
                        "2018-11-08T13:00:00.000,Mike,0",
                        "2018-11-08T13:00:00.010,Mike,10",
                        "2018-11-08T13:00:00.020,Mike,20",
                        "2018-11-08T13:00:00.030,Mike,30",
                        "2018-11-08T13:00:00.040,Mike,40",
                        "2018-11-08T13:00:00.050,Mike,50",
                        "2018-11-08T13:00:00.060,Mike,60",
                        "2018-11-08T13:00:00.070,Mike,70",
                        "2018-11-08T13:00:00.080,Mike,80",
                        "2018-11-08T13:00:00.090,Mike,90",
                        "2018-11-08T13:00:00.100,Mike,100",
                        "WATERMARK.2018-11-08T13:00:00.101",
                        "2018-11-08T13:00:00.110,Mike,110",
                        "2018-11-08T13:00:00.120,Mike,120",
                        "2018-11-08T13:00:00.130,Mike,130",
                        "2018-11-08T13:00:00.140,Mike,140",
                        "2018-11-08T13:00:00.150,Mike,150",
                        "2018-11-08T13:00:00.160,Mike,160",
                        "2018-11-08T13:00:00.170,Mike,170",
                        "2018-11-08T13:00:00.180,Mike,180",
                        "2018-11-08T13:00:00.190,Mike,190",
                        "2018-11-08T13:00:00.200,Mike,200",
                        "WATERMARK.2018-11-08T13:00:00.201",
                        "2018-11-08T13:00:00.210,Mike,210",
                        "2018-11-08T13:00:00.220,Mike,220",
                        "2018-11-08T13:00:00.230,Mike,230",
                        "2018-11-08T13:00:00.240,Mike,240",
                        "2018-11-08T13:00:00.250,Mike,250",
                        "2018-11-08T13:00:00.260,Mike,260",
                        "2018-11-08T13:00:00.270,Mike,270",
                        "2018-11-08T13:00:00.280,Mike,280",
                        "2018-11-08T13:00:00.290,Mike,290",
                        "2018-11-08T13:00:00.300,Mike,300",
                        "WATERMARK.2018-11-08T13:00:00.301"
                ))
                .map(new CsvStringTuple2MapFunction())
                .map(new MapFunction<Tuple2<String, String>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, String> value) throws Exception {
                        return Tuple2.of(value.f0, Integer.valueOf(value.f1));
                    }
                });
    }

    //    orig   timestamp=2018-11-08T13:00 watermark=-9223372036854775808 value=(Mike,0)
    //    orig   timestamp=2018-11-08T13:00:00.010 watermark=-9223372036854775808 value=(Mike,10)
    //    orig   timestamp=2018-11-08T13:00:00.020 watermark=-9223372036854775808 value=(Mike,20)
    //    orig   timestamp=2018-11-08T13:00:00.030 watermark=-9223372036854775808 value=(Mike,30)
    //    orig   timestamp=2018-11-08T13:00:00.040 watermark=-9223372036854775808 value=(Mike,40)
    //    orig   timestamp=2018-11-08T13:00:00.050 watermark=-9223372036854775808 value=(Mike,50)
    //    orig   timestamp=2018-11-08T13:00:00.060 watermark=-9223372036854775808 value=(Mike,60)
    //    orig   timestamp=2018-11-08T13:00:00.070 watermark=-9223372036854775808 value=(Mike,70)
    //    orig   timestamp=2018-11-08T13:00:00.080 watermark=-9223372036854775808 value=(Mike,80)
    //    orig   timestamp=2018-11-08T13:00:00.090 watermark=-9223372036854775808 value=(Mike,90)
    //
    //    orig   timestamp=2018-11-08T13:00:00.100 watermark=-9223372036854775808 value=(Mike,100)
    //    orig   timestamp=2018-11-08T13:00:00.110 watermark=2018-11-08T13:00:00.101 value=(Mike,110)
    //    orig   timestamp=2018-11-08T13:00:00.120 watermark=2018-11-08T13:00:00.101 value=(Mike,120)
    //    orig   timestamp=2018-11-08T13:00:00.130 watermark=2018-11-08T13:00:00.101 value=(Mike,130)
    //    orig   timestamp=2018-11-08T13:00:00.140 watermark=2018-11-08T13:00:00.101 value=(Mike,140)
    //    orig   timestamp=2018-11-08T13:00:00.150 watermark=2018-11-08T13:00:00.101 value=(Mike,150)
    //    orig   timestamp=2018-11-08T13:00:00.160 watermark=2018-11-08T13:00:00.101 value=(Mike,160)
    //    orig   timestamp=2018-11-08T13:00:00.170 watermark=2018-11-08T13:00:00.101 value=(Mike,170)
    //    orig   timestamp=2018-11-08T13:00:00.180 watermark=2018-11-08T13:00:00.101 value=(Mike,180)
    //    orig   timestamp=2018-11-08T13:00:00.190 watermark=2018-11-08T13:00:00.101 value=(Mike,190)
    //
    //    orig   timestamp=2018-11-08T13:00:00.200 watermark=2018-11-08T13:00:00.101 value=(Mike,200)
    //    orig   timestamp=2018-11-08T13:00:00.210 watermark=2018-11-08T13:00:00.201 value=(Mike,210)
    //    orig   timestamp=2018-11-08T13:00:00.220 watermark=2018-11-08T13:00:00.201 value=(Mike,220)
    //    orig   timestamp=2018-11-08T13:00:00.230 watermark=2018-11-08T13:00:00.201 value=(Mike,230)
    //    orig   timestamp=2018-11-08T13:00:00.240 watermark=2018-11-08T13:00:00.201 value=(Mike,240)
    //    orig   timestamp=2018-11-08T13:00:00.250 watermark=2018-11-08T13:00:00.201 value=(Mike,250)
    //    orig   timestamp=2018-11-08T13:00:00.260 watermark=2018-11-08T13:00:00.201 value=(Mike,260)
    //    orig   timestamp=2018-11-08T13:00:00.270 watermark=2018-11-08T13:00:00.201 value=(Mike,270)
    //    orig   timestamp=2018-11-08T13:00:00.280 watermark=2018-11-08T13:00:00.201 value=(Mike,280)
    //    orig   timestamp=2018-11-08T13:00:00.290 watermark=2018-11-08T13:00:00.201 value=(Mike,290)
    //
    //    orig   timestamp=2018-11-08T13:00:00.300 watermark=2018-11-08T13:00:00.201 value=(Mike,300)
    //
    //    window timestamp=2018-11-08T13:00:00.099 watermark=-9223372036854775808 value=(Mike,240)
    //    window timestamp=2018-11-08T13:00:00.199 watermark=2018-11-08T13:00:00.101 value=(Mike,540)
    //    window timestamp=2018-11-08T13:00:00.299 watermark=2018-11-08T13:00:00.201 value=(Mike,840)
    //    window timestamp=2018-11-08T13:00:00.399 watermark=2018-11-08T13:00:00.301 value=(Mike,300)
    @Test
    public void countEvictorTest() throws Exception {
        final SingleOutputStreamOperator<Tuple2<String, Integer>> stream = orig
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(100L)))
                .evictor(CountEvictor.of(3L))
                .sum(1);

        orig.addSink(new LogSink<>("orig  "));
        stream.addSink(new LogSink<>("window"));

        assertStreamEquals(Arrays.asList(
                new Tuple2<>("Mike", 240),
                new Tuple2<>("Mike", 540),
                new Tuple2<>("Mike", 840),
                new Tuple2<>("Mike", 300)),
                stream);
    }

    //    window timestamp=2018-11-08T13:00:00.099 watermark=-9223372036854775808 value=(Mike,240)
    //    window timestamp=2018-11-08T13:00:00.199 watermark=2018-11-08T13:00:00.101 value=(Mike,540)
    //    window timestamp=2018-11-08T13:00:00.299 watermark=2018-11-08T13:00:00.201 value=(Mike,840)
    //    window timestamp=2018-11-08T13:00:00.399 watermark=2018-11-08T13:00:00.301 value=(Mike,300)
    @Test
    public void timeEvictorTest() throws Exception {
        final SingleOutputStreamOperator<Tuple2<String, Integer>> stream = orig
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(100L)))
                .evictor(TimeEvictor.of(Time.milliseconds(30L)))
                .sum(1);

        orig.addSink(new LogSink<>("orig  "));
        stream.addSink(new LogSink<>("window"));

        assertStreamEquals(Arrays.asList(
                new Tuple2<>("Mike", 240),
                new Tuple2<>("Mike", 540),
                new Tuple2<>("Mike", 840),
                new Tuple2<>("Mike", 300)),
                stream);
    }
}
