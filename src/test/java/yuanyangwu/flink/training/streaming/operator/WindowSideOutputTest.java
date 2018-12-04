package yuanyangwu.flink.training.streaming.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.junit.Before;
import org.junit.Test;
import yuanyangwu.flink.training.TestUtil;
import yuanyangwu.flink.training.streaming.source.csv.CsvStringTuple2MapFunction;
import yuanyangwu.flink.training.streaming.source.csv.TimestampedCsvSource;
import yuanyangwu.flink.training.util.LogSink;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class WindowSideOutputTest {
    private SingleOutputStreamOperator<Tuple2<String, Long>> orig;

    @Before
    public void setup() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        orig = TimestampedCsvSource.fromCollection(
                env,
                Arrays.asList(
                        "2018-11-08T13:00:00.000,Mike,0",
                        "2018-11-08T13:00:00.010,Mike,10",
                        "WATERMARK.2018-11-08T13:00:00.049",
                        "2018-11-08T13:00:00.020,Mike,200",
                        "WATERMARK.2018-11-08T13:00:00.050",
                        "2018-11-08T13:00:00.090,Mike,9000000000",
                        "2018-11-08T13:00:00.100,Mike,10000000000",
                        "WATERMARK.2018-11-08T13:00:00.099",
                        "2018-11-08T13:00:00.030,Mike,3000",
                        "WATERMARK.2018-11-08T13:00:00.108",
                        "2018-11-08T13:00:00.040,Mike,40000",
                        "WATERMARK.2018-11-08T13:00:00.109",
                        "2018-11-08T13:00:00.060,Mike,6000000",
                        "WATERMARK.2018-11-08T13:00:00.149",
                        "2018-11-08T13:00:00.070,Mike,70000000",
                        "WATERMARK.2018-11-08T13:00:00.150",
                        "2018-11-08T13:00:00.080,Mike,800000000",
                        "WATERMARK.2018-11-08T13:00:00.158",
                        "2018-11-08T13:00:00.110,Mike,10000000010",
                        "WATERMARK.2018-11-08T13:00:00.159",
                        "2018-11-08T13:00:00.050,Mike,500000",
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

    //    orig   timestamp=2018-11-08T13:00 watermark=-9223372036854775808 value=(Mike,0)
    //    orig   timestamp=2018-11-08T13:00:00.010 watermark=-9223372036854775808 value=(Mike,10)
    //    orig   timestamp=2018-11-08T13:00:00.020 watermark=2018-11-08T13:00:00.049 value=(Mike,200)
    //    orig   timestamp=2018-11-08T13:00:00.090 watermark=2018-11-08T13:00:00.050 value=(Mike,9000000000)
    //    orig   timestamp=2018-11-08T13:00:00.100 watermark=2018-11-08T13:00:00.050 value=(Mike,10000000000)
    //    orig   timestamp=2018-11-08T13:00:00.030 watermark=2018-11-08T13:00:00.099 value=(Mike,3000)
    //    orig   timestamp=2018-11-08T13:00:00.040 watermark=2018-11-08T13:00:00.108 value=(Mike,40000)
    //    orig   timestamp=2018-11-08T13:00:00.060 watermark=2018-11-08T13:00:00.109 value=(Mike,6000000)
    //    orig   timestamp=2018-11-08T13:00:00.070 watermark=2018-11-08T13:00:00.149 value=(Mike,70000000)
    //    orig   timestamp=2018-11-08T13:00:00.080 watermark=2018-11-08T13:00:00.150 value=(Mike,800000000)
    //    orig   timestamp=2018-11-08T13:00:00.110 watermark=2018-11-08T13:00:00.158 value=(Mike,10000000010)
    //    orig   timestamp=2018-11-08T13:00:00.050 watermark=2018-11-08T13:00:00.159 value=(Mike,500000)
    //    orig   timestamp=2018-11-08T13:00:00.200 watermark=2018-11-08T13:00:00.159 value=(Mike,200000000000)
    //    orig   timestamp=2018-11-08T13:00:00.190 watermark=2018-11-08T13:00:00.159 value=(Mike,19000000000)
    //    orig   timestamp=2018-11-08T13:00:00.120 watermark=2018-11-08T13:00:00.199 value=(Mike,10000000200)
    //    orig   timestamp=2018-11-08T13:00:00.130 watermark=2018-11-08T13:00:00.199 value=(Mike,10000003000)
    //    orig   timestamp=2018-11-08T13:00:00.140 watermark=2018-11-08T13:00:00.208 value=(Mike,10000040000)
    //    orig   timestamp=2018-11-08T13:00:00.150 watermark=2018-11-08T13:00:00.209 value=(Mike,10000500000)
    //    orig   timestamp=2018-11-08T13:00:00.160 watermark=2018-11-08T13:00:00.209 value=(Mike,10006000000)
    //    orig   timestamp=2018-11-08T13:00:00.170 watermark=2018-11-08T13:00:00.209 value=(Mike,10070000000)
    //    orig   timestamp=2018-11-08T13:00:00.180 watermark=2018-11-08T13:00:00.209 value=(Mike,10800000000)
    //
    //    window timestamp=2018-11-08T13:00:00.099 watermark=2018-11-08T13:00:00.050 value=(Mike,9000000210)
    //    late   timestamp=2018-11-08T13:00:00.030 watermark=2018-11-08T13:00:00.099 value=(Mike,3000)
    //    late   timestamp=2018-11-08T13:00:00.040 watermark=2018-11-08T13:00:00.108 value=(Mike,40000)
    //    late   timestamp=2018-11-08T13:00:00.060 watermark=2018-11-08T13:00:00.109 value=(Mike,6000000)
    //    late   timestamp=2018-11-08T13:00:00.070 watermark=2018-11-08T13:00:00.149 value=(Mike,70000000)
    //    late   timestamp=2018-11-08T13:00:00.080 watermark=2018-11-08T13:00:00.150 value=(Mike,800000000)
    //    late   timestamp=2018-11-08T13:00:00.050 watermark=2018-11-08T13:00:00.159 value=(Mike,500000)
    //    window timestamp=2018-11-08T13:00:00.199 watermark=2018-11-08T13:00:00.159 value=(Mike,39000000010)
    //    late   timestamp=2018-11-08T13:00:00.120 watermark=2018-11-08T13:00:00.199 value=(Mike,10000000200)
    //    late   timestamp=2018-11-08T13:00:00.130 watermark=2018-11-08T13:00:00.199 value=(Mike,10000003000)
    //    late   timestamp=2018-11-08T13:00:00.140 watermark=2018-11-08T13:00:00.208 value=(Mike,10000040000)
    //    late   timestamp=2018-11-08T13:00:00.150 watermark=2018-11-08T13:00:00.209 value=(Mike,10000500000)
    //    late   timestamp=2018-11-08T13:00:00.160 watermark=2018-11-08T13:00:00.209 value=(Mike,10006000000)
    //    late   timestamp=2018-11-08T13:00:00.170 watermark=2018-11-08T13:00:00.209 value=(Mike,10070000000)
    //    late   timestamp=2018-11-08T13:00:00.180 watermark=2018-11-08T13:00:00.209 value=(Mike,10800000000)
    //    window timestamp=2018-11-08T13:00:00.299 watermark=2018-11-08T13:00:00.209 value=(Mike,200000000000)
    @Test
    public void tumblingWindowNoLatenessTest() throws Exception {
        OutputTag<Tuple2<String, Long>> lateOutputTag = new OutputTag<Tuple2<String, Long>>("late-data"){};
        final SingleOutputStreamOperator<Tuple2<String, Long>> stream = orig
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(100L)))
                //.allowedLateness(Time.milliseconds(0L))
                .sideOutputLateData(lateOutputTag)
                .sum(1);

        final DataStream<Tuple2<String, Long>> sideOutput = stream.getSideOutput(lateOutputTag);

        orig.addSink(new LogSink<>("orig  "));
        stream.addSink(new LogSink<>("window"));
        sideOutput.addSink(new LogSink<>("late  "));

        assertEquals(13, TestUtil.streamToCollection(sideOutput).size());
    }

    //    window timestamp=2018-11-08T13:00:00.099 watermark=2018-11-08T13:00:00.050 value=(Mike,9000000210)
    //    window timestamp=2018-11-08T13:00:00.099 watermark=2018-11-08T13:00:00.099 value=(Mike,9000003210)
    //    window timestamp=2018-11-08T13:00:00.099 watermark=2018-11-08T13:00:00.108 value=(Mike,9000043210)
    //    late   timestamp=2018-11-08T13:00:00.060 watermark=2018-11-08T13:00:00.109 value=(Mike,6000000)
    //    late   timestamp=2018-11-08T13:00:00.070 watermark=2018-11-08T13:00:00.149 value=(Mike,70000000)
    //    late   timestamp=2018-11-08T13:00:00.080 watermark=2018-11-08T13:00:00.150 value=(Mike,800000000)
    //    late   timestamp=2018-11-08T13:00:00.050 watermark=2018-11-08T13:00:00.159 value=(Mike,500000)
    //    window timestamp=2018-11-08T13:00:00.199 watermark=2018-11-08T13:00:00.159 value=(Mike,39000000010)
    //    window timestamp=2018-11-08T13:00:00.199 watermark=2018-11-08T13:00:00.199 value=(Mike,49000000210)
    //    window timestamp=2018-11-08T13:00:00.199 watermark=2018-11-08T13:00:00.199 value=(Mike,59000003210)
    //    window timestamp=2018-11-08T13:00:00.199 watermark=2018-11-08T13:00:00.208 value=(Mike,69000043210)
    //    late   timestamp=2018-11-08T13:00:00.150 watermark=2018-11-08T13:00:00.209 value=(Mike,10000500000)
    //    late   timestamp=2018-11-08T13:00:00.160 watermark=2018-11-08T13:00:00.209 value=(Mike,10006000000)
    //    late   timestamp=2018-11-08T13:00:00.170 watermark=2018-11-08T13:00:00.209 value=(Mike,10070000000)
    //    late   timestamp=2018-11-08T13:00:00.180 watermark=2018-11-08T13:00:00.209 value=(Mike,10800000000)
    //    window timestamp=2018-11-08T13:00:00.299 watermark=2018-11-08T13:00:00.209 value=(Mike,200000000000)
    @Test
    public void tumblingWindowAllowLatenessTest() throws Exception {
        OutputTag<Tuple2<String, Long>> lateOutputTag = new OutputTag<Tuple2<String, Long>>("late-data"){};
        final SingleOutputStreamOperator<Tuple2<String, Long>> stream = orig
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(100L)))
                .allowedLateness(Time.milliseconds(10L))
                .sideOutputLateData(lateOutputTag)
                .sum(1);

        final DataStream<Tuple2<String, Long>> sideOutput = stream.getSideOutput(lateOutputTag);

        orig.addSink(new LogSink<>("orig  "));
        stream.addSink(new LogSink<>("window"));
        sideOutput.addSink(new LogSink<>("late  "));

        assertEquals(8, TestUtil.streamToCollection(sideOutput).size());
    }

    //    window timestamp=2018-11-08T13:00:00.049 watermark=-9223372036854775808 value=(Mike,10)
    //    window timestamp=2018-11-08T13:00:00.099 watermark=2018-11-08T13:00:00.050 value=(Mike,9000000210)
    //    late   timestamp=2018-11-08T13:00:00.030 watermark=2018-11-08T13:00:00.099 value=(Mike,3000)
    //    late   timestamp=2018-11-08T13:00:00.040 watermark=2018-11-08T13:00:00.108 value=(Mike,40000)
    //    window timestamp=2018-11-08T13:00:00.149 watermark=2018-11-08T13:00:00.109 value=(Mike,19006000000)
    //    late   timestamp=2018-11-08T13:00:00.070 watermark=2018-11-08T13:00:00.149 value=(Mike,70000000)
    //    late   timestamp=2018-11-08T13:00:00.080 watermark=2018-11-08T13:00:00.150 value=(Mike,800000000)
    //    late   timestamp=2018-11-08T13:00:00.050 watermark=2018-11-08T13:00:00.159 value=(Mike,500000)
    //    window timestamp=2018-11-08T13:00:00.199 watermark=2018-11-08T13:00:00.159 value=(Mike,39000000010)
    //    late   timestamp=2018-11-08T13:00:00.120 watermark=2018-11-08T13:00:00.199 value=(Mike,10000000200)
    //    late   timestamp=2018-11-08T13:00:00.130 watermark=2018-11-08T13:00:00.199 value=(Mike,10000003000)
    //    late   timestamp=2018-11-08T13:00:00.140 watermark=2018-11-08T13:00:00.208 value=(Mike,10000040000)
    //    window timestamp=2018-11-08T13:00:00.249 watermark=2018-11-08T13:00:00.209 value=(Mike,259876500000)
    //    window timestamp=2018-11-08T13:00:00.299 watermark=2018-11-08T13:00:00.209 value=(Mike,200000000000)
    @Test
    public void slidingWindowNoLatenessTest() throws Exception {
        OutputTag<Tuple2<String, Long>> lateOutputTag = new OutputTag<Tuple2<String, Long>>("late-data"){};
        final SingleOutputStreamOperator<Tuple2<String, Long>> stream = orig
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.milliseconds(100L), Time.milliseconds(50L)))
                //.allowedLateness(Time.milliseconds(0L))
                .sideOutputLateData(lateOutputTag)
                .sum(1);

        final DataStream<Tuple2<String, Long>> sideOutput = stream.getSideOutput(lateOutputTag);

        orig.addSink(new LogSink<>("orig  "));
        stream.addSink(new LogSink<>("window"));
        sideOutput.addSink(new LogSink<>("late  "));

        assertEquals(8, TestUtil.streamToCollection(sideOutput).size());
    }

}
