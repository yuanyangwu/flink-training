package yuanyangwu.flink.training.streaming.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;
import yuanyangwu.flink.training.streaming.source.csv.CsvStringTuple2MapFunction;
import yuanyangwu.flink.training.streaming.source.csv.TimestampedCsvSource;
import yuanyangwu.flink.training.util.LogSink;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static yuanyangwu.flink.training.TestUtil.streamToCollection;

public class UnionTest {
    private StreamExecutionEnvironment env;
    private SingleOutputStreamOperator<Tuple2<String, Integer>> orig1;
    private SingleOutputStreamOperator<Tuple2<String, Integer>> orig2;

    @Before
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        orig1 = TimestampedCsvSource.fromCollection(
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

        orig2 = TimestampedCsvSource.fromCollection(
                env,
                Arrays.asList(
                        "2018-11-08T13:00:00.050,Mike,60",
                        "2018-11-08T13:00:00.150,John,70",
                        "WATERMARK.2018-11-08T13:00:00.170",
                        "2018-11-08T13:00:00.250,Mike,80",
                        "2018-11-08T13:00:00.350,Mike,75",
                        "WATERMARK.2018-11-08T13:00:00.350",
                        "2018-11-08T13:00:00.450,John,62",
                        "2018-11-08T13:00:00.550,John,100",
                        "WATERMARK.2018-11-08T13:00:00.550"
                ))
                .map(new CsvStringTuple2MapFunction())
                .map(new MapFunction<Tuple2<String, String>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, String> value) throws Exception {
                        return Tuple2.of(value.f0, Integer.valueOf(value.f1));
                    }
                });
    }

    //    orig1  timestamp=2018-11-08T13:00 watermark=-9223372036854775808 value=(Mike,10)
    //    orig1  timestamp=2018-11-08T13:00:00.100 watermark=-9223372036854775808 value=(John,20)
    //    orig1  timestamp=2018-11-08T13:00:00.200 watermark=2018-11-08T13:00:00.120 value=(Mike,30)
    //    orig1  timestamp=2018-11-08T13:00:00.300 watermark=2018-11-08T13:00:00.120 value=(Mike,25)
    //    orig1  timestamp=2018-11-08T13:00:00.400 watermark=2018-11-08T13:00:00.300 value=(John,12)
    //    orig1  timestamp=2018-11-08T13:00:00.500 watermark=2018-11-08T13:00:00.300 value=(John,50)
    //
    //    orig2  timestamp=2018-11-08T13:00:00.050 watermark=-9223372036854775808 value=(Mike,60)
    //    orig2  timestamp=2018-11-08T13:00:00.150 watermark=-9223372036854775808 value=(John,70)
    //    orig2  timestamp=2018-11-08T13:00:00.250 watermark=2018-11-08T13:00:00.170 value=(Mike,80)
    //    orig2  timestamp=2018-11-08T13:00:00.350 watermark=2018-11-08T13:00:00.170 value=(Mike,75)
    //    orig2  timestamp=2018-11-08T13:00:00.450 watermark=2018-11-08T13:00:00.350 value=(John,62)
    //    orig2  timestamp=2018-11-08T13:00:00.550 watermark=2018-11-08T13:00:00.350 value=(John,100)
    //
    //    union timestamp=2018-11-08T13:00:00.050 watermark=-9223372036854775808 value=(Mike,60)
    //    union timestamp=2018-11-08T13:00:00.150 watermark=-9223372036854775808 value=(John,70)
    //    union timestamp=2018-11-08T13:00:00.250 watermark=-9223372036854775808 value=(Mike,80)
    //    union timestamp=2018-11-08T13:00:00.350 watermark=-9223372036854775808 value=(Mike,75)
    //    union timestamp=2018-11-08T13:00:00.450 watermark=-9223372036854775808 value=(John,62)
    //    union timestamp=2018-11-08T13:00:00.550 watermark=-9223372036854775808 value=(John,100)
    //    union timestamp=2018-11-08T13:00 watermark=-9223372036854775808 value=(Mike,10)
    //    union timestamp=2018-11-08T13:00:00.100 watermark=-9223372036854775808 value=(John,20)
    //    union timestamp=2018-11-08T13:00:00.200 watermark=2018-11-08T13:00:00.120 value=(Mike,30)
    //    union timestamp=2018-11-08T13:00:00.300 watermark=2018-11-08T13:00:00.120 value=(Mike,25)
    //    union timestamp=2018-11-08T13:00:00.400 watermark=2018-11-08T13:00:00.300 value=(John,12)
    //    union timestamp=2018-11-08T13:00:00.500 watermark=2018-11-08T13:00:00.300 value=(John,50)
    @Test
    public void orig1Union2() throws Exception {
        orig1.addSink(new LogSink<>("orig1 "));
        orig2.addSink(new LogSink<>("orig2 "));

        DataStream<Tuple2<String, Integer>> stream =
                orig1.union(orig2);
        stream.addSink(new LogSink<>("union"));

        assertEquals(12, streamToCollection(stream).size());
    }

    //    union timestamp=2018-11-08T13:00 watermark=-9223372036854775808 value=(Mike,10)
    //    union timestamp=2018-11-08T13:00:00.100 watermark=-9223372036854775808 value=(John,20)
    //    union timestamp=2018-11-08T13:00:00.200 watermark=-9223372036854775808 value=(Mike,30)
    //    union timestamp=2018-11-08T13:00:00.300 watermark=-9223372036854775808 value=(Mike,25)
    //    union timestamp=2018-11-08T13:00:00.400 watermark=-9223372036854775808 value=(John,12)
    //    union timestamp=2018-11-08T13:00:00.500 watermark=-9223372036854775808 value=(John,50)
    //    union timestamp=2018-11-08T13:00:00.050 watermark=-9223372036854775808 value=(Mike,60)
    //    union timestamp=2018-11-08T13:00:00.150 watermark=-9223372036854775808 value=(John,70)
    //    union timestamp=2018-11-08T13:00:00.250 watermark=2018-11-08T13:00:00.170 value=(Mike,80)
    //    union timestamp=2018-11-08T13:00:00.350 watermark=2018-11-08T13:00:00.170 value=(Mike,75)
    //    union timestamp=2018-11-08T13:00:00.450 watermark=2018-11-08T13:00:00.350 value=(John,62)
    //    union timestamp=2018-11-08T13:00:00.550 watermark=2018-11-08T13:00:00.350 value=(John,100)
    @Test
    public void orig2Union1() throws Exception {
        orig1.addSink(new LogSink<>("orig1 "));
        orig2.addSink(new LogSink<>("orig2 "));

        DataStream<Tuple2<String, Integer>> stream =
                orig2.union(orig1);
        stream.addSink(new LogSink<>("union"));

        assertEquals(12, streamToCollection(stream).size());
    }

    //    union timestamp=2018-11-08T13:00 watermark=-9223372036854775808 value=(Mike,10)
    //    union timestamp=2018-11-08T13:00 watermark=-9223372036854775808 value=(Mike,10)
    //    union timestamp=2018-11-08T13:00:00.100 watermark=-9223372036854775808 value=(John,20)
    //    union timestamp=2018-11-08T13:00:00.100 watermark=-9223372036854775808 value=(John,20)
    //    union timestamp=2018-11-08T13:00:00.200 watermark=-9223372036854775808 value=(Mike,30)
    //    union timestamp=2018-11-08T13:00:00.200 watermark=-9223372036854775808 value=(Mike,30)
    //    union timestamp=2018-11-08T13:00:00.300 watermark=-9223372036854775808 value=(Mike,25)
    //    union timestamp=2018-11-08T13:00:00.300 watermark=-9223372036854775808 value=(Mike,25)
    //    union timestamp=2018-11-08T13:00:00.400 watermark=-9223372036854775808 value=(John,12)
    //    union timestamp=2018-11-08T13:00:00.400 watermark=-9223372036854775808 value=(John,12)
    //    union timestamp=2018-11-08T13:00:00.500 watermark=-9223372036854775808 value=(John,50)
    //    union timestamp=2018-11-08T13:00:00.500 watermark=-9223372036854775808 value=(John,50)
    @Test
    public void orig1Union1() throws Exception {
        orig1.addSink(new LogSink<>("orig1 "));

        DataStream<Tuple2<String, Integer>> stream =
                orig1.union(orig1);
        stream.addSink(new LogSink<>("union"));

        assertEquals(12, streamToCollection(stream).size());
    }
}
