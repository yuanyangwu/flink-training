package yuanyangwu.flink.training.streaming.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;
import yuanyangwu.flink.training.streaming.source.csv.CsvStringTuple3MapFunction;
import yuanyangwu.flink.training.streaming.source.csv.TimestampedCsvSource;
import yuanyangwu.flink.training.util.LogSink;

import java.util.Arrays;

import static yuanyangwu.flink.training.TestUtil.assertStreamEquals;

public class ProjectTest {
    private SingleOutputStreamOperator<Tuple3<String, Integer, String>> orig;

    @Before
    public void setup() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        orig = TimestampedCsvSource.fromCollection(
                env,
                Arrays.asList(
                        "2018-11-08T13:00:00.000,Mike,10,C",
                        "2018-11-08T13:00:00.100,John,20,B",
                        "WATERMARK.2018-11-08T13:00:00.120",
                        "2018-11-08T13:00:00.200,Mike,30,A",
                        "2018-11-08T13:00:00.300,Mike,25,B",
                        "WATERMARK.2018-11-08T13:00:00.300",
                        "2018-11-08T13:00:00.400,John,12,C",
                        "2018-11-08T13:00:00.500,John,50,A",
                        "WATERMARK.2018-11-08T13:00:00.500"
                ))
                .map(new CsvStringTuple3MapFunction())
                .map(new MapFunction<Tuple3<String, String, String>, Tuple3<String, Integer, String>>() {
                    @Override
                    public Tuple3<String, Integer, String> map(Tuple3<String, String, String> value) throws Exception {
                        return Tuple3.of(value.f0, Integer.valueOf(value.f1), value.f2);
                    }
                });
    }

    //    orig    timestamp=2018-11-08T13:00 watermark=-9223372036854775808 value=(Mike,10,C)
    //    orig    timestamp=2018-11-08T13:00:00.100 watermark=-9223372036854775808 value=(John,20,B)
    //    orig    timestamp=2018-11-08T13:00:00.200 watermark=2018-11-08T13:00:00.120 value=(Mike,30,A)
    //    orig    timestamp=2018-11-08T13:00:00.300 watermark=2018-11-08T13:00:00.120 value=(Mike,25,B)
    //    orig    timestamp=2018-11-08T13:00:00.400 watermark=2018-11-08T13:00:00.300 value=(John,12,C)
    //    orig    timestamp=2018-11-08T13:00:00.500 watermark=2018-11-08T13:00:00.300 value=(John,50,A)
    //
    //    project timestamp=2018-11-08T13:00 watermark=-9223372036854775808 value=(C,10)
    //    project timestamp=2018-11-08T13:00:00.100 watermark=-9223372036854775808 value=(B,20)
    //    project timestamp=2018-11-08T13:00:00.200 watermark=2018-11-08T13:00:00.120 value=(A,30)
    //    project timestamp=2018-11-08T13:00:00.300 watermark=2018-11-08T13:00:00.120 value=(B,25)
    //    project timestamp=2018-11-08T13:00:00.400 watermark=2018-11-08T13:00:00.300 value=(C,12)
    //    project timestamp=2018-11-08T13:00:00.500 watermark=2018-11-08T13:00:00.300 value=(A,50)
    @Test
    public void projectTest() throws Exception {
        final SingleOutputStreamOperator<Tuple2<String, Integer>> stream = orig
                .project(2, 1);

        orig.addSink(new LogSink<>("orig   "));
        stream.addSink(new LogSink<>("project"));

        assertStreamEquals(Arrays.asList(
                new Tuple2<>("C", 10),
                new Tuple2<>("B", 20),
                new Tuple2<>("A", 30),
                new Tuple2<>("B", 25),
                new Tuple2<>("C", 12),
                new Tuple2<>("A", 50)),
                stream);
    }
}
