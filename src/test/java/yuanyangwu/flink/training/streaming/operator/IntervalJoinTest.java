package yuanyangwu.flink.training.streaming.operator;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;
import yuanyangwu.flink.training.streaming.source.csv.CsvStringTuple2MapFunction;
import yuanyangwu.flink.training.streaming.source.csv.TimestampedCsvSource;
import yuanyangwu.flink.training.util.LogSink;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static yuanyangwu.flink.training.TestUtil.streamToCollection;

public class IntervalJoinTest {
    private StreamExecutionEnvironment env;
    private SingleOutputStreamOperator<Tuple2<String, Integer>> nameIncoming;
    private SingleOutputStreamOperator<Tuple2<String, String>> nameGrade;

    @Before
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        nameIncoming = TimestampedCsvSource.fromCollection(
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

        nameGrade = TimestampedCsvSource.fromCollection(
                env,
                Arrays.asList(
                        "2018-11-08T13:00:00.020,Mike,A",
                        "2018-11-08T13:00:00.090,John,B",
                        "WATERMARK.2018-11-08T13:00:00.170",
                        "2018-11-08T13:00:00.110,John,C",
                        "2018-11-08T13:00:00.290,Mike,D",
                        "WATERMARK.2018-11-08T13:00:00.350",
                        "2018-11-08T13:00:00.379,John,E",
                        "2018-11-08T13:00:00.510,John,F",
                        "WATERMARK.2018-11-08T13:00:00.550"
                ))
                .map(new CsvStringTuple2MapFunction());
    }

    //    nameIncoming timestamp=2018-11-08T13:00 watermark=-9223372036854775808 value=(Mike,10)
    //    nameIncoming timestamp=2018-11-08T13:00:00.100 watermark=-9223372036854775808 value=(John,20)
    //    nameIncoming timestamp=2018-11-08T13:00:00.200 watermark=2018-11-08T13:00:00.120 value=(Mike,30)
    //    nameIncoming timestamp=2018-11-08T13:00:00.300 watermark=2018-11-08T13:00:00.120 value=(Mike,25)
    //    nameIncoming timestamp=2018-11-08T13:00:00.400 watermark=2018-11-08T13:00:00.300 value=(John,12)
    //    nameIncoming timestamp=2018-11-08T13:00:00.500 watermark=2018-11-08T13:00:00.300 value=(John,50)
    //
    //    nameGrade    timestamp=2018-11-08T13:00:00.020 watermark=-9223372036854775808 value=(Mike,A)
    //    nameGrade    timestamp=2018-11-08T13:00:00.090 watermark=-9223372036854775808 value=(John,B)
    //    nameGrade    timestamp=2018-11-08T13:00:00.110 watermark=2018-11-08T13:00:00.170 value=(John,C)
    //    nameGrade    timestamp=2018-11-08T13:00:00.290 watermark=2018-11-08T13:00:00.170 value=(Mike,D)
    //    nameGrade    timestamp=2018-11-08T13:00:00.379 watermark=2018-11-08T13:00:00.350 value=(John,E)
    //    nameGrade    timestamp=2018-11-08T13:00:00.510 watermark=2018-11-08T13:00:00.350 value=(John,F)
    //
    //    join timestamp=2018-11-08T13:00:00.020 watermark=-9223372036854775808 value=(Mike,10,A)
    //    join timestamp=2018-11-08T13:00:00.100 watermark=-9223372036854775808 value=(John,20,B)
    //    join timestamp=2018-11-08T13:00:00.300 watermark=2018-11-08T13:00:00.170 value=(Mike,25,D)
    //    join timestamp=2018-11-08T13:00:00.510 watermark=2018-11-08T13:00:00.350 value=(John,50,F)
    @Test
    public void intervalJoinTest() throws Exception {
        nameIncoming.addSink(new LogSink<>("nameIncoming"));
        nameGrade.addSink(new LogSink<>("nameGrade   "));

        DataStream<Tuple3<String, Integer, String>> stream = nameIncoming
                .keyBy(0)
                .intervalJoin(nameGrade.keyBy(0))
                .between(Time.milliseconds(-20L), Time.milliseconds(20L))
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple2<String, String>, Tuple3<String, Integer, String>>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> left, Tuple2<String, String> right, Context ctx, Collector<Tuple3<String, Integer, String>> out) throws Exception {
                        out.collect(new Tuple3<>(left.f0, left.f1, right.f1));
                    }
                });

        stream.addSink(new LogSink<>("join"));

        assertEquals(4, streamToCollection(stream).size());
    }
}
