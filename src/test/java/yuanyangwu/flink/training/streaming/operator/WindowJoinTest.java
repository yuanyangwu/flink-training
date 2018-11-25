package yuanyangwu.flink.training.streaming.operator;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Before;
import org.junit.Test;
import yuanyangwu.flink.training.streaming.source.csv.CsvStringTuple2MapFunction;
import yuanyangwu.flink.training.streaming.source.csv.TimestampedCsvSource;
import yuanyangwu.flink.training.util.LogSink;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static yuanyangwu.flink.training.TestUtil.streamToCollection;

public class WindowJoinTest {
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
                        "2018-11-08T13:00:00.050,Mike,A",
                        "2018-11-08T13:00:00.150,John,B",
                        "WATERMARK.2018-11-08T13:00:00.170",
                        "2018-11-08T13:00:00.250,Mike,C",
                        "2018-11-08T13:00:00.350,Mike,D",
                        "WATERMARK.2018-11-08T13:00:00.350",
                        "2018-11-08T13:00:00.450,John,E",
                        "2018-11-08T13:00:00.550,John,F",
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
    //    nameGrade    timestamp=2018-11-08T13:00:00.050 watermark=-9223372036854775808 value=(Mike,A)
    //    nameGrade    timestamp=2018-11-08T13:00:00.150 watermark=-9223372036854775808 value=(John,B)
    //    nameGrade    timestamp=2018-11-08T13:00:00.250 watermark=2018-11-08T13:00:00.170 value=(Mike,C)
    //    nameGrade    timestamp=2018-11-08T13:00:00.350 watermark=2018-11-08T13:00:00.170 value=(Mike,D)
    //    nameGrade    timestamp=2018-11-08T13:00:00.450 watermark=2018-11-08T13:00:00.350 value=(John,E)
    //    nameGrade    timestamp=2018-11-08T13:00:00.550 watermark=2018-11-08T13:00:00.350 value=(John,F)
    //
    //    join timestamp=2018-11-08T13:00:00.199 watermark=2018-11-08T13:00:00.120 value=(Mike,10,A)
    //    join timestamp=2018-11-08T13:00:00.199 watermark=2018-11-08T13:00:00.120 value=(John,20,B)
    //
    //    join timestamp=2018-11-08T13:00:00.399 watermark=2018-11-08T13:00:00.300 value=(Mike,30,C)
    //    join timestamp=2018-11-08T13:00:00.399 watermark=2018-11-08T13:00:00.300 value=(Mike,30,D)
    //    join timestamp=2018-11-08T13:00:00.399 watermark=2018-11-08T13:00:00.300 value=(Mike,25,C)
    //    join timestamp=2018-11-08T13:00:00.399 watermark=2018-11-08T13:00:00.300 value=(Mike,25,D)
    //
    //    join timestamp=2018-11-08T13:00:00.599 watermark=2018-11-08T13:00:00.500 value=(John,12,E)
    //    join timestamp=2018-11-08T13:00:00.599 watermark=2018-11-08T13:00:00.500 value=(John,12,F)
    //    join timestamp=2018-11-08T13:00:00.599 watermark=2018-11-08T13:00:00.500 value=(John,50,E)
    //    join timestamp=2018-11-08T13:00:00.599 watermark=2018-11-08T13:00:00.500 value=(John,50,F)
    @Test
    public void windowJoinTest() throws Exception {
        nameIncoming.addSink(new LogSink<>("nameIncoming"));
        nameGrade.addSink(new LogSink<>("nameGrade   "));

        DataStream<Tuple3<String, Integer, String>> stream = nameIncoming
                .join(nameGrade)
                .where(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .equalTo(new KeySelector<Tuple2<String, String>, String>() {
                    @Override
                    public String getKey(Tuple2<String, String> value) throws Exception {
                        return value.f0;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.milliseconds(200L)))
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, String>, Tuple3<String, Integer, String>>() {
                    @Override
                    public Tuple3<String, Integer, String> join(Tuple2<String, Integer> first, Tuple2<String, String> second) throws Exception {
                        return new Tuple3<>(first.f0, first.f1, second.f1);
                    }
                });
                
        stream.addSink(new LogSink<>("join"));

        assertEquals(10, streamToCollection(stream).size());
    }
}
