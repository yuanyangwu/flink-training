package yuanyangwu.flink.training.streaming.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yuanyangwu.flink.training.element.PersonIncoming;
import yuanyangwu.flink.training.streaming.source.csv.TimestampedCsvSource;
import yuanyangwu.flink.training.util.FlinkTimestamp;
import yuanyangwu.flink.training.util.LogSink;

import javax.annotation.Nullable;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class CsvSourceApp {
    private static Logger LOG = LoggerFactory.getLogger(CsvSourceApp.class);

    public static void main(String[] args) {
        try {
            final String PATH = "file:///C:/dev/work/flink-training/src/main/resources/person_incoming.csv";

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env
                    .setBufferTimeout(1000)
                    .setParallelism(1)
                    .setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            SingleOutputStreamOperator<String> source = TimestampedCsvSource.fromFile(env, PATH);
            source
                    .map(new MapFunction<String, PersonIncoming>() {
                        @Override
                        public PersonIncoming map(String value) throws Exception {
                            String[] fields = value.split(",", 2);
                            return new PersonIncoming(fields[0], Integer.parseInt(fields[1]));
                        }
                    })
                    .addSink(new LogSink<>("csv"));

            env.execute("CsvSourceApp");
        } catch (Exception e) {
            LOG.error("main", e);
        }
    }
}
