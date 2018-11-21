package yuanyangwu.flink.training.streaming.source.csv;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yuanyangwu.flink.training.util.FlinkTimestamp;
import yuanyangwu.flink.training.util.LogSink;

import javax.annotation.Nullable;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;

/**
 * Convert
 * CSV row format
 *   datetime,field1,field2,...,    a string event whose value is "field1,field2,...," with event timestamp of "datetime"
 *   WATERMARK.datetime,            a watermark with timestamp of "datetime"
 */
public class TimestampedCsvSource {
    private static Logger LOG = LoggerFactory.getLogger(TimestampedCsvSource.class);
    private static final String WATERMARK_PREFIX = "WATERMARK.";

    private static class StringAssignerWithPunctuatedWatermarks implements AssignerWithPunctuatedWatermarks<String> {
        private static final long serialVersionUID = 1L;
        private long timestamp = FlinkTimestamp.fromLocalDateTime(LocalDateTime.now(ZoneOffset.UTC));

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
            try {
                String[] fields = lastElement.split(",", 2);
                if (!fields[0].startsWith(WATERMARK_PREFIX)) {
                    return null;
                }
                String watermark = fields[0].substring(WATERMARK_PREFIX.length());
                LocalDateTime dateTime = LocalDateTime.parse(watermark);
                return new Watermark(FlinkTimestamp.fromLocalDateTime(dateTime));
            } catch (Exception e) {
                LOG.error("Parse watermark failure. row={}", lastElement, e);
            }
            return null;
        }

        @Override
        public long extractTimestamp(String element, long previousElementTimestamp) {
            try {
                String [] fields = element.split(",", 2);
                if (fields[0].startsWith(WATERMARK_PREFIX)) {
                    return timestamp;
                }
                LocalDateTime dateTime = LocalDateTime.parse(fields[0]);
                timestamp = FlinkTimestamp.fromLocalDateTime(dateTime);
            } catch (DateTimeException e) {
                LOG.error("Parse timestamp failure. row={}", element, e);
                // increase by 1 if first field is invalid
                timestamp++;
            }
            return timestamp;
        }
    }

    private static SingleOutputStreamOperator<String> fromStream(SingleOutputStreamOperator<String> stream) {
        return stream
                .assignTimestampsAndWatermarks(new StringAssignerWithPunctuatedWatermarks())
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String [] fields = value.split(",", 2);
                        if (fields.length < 2 || fields[0].startsWith(WATERMARK_PREFIX)) {
                            return;
                        }

                        out.collect(fields[1]);
                    }
                });
    }

    public static SingleOutputStreamOperator<String> fromCollection(StreamExecutionEnvironment env, Collection<String> input) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return fromStream(
                env
                .setParallelism(1)
                .fromCollection(input)
                );
    }

    public static SingleOutputStreamOperator<String> fromFile(StreamExecutionEnvironment env, String path) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return fromStream(env.readTextFile(path));
    }

    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            SingleOutputStreamOperator<String> stream = fromCollection(
                    env,
                    Arrays.asList(
                            "2018-11-08T13:00:00.000,Mike,10",
                            "2018-11-08T13:00:00.100,John,20",
                            "WATERMARK.2018-11-08T13:00:00.120",
                            "2018-11-08T13:00:00.200,Mary,30",
                            "2018-11-08T13:00:00.300,Mike,25",
                            "WATERMARK.2018-11-08T13:00:00.300",
                            "2018-11-08T13:00:00.400,Mary,12",
                            "2018-11-08T13:00:00.500,Jane,50",
                            "WATERMARK.2018-11-08T13:00:00.500"
                    ));

            stream
                    .map(new CsvStringTuple2MapFunction())
                    .addSink(new LogSink<>("test"));
            final JobExecutionResult execute = env.execute("");
        } catch (Exception e) {
            LOG.error("main", e);
        }
    }
}
