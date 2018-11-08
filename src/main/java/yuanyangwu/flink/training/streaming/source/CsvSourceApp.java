package yuanyangwu.flink.training.streaming.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yuanyangwu.flink.training.util.FlinkTimestamp;
import yuanyangwu.flink.training.util.LogSink;

import javax.annotation.Nullable;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class CsvSourceApp {
    private static Logger LOG = LoggerFactory.getLogger(CsvSourceApp.class);

    public static class Person  {
        public String name;
        public Integer incoming;

        public Person(String name, Integer incoming) {
            this.name = name;
            this.incoming = incoming;
        }

        @Override
        public String toString() {
            return "Person{name=" + name + ", incoming=" + incoming + "}";
        }
    }

    private static class StringAssignerWithPunctuatedWatermarks implements AssignerWithPunctuatedWatermarks<String> {
        private static final long serialVersionUID = 1L;
        private long timestamp = FlinkTimestamp.fromLocalDateTime(LocalDateTime.now(ZoneOffset.UTC));

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
            return new Watermark(extractedTimestamp - 1);
        }

        @Override
        public long extractTimestamp(String element, long previousElementTimestamp) {
            try {
                String [] fields = element.split(",", 2);
                LocalDateTime dateTime = LocalDateTime.parse(fields[0]);
                timestamp = FlinkTimestamp.fromLocalDateTime(dateTime);
            } catch (DateTimeException e) {
                // increase by 1 if first field is invalid
                timestamp++;
            }
            return timestamp;
        }
    }

    public static void main(String[] args) {
        try {
            final String PATH = "file:///C:/dev/work/flink-training/src/main/resources/person_incoming.csv";

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env
                    .setBufferTimeout(1000)
                    .setParallelism(1)
                    .setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            DataStreamSource<String> source = env.readTextFile(PATH);
            source
                    .assignTimestampsAndWatermarks(new StringAssignerWithPunctuatedWatermarks())
                    .map(new MapFunction<String, Person>() {
                        @Override
                        public Person map(String value) throws Exception {
                            String[] fields = value.split(",", 4);
                            return new Person(fields[1], Integer.parseInt(fields[2]));
                        }
                    })
                    .addSink(new LogSink<>("csv"));

            env.execute("CsvSourceApp");
        } catch (Exception e) {
            LOG.error("main", e);
        }
    }
}
