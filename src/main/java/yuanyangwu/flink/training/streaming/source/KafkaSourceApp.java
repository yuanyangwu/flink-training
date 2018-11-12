package yuanyangwu.flink.training.streaming.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yuanyangwu.flink.training.util.FlinkTimestamp;
import yuanyangwu.flink.training.util.LogSink;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.Properties;

public class KafkaSourceApp {
    private static Logger LOG = LoggerFactory.getLogger(KafkaSourceApp.class);

    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env
                    .setParallelism(1)
                    .setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "192.168.11.60:9092");
            // only required for Kafka 0.8
            properties.setProperty("zookeeper.connect", "192.168.11.60:2181");
            properties.setProperty("group.id", "kafka-source");

            FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(
                    "my_topic", // Kafka topic
                    new SimpleStringSchema(), // String-type event
                    properties); // Kafka properties
            consumer
                    .setStartFromEarliest() // read from beginning
                    .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<String>() { // parse timestamp from event
                        private static final long serialVersionUID = 1L;
                        private long timestamp = FlinkTimestamp.fromLocalDateTime(LocalDateTime.now());

                        @Override
                        public long extractTimestamp(String element, long previousElementTimestamp) {
                            try {
                                // parse date time from first field
                                String [] fields = element.split(",", 2);
                                LocalDateTime dateTime = LocalDateTime.parse(fields[0]);
                                timestamp = FlinkTimestamp.fromLocalDateTime(dateTime);
                            } catch (DateTimeParseException e) {
                                // increase by 1 if first field is invalid
                                timestamp++;
                            }
                            return timestamp;
                        }

                        @Override
                        public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
                            return new Watermark(extractedTimestamp - 1);
                        }

                    });

            DataStreamSource<String> source = env.addSource(consumer);
            source
                    .addSink(new LogSink<String>("KafkaSource"));

            env.execute("KafkaSourceApp");
        } catch (Exception e) {
            LOG.error("main", e);
        }
    }
}
