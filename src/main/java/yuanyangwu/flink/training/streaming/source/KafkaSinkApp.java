package yuanyangwu.flink.training.streaming.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yuanyangwu.flink.training.util.LogSink;

import java.time.LocalDateTime;
import java.util.Properties;

public class KafkaSinkApp {
    private static Logger LOG = LoggerFactory.getLogger(KafkaSinkApp.class);

    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env
                    .setParallelism(1)
                    .setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            FlinkKafkaProducer011<String> producer = new FlinkKafkaProducer011<String>(
                    "192.168.11.60:9092", // Kafka broker
                    "my_topic", // Kafka topic
                    new SimpleStringSchema()); // String-type event

            // When writing events to Kakfa, write each event with its event timestamp.
            // Kafka uses event timestamp for retention
            // Unfortunately, Kafka consumer cannot print Kafka event timestamp.
            producer.setWriteTimestampToKafka(true);

            // Add TimestampedSourceApp.TimestampedIntegerSource as source
            // then convert it to comma-separated string
            SingleOutputStreamOperator<String> source = env
                    .addSource(new TimestampedSourceApp.TimestampedIntegerSource())
                    .map(new MapFunction<Tuple2<LocalDateTime, Integer>, String>() {
                        @Override
                        public String map(Tuple2<LocalDateTime, Integer> value) throws Exception {
                            return value.f0.toString() + "," + value.f1;
                        }
                    });

            // connect same source to both Kafka sink and LogSink sink to monitor what is written to Kafka
            source.addSink(producer);
            source.addSink(new LogSink<String>("KafkaSink"));

            env.execute("KafkaSinkApp");
        } catch (Exception e) {
            LOG.error("main", e);
        }
    }
}
