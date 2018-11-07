package yuanyangwu.flink.training.streaming.source;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yuanyangwu.flink.training.util.FlinkTimestamp;
import yuanyangwu.flink.training.util.LogSink;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class PojoSourceApp {
    private static Logger LOG = LoggerFactory.getLogger(PojoSourceApp.class);

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

    public static class PersonSource implements SourceFunction<Person> {
        private static final long serialVersionUID = 1L;
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Person> ctx) throws Exception {
            final String [] names = {"Tom", "John", "Alice", "Mary"};
            int counter = 0;
            while (isRunning) {
                LocalDateTime current = LocalDateTime.now(ZoneOffset.UTC);
                long epoch = FlinkTimestamp.fromLocalDateTime(current);
                ctx.collectWithTimestamp(new Person(names[counter % names.length], counter), epoch);
                if (counter % 10 == 0) {
                    ctx.emitWatermark(new Watermark(epoch - 1));
                }
                counter = (counter + 1) % 100;
                Thread.sleep(100L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env
                    .setBufferTimeout(1000)
                    .setParallelism(1)
                    .setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            DataStreamSource<Person> source = env.addSource(new PersonSource());
            source.addSink(new LogSink<>("PersonSink"));

            env.execute("PojoSourceApp");
        } catch (Exception e) {
            LOG.error("main", e);
        }
    }
}
