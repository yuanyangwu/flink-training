package yuanyangwu.flink.training.streaming.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yuanyangwu.flink.training.util.FlinkTimestamp;
import yuanyangwu.flink.training.util.LogSink;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class TimestampedSourceApp {
    private static Logger LOG = LoggerFactory.getLogger(TimestampedSourceApp.class);

    // LocalDateTime is event timestamp
    // Use LocalDateTime instead of Long because Tuple2.toString() can print LocalDateTime
    // in the human friend format
    public static class TimestampedIntegerSource implements SourceFunction<Tuple2<LocalDateTime, Integer>> {
        private static final long serialVersionUID = 1L;
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple2<LocalDateTime, Integer>> ctx) throws Exception {
            int counter = 0;
            while (isRunning) {
                LocalDateTime current = LocalDateTime.now(ZoneOffset.UTC);
                long epoch = FlinkTimestamp.fromLocalDateTime(current);
                ctx.collectWithTimestamp(Tuple2.of(current, counter), epoch);
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

            DataStreamSource<Tuple2<LocalDateTime, Integer>> source = env.addSource(new TimestampedIntegerSource());
            source.addSink(new LogSink<Tuple2<LocalDateTime, Integer>>("sink1"));
//            source
//                    .process(new ProcessFunction<Tuple2<LocalDateTime, Integer>, Tuple2<LocalDateTime, Integer>>() {
//                        @Override
//                        public void processElement(Tuple2<LocalDateTime, Integer> value, Context ctx, Collector<Tuple2<LocalDateTime, Integer>> out) throws Exception {
//                            LOG.info("timestamp={} value={}", FlinkTimestamp.toLocalDateTime(ctx.timestamp()), value);
//                            out.collect(value);
//                        }
//                    })
//                    .print();

            env.execute("TimestampedSourceApp");
        } catch (Exception e) {
            LOG.error("main", e);
        }
    }
}
