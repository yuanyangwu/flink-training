package yuanyangwu.flink.training.streaming.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegerSourceApp {
    private static Logger LOG = LoggerFactory.getLogger(IntegerSourceApp.class);

    public static class IntegerSource implements SourceFunction<Integer> {
        private static final long serialVersionUID = 1L;
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            int counter = 0;
            while (isRunning) {
                counter = (counter + 1) % 100;
                ctx.collect(counter);
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
                    .setParallelism(1);

            DataStreamSource<Integer> source = env.addSource(new IntegerSource());
            source.print();

            env.execute("IntegerSourceApp");
        } catch (Exception e) {
            LOG.error("main", e);
        }
    }
}
