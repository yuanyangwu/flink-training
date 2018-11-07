package yuanyangwu.flink.training.util;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogSink<IN> implements SinkFunction<IN> {
    private static Logger LOG = LoggerFactory.getLogger(LogSink.class);

    private static final long serialVersionUID = 1L;
    private String prefix;

    public LogSink(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        if (context.timestamp() == null) {
            LOG.info("{} timestamp=null watermark=null value={}",
                    prefix,
                    value);
        } else if (context.currentWatermark() < 0) {
            LOG.info("{} timestamp={} watermark={} value={}",
                    prefix,
                    FlinkTimestamp.toLocalDateTime(context.timestamp()),
                    context.currentWatermark(),
                    value);
        } else {
            LOG.info("{} timestamp={} watermark={} value={}",
                    prefix,
                    FlinkTimestamp.toLocalDateTime(context.timestamp()),
                    FlinkTimestamp.toLocalDateTime(context.currentWatermark()),
                    value);
        }
    }
}
