package yuanyangwu.flink.training.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yuanyangwu.flink.training.streaming.source.IntegerSourceApp;
import yuanyangwu.flink.training.util.LogSink;

public class IntegerTableSourceApp {
    private static Logger LOG = LoggerFactory.getLogger(IntegerTableSourceApp.class);

    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

            DataStreamSource<Integer> source = env.addSource(new IntegerSourceApp.IntegerSource());
            source.addSink(new LogSink<>("source"));

            final Table table = tEnv.fromDataStream(source, "value");
            table.printSchema();

            final DataStream<Integer> output = tEnv.toAppendStream(table, Integer.class);
            output.addSink(new LogSink<>("table"));

            env.execute("IntegerTableSourceApp");
        } catch (Exception e) {
            LOG.error("main", e);
        }
    }
}
