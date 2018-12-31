package yuanyangwu.flink.training.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import yuanyangwu.flink.training.util.LogSink;

public class StreamTableApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        final DataStreamSource<Integer> source = env.fromElements(1, 2, 3);
        final Table table = tEnv.fromDataStream(source, "value");

        table.printSchema();

        final DataStream<Integer> output = tEnv.toAppendStream(table, Integer.class);
        output.addSink(new LogSink<>("table"));
        env.execute("StreamTableApp");
    }
}
