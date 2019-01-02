package yuanyangwu.flink.training.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yuanyangwu.flink.training.element.PersonIncoming;
import yuanyangwu.flink.training.streaming.source.PojoSourceApp;
import yuanyangwu.flink.training.util.LogSink;

public class PojoTableSourceApp {
    private static Logger LOG = LoggerFactory.getLogger(PojoTableSourceApp.class);

    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
            DataStreamSource<PersonIncoming> source = env.addSource(new PojoSourceApp.PersonSource());
            source.addSink(new LogSink<>("source"));

            final Table table = tEnv.fromDataStream(source);
            table.printSchema();

            final DataStream<PersonIncoming> output = tEnv.toAppendStream(table, PersonIncoming.class);//TypeInformation.of(new TypeHint<PersonIncoming>(){}));
            output.addSink(new LogSink<>("table"));

            env.execute("PojoTableSourceApp");
        } catch (Exception e) {
            LOG.error("main", e);
        }
    }
}
