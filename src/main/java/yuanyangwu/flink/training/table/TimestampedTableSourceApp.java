package yuanyangwu.flink.training.table;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yuanyangwu.flink.training.streaming.source.TimestampedSourceApp;
import yuanyangwu.flink.training.util.LogSink;

import java.time.LocalDateTime;

public class TimestampedTableSourceApp {
    private static Logger LOG = LoggerFactory.getLogger(IntegerTableSourceApp.class);

    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
            DataStreamSource<Tuple2<LocalDateTime, Integer>> source = env.addSource(new TimestampedSourceApp.TimestampedIntegerSource());
            source.addSink(new LogSink<>("source"));

            final Table table = tEnv.fromDataStream(source, "timestamp, value");
            table.printSchema();

            final DataStream<Tuple2<LocalDateTime, Integer>> output = tEnv.toAppendStream(table, TypeInformation.of(new TypeHint<Tuple2<LocalDateTime, Integer>>(){}));
            output.addSink(new LogSink<>("table"));

            env.execute("TimestampedTableSourceApp");
        } catch (Exception e) {
            LOG.error("main", e);
        }
    }
}
