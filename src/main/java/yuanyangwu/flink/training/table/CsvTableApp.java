package yuanyangwu.flink.training.table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yuanyangwu.flink.training.element.PersonIncoming;
import yuanyangwu.flink.training.streaming.source.csv.TimestampedCsvSource;
import yuanyangwu.flink.training.util.LogSink;

public class CsvTableApp {
    private static Logger LOG = LoggerFactory.getLogger(CsvTableApp.class);

    public static void main(String[] args) {
        try {
            final String PATH = "file:///C:/dev/work/flink-training/src/main/resources/person_incoming.csv";

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

            SingleOutputStreamOperator<String> source = TimestampedCsvSource.fromFile(env, PATH);
            source.addSink(new LogSink<>("csv"));

            final SingleOutputStreamOperator<PersonIncoming> stream = source
                    .map(new MapFunction<String, PersonIncoming>() {
                        @Override
                        public PersonIncoming map(String value) throws Exception {
                            String[] fields = value.split(",", 2);
                            return new PersonIncoming(fields[0], Integer.parseInt(fields[1]));
                        }
                    });

            final Table table = tEnv.fromDataStream(stream);
            table.printSchema();

            final DataStream<PersonIncoming> output = tEnv.toAppendStream(table, PersonIncoming.class);

            output.addSink(new LogSink<>("table"));

            env.execute("CsvTableApp");
        } catch (Exception e) {
            LOG.error("main", e);
        }
    }
}
