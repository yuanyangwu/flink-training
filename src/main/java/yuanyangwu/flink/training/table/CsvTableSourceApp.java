package yuanyangwu.flink.training.table;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yuanyangwu.flink.training.util.LogSink;

public class CsvTableSourceApp {
    private static Logger LOG = LoggerFactory.getLogger(CsvTableSourceApp.class);

    public static void main(String[] args) {
        try {
            final String PATH = "file:///C:/dev/work/flink-training/src/main/resources/person_incoming.csv";

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

            final CsvTableSource csvTableSource = CsvTableSource.builder()
                    .path(PATH)
                    .commentPrefix("WATERMARK") // ignore line starting with "WATERMARK" as comment
                    .fieldDelimiter(",")
                    .field("Timestamp", Types.STRING)
                    .field("Person", Types.STRING)
                    .field("Incoming", Types.INT)
                    .build();
            tEnv.registerTableSource("PersonIncoming", csvTableSource);

            final Table table = tEnv
                    .scan("PersonIncoming")
                    .filter("Person <> 'Mike'");
            table.printSchema();

            final DataStream<Row> output = tEnv.toAppendStream(table, Row.class);

            output.addSink(new LogSink<>("table"));

            env.execute("CsvTableSourceApp");
        } catch (Exception e) {
            LOG.error("main", e);
        }
    }
}
