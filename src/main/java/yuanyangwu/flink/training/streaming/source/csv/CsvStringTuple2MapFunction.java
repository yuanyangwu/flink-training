package yuanyangwu.flink.training.streaming.source.csv;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class CsvStringTuple2MapFunction implements MapFunction<String, Tuple2<String, String>> {
    @Override
    public Tuple2<String, String> map(String value) throws Exception {
        String [] fields = value.split(",", 2);
        return new Tuple2<>(fields[0], fields[1]);
    }
}
