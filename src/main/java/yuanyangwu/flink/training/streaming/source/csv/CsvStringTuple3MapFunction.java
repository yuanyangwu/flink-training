package yuanyangwu.flink.training.streaming.source.csv;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class CsvStringTuple3MapFunction implements MapFunction<String, Tuple3<String, String, String>> {
    @Override
    public Tuple3<String, String, String> map(String value) throws Exception {
        String [] fields = value.split(",", 3);
        return new Tuple3<>(fields[0], fields[1], fields[2]);
    }
}
