package yuanyangwu.flink.training.streaming.operator;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ReduceTest {
    @Test
    public void keyByTupleTest() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final SingleOutputStreamOperator<Tuple2<String, Integer>> stream;
        stream = env
                .setParallelism(1) // set parallelism = 1 for fromElements
                .fromElements(
                        new Tuple2<>("Tom", 10),
                        new Tuple2<>("Mary", 20),
                        new Tuple2<>("Tom", 15),
                        new Tuple2<>("Mary", 1),
                        new Tuple2<>("Mary", 2))
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                });

        // convert stream to list
        final Iterator<Tuple2<String, Integer>> iterator = DataStreamUtils.collect(stream);
        List<Tuple2<String, Integer>> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);

        assertEquals(Arrays.asList(
                new Tuple2<>("Tom", 10),
                new Tuple2<>("Mary", 20),
                new Tuple2<>("Tom", 25),
                new Tuple2<>("Mary", 21),
                new Tuple2<>("Mary", 23))
                , result);
    }
}
