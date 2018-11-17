package yuanyangwu.flink.training.streaming.operator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FlatMapTest {
    private static class StringStringFlatMapFunction implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            for (String word: value.split(" ")) {
                if (!word.isEmpty()) {
                    out.collect(word);
                }
            }
        }
    }

    @Test
    public void flatMapIntegrationTest() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final SingleOutputStreamOperator<String> stream;
        stream = env
                .setParallelism(1) // set parallelism = 1 to avoid orderless by multi-thread
                .fromElements("Apple is red", " ", "Banana is yellow")
                .flatMap(new StringStringFlatMapFunction());

        // convert stream to list
        final Iterator<String> iterator = DataStreamUtils.collect(stream);
        List<String> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);

        assertEquals(Arrays.asList("Apple", "is", "red", "Banana", "is", "yellow"), result);
    }
}
