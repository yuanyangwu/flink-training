package yuanyangwu.flink.training.streaming.operator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FilterTest {
    private static class IntegerFilterFunction implements FilterFunction<Integer> {
        @Override
        public boolean filter(Integer value) throws Exception {
            return value % 2 == 0;
        }
    }

    @Test
    public void filterIntegrationTest() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final SingleOutputStreamOperator<Integer> stream;
        stream = env
                .setParallelism(1) // set parallelism = 1 to avoid orderless by multi-thread
                .fromElements(1, 2, 3, 4)
                .filter(new IntegerFilterFunction());

        // convert stream to list
        final Iterator<Integer> iterator = DataStreamUtils.collect(stream);
        List<Integer> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);

        assertEquals(Arrays.asList(2, 4), result);
    }
}
