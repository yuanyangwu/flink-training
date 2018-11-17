package yuanyangwu.flink.training.streaming.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import static org.junit.Assert.*;

import java.util.*;

public class MapTest {
    private static class LongStringMapFunction implements MapFunction<Long, String> {
        @Override
        public String map(Long value) throws Exception {
            return Long.toString(value * 2);
        }
    }

    @Test
    public void mapUnitTest() throws Exception {
        assertEquals("24", new LongStringMapFunction().map(12L));
    }

    @Test
    public void mapIntegrationTest() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final SingleOutputStreamOperator<String> stream = env
                .setParallelism(1) // set parallelism = 1 to avoid orderless by multi-thread
                .fromElements(1L, 21L, 22L)
                .map(new LongStringMapFunction());

        // convert stream to list
        final Iterator<String> iterator = DataStreamUtils.collect(stream);
        List<String> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);

        assertEquals(Arrays.asList("2", "42", "44"), result);
    }
}
