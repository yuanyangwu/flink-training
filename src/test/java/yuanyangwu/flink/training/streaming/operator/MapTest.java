package yuanyangwu.flink.training.streaming.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static yuanyangwu.flink.training.TestUtil.assertStreamEquals;

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

        assertStreamEquals(Arrays.asList("2", "42", "44"), stream);
    }
}
