package yuanyangwu.flink.training.streaming.operator;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static yuanyangwu.flink.training.TestUtil.assertStreamEquals;

public class AsyncTest {
    private static Logger LOG = LoggerFactory.getLogger(AsyncTest.class);

    @Test
    public void asyncTest() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final SingleOutputStreamOperator<Integer> stream = env
                .setParallelism(1) // set parallelism = 1 to avoid orderless by multi-thread
                .fromElements(1, 2, 3);

        final SingleOutputStreamOperator<Integer> result = AsyncDataStream.orderedWait(stream, new AsyncFunction<Integer, Integer>() {
            @Override
            public void asyncInvoke(Integer input, ResultFuture<Integer> resultFuture) throws Exception {
                LOG.info("async start {}", input);
                CompletableFuture.supplyAsync(new Supplier<Integer>() {

                    @Override
                    public Integer get() {
                        try {
                            Thread.sleep((long)(100.0 * Math.random()));
                            return input;
                        } catch (InterruptedException e) {
                            // Normally handled explicitly.
                            return input;
                        }
                    }
                }).thenAccept( (Integer result) -> {
                    resultFuture.complete(Collections.singleton(result));
                    LOG.info("async end {}", input);
                });
            }
        }, 1000, TimeUnit.MILLISECONDS);

        assertStreamEquals(Arrays.asList(1, 2, 3), result);
    }
}
