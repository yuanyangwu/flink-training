package yuanyangwu.flink.training.streaming.operator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;
import yuanyangwu.flink.training.element.PersonIncoming;

import java.util.Arrays;

import static yuanyangwu.flink.training.TestUtil.assertStreamEquals;

public class AggregationTest {
    @Test
    public void maxTupleTest() throws Exception {
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
                .max(1);

        assertStreamEquals(Arrays.asList(
                new Tuple2<>("Tom", 10),
                new Tuple2<>("Mary", 20),
                new Tuple2<>("Tom", 15),
                new Tuple2<>("Mary", 20),
                new Tuple2<>("Mary", 20)),
                stream);
    }

    @Test
    public void maxByTupleTest() throws Exception {
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
                .maxBy(1);

        assertStreamEquals(Arrays.asList(
                new Tuple2<>("Tom", 10),
                new Tuple2<>("Mary", 20),
                new Tuple2<>("Tom", 15),
                new Tuple2<>("Mary", 20),
                new Tuple2<>("Mary", 20)),
                stream);
    }

    @Test
    public void minPojoTest() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final SingleOutputStreamOperator<PersonIncoming> stream;
        stream = env
                .setParallelism(1) // set parallelism = 1 for fromElements
                .fromElements(
                        new PersonIncoming("Tom", 10),
                        new PersonIncoming("Mary", 20),
                        new PersonIncoming("Tom", 15),
                        new PersonIncoming("Mary", 1),
                        new PersonIncoming("Mary", 2))
                .keyBy("person")
                .min("incoming");

        assertStreamEquals(Arrays.asList(
                new PersonIncoming("Tom", 10),
                new PersonIncoming("Mary", 20),
                new PersonIncoming("Tom", 10),
                new PersonIncoming("Mary", 1),
                new PersonIncoming("Mary", 1)),
                stream);
    }
}
