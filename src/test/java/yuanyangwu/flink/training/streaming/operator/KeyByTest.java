package yuanyangwu.flink.training.streaming.operator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;
import yuanyangwu.flink.training.util.LogSink;
import yuanyangwu.flink.training.element.PersonIncoming;
import yuanyangwu.flink.training.element.TupleBasedPersonIncoming;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class KeyByTest {
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
                .sum(1);

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

    @Test
    public void keyByTupleBasedPersonIncomingTest() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final SingleOutputStreamOperator<TupleBasedPersonIncoming> stream;
        stream = env
                .setParallelism(1)  // set parallelism 1 for fromCollection()
                .fromCollection(    // fromElements cannot guess typeInfo properly
                        Arrays.asList(
                            new TupleBasedPersonIncoming("Tom", 10),
                            new TupleBasedPersonIncoming("Mary", 20),
                            new TupleBasedPersonIncoming("Tom", 15),
                            new TupleBasedPersonIncoming("Mary", 1),
                            new TupleBasedPersonIncoming("Mary", 2))
                )
                .keyBy(0)
                .sum(1);

        stream.addSink(new LogSink<>("keyByTupleBasedPersonIncomingTest"));

        // convert stream to list
        final Iterator<TupleBasedPersonIncoming> iterator = DataStreamUtils.collect(stream);
        List<TupleBasedPersonIncoming> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);

        List<TupleBasedPersonIncoming> expected = Arrays.asList(
                new TupleBasedPersonIncoming("Tom", 10),
                new TupleBasedPersonIncoming("Mary", 20),
                new TupleBasedPersonIncoming("Tom", 25),
                new TupleBasedPersonIncoming("Mary", 21),
                new TupleBasedPersonIncoming("Mary", 23));
        assertEquals(expected, result);
    }

    @Test
    public void keyByPojoTest() throws Exception {
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
                .sum("incoming");

        stream.addSink(new LogSink<>("keyByPojoTest"));

        // convert stream to list
        final Iterator<PersonIncoming> iterator = DataStreamUtils.collect(stream);
        List<PersonIncoming> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);

        List<PersonIncoming> expected = Arrays.asList(
                new PersonIncoming("Tom", 10),
                new PersonIncoming("Mary", 20),
                new PersonIncoming("Tom", 25),
                new PersonIncoming("Mary", 21),
                new PersonIncoming("Mary", 23));

        assertEquals(expected, result);
    }
}
