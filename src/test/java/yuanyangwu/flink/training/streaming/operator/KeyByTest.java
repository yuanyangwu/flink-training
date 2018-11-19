package yuanyangwu.flink.training.streaming.operator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;
import yuanyangwu.flink.training.util.LogSink;

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

    public static class TupleBasedPersonIncoming extends Tuple2<String, Integer> {
        public TupleBasedPersonIncoming() {
            super();
        }

        public TupleBasedPersonIncoming(String person, Integer incoming) {
            super(person, incoming);
        }

        public void setPerson(String person) {
            this.f0 = person;
        }

        public String getPerson() {
            return this.f0;
        }

        public void setIncoming(Integer incoming) {
            this.f1 = incoming;
        }

        public Integer getIncoming() {
            return this.f1;
        }
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

    public static class PersonIncoming {
        public String person;
        public int incoming;

        public PersonIncoming() {
            person = "";
            incoming = 0;
        }

        public PersonIncoming(String person, int incoming) {
            this.person = person;
            this.incoming = incoming;
        }

        public String getPerson() {
            return person;
        }

        public void setPerson(String person) {
            this.person = person;
        }

        public int getIncoming() {
            return incoming;
        }

        public void setIncoming(int incoming) {
            this.incoming = incoming;
        }

        @Override
        public String toString() {
            return "{person=" + person + ", incoming=" + incoming + "}";
        }

        @Override
        public int hashCode() {
            int result = person != null ? person.hashCode() : 0;
            result = 31 * result + incoming;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof PersonIncoming)) {
                return false;
            }
            @SuppressWarnings("rawtypes")
            PersonIncoming value = (PersonIncoming) obj;
            if (person != null ? !person.equals(value.person) : value.person != null) {
                return false;
            }
            return incoming == value.incoming;
        }
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
