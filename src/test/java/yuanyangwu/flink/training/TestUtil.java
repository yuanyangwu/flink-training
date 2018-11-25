package yuanyangwu.flink.training;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestUtil {
    public static <T> Collection<T> streamToCollection(DataStream<T> stream) throws IOException {
        final Iterator<T> iterator = DataStreamUtils.collect(stream);
        List<T> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);

        return result;
    }

    public static <T> void assertStreamEquals(Collection<T> expected, DataStream<T> stream) throws IOException {
        Collection<T> actual = streamToCollection(stream);
        assertEquals(expected, actual);
    }
}
