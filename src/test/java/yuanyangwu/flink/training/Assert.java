package yuanyangwu.flink.training;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class Assert {
    public static <T> void assertStreamEquals(Collection<T> expected, DataStream<T> stream) throws IOException {
        // convert stream to list
        final Iterator<T> iterator = DataStreamUtils.collect(stream);
        List<T> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);

        assertEquals(expected, result);
    }
}
