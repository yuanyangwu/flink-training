package yuanyangwu.flink.training.util;

import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.Assert.*;

public class FlinkTimestampTest {

    @Test
    public void fromLocalDateTime() {
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        long epoch = FlinkTimestamp.fromLocalDateTime(now);
        assertEquals(now, FlinkTimestamp.toLocalDateTime(epoch));
    }
}