package yuanyangwu.flink.training.util;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public final class FlinkTimestamp {
    public static long fromLocalDateTime(LocalDateTime localDateTime) {
        return localDateTime.toEpochSecond(ZoneOffset.UTC) * 1000
                + localDateTime.getNano() / 1000000;
    }

    public static LocalDateTime toLocalDateTime(long timestampInMillisecond) {
        return LocalDateTime.ofEpochSecond(
                timestampInMillisecond / 1000,
                (int)(timestampInMillisecond % 1000 * 1000000),
                ZoneOffset.UTC);
    }
}
