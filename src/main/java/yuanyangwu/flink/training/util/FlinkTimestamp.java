package yuanyangwu.flink.training.util;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public final class FlinkTimestamp {
    public static long fromLocalDateTime(LocalDateTime localDataTime) {
        return localDataTime.toEpochSecond(ZoneOffset.UTC) * 1000
                + localDataTime.getNano() / 1000000;
    }

    public static LocalDateTime toLocalDateTime(long timestampInMillisecond) {
        return LocalDateTime.ofEpochSecond(
                timestampInMillisecond / 1000,
                (int)(timestampInMillisecond % 1000 * 1000000),
                ZoneOffset.UTC);
    }
}
