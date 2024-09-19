package org.apache.hadoop.fs.azurebfs.constants;

import java.util.Arrays;
import java.util.List;

public final class MetricsConstants {
    public static final String RETRY = "RETRY";
    public static final String BASE = "BASE";
    public static final List<String> RETRY_LIST = Arrays.asList("1", "2", "3", "4", "5_15", "15_25", "25AndAbove");
}
