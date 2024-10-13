package org.apache.hadoop.fs.azurebfs.utils;

public class TimeUtils {

    private static final int ONE_MILLION = 1000_000;

    /**
     * Returns the elapsed time in milliseconds.
     */
    public static long elapsedTimeMs(final long startTime) {
        return (System.nanoTime() - startTime) / ONE_MILLION;
    }

}
