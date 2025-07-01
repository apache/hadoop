package org.apache.hadoop.fs.s3a.impl.streams;

import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import software.amazon.s3.analyticsaccelerator.util.RequestCallback;

/**
 * Implementation of AAL's RequestCallback interface that tracks analytics operations.
 */
public class AnalyticsRequestCallback implements RequestCallback {
    private final S3AInputStreamStatistics statistics;

    /**
     * Create a new callback instance.
     * @param statistics the statistics to update
     */
    public AnalyticsRequestCallback(S3AInputStreamStatistics statistics) {
        this.statistics = statistics;
    }

    @Override
    public void onGetRequest() {
        statistics.incrementAnalyticsGetRequests();
    }

    @Override
    public void onHeadRequest() {
        statistics.incrementAnalyticsHeadRequests();
    }
}

