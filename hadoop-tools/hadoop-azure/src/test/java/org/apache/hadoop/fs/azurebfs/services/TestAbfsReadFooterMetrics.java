package org.apache.hadoop.fs.azurebfs.services;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestAbfsReadFooterMetrics {
    @Test
    public void testReadFooterMetrics() throws Exception {
        AbfsReadFooterMetrics metrics = new AbfsReadFooterMetrics();
        metrics.checkMetricUpdate("Test", 1000, 4000, 20);
        metrics.checkMetricUpdate("Test", 1000, 4000, 20);
        metrics.checkMetricUpdate("Test", 1000, 4000, 20);
        metrics.checkMetricUpdate("Test", 1000, 4000, 20);
        metrics.checkMetricUpdate("Test1", 1000, 1998, 20);
        metrics.checkMetricUpdate("Test1", 988, 1998, 20);
        metrics.checkMetricUpdate("Test1", 988, 1998, 20);
        Assertions.assertThat(metrics.toString())
                .describedAs("Unexpected thread 'abfs-timer-client' found")
                .isEqualTo("$NON_PARQUET:$FR=1000.000_2979.000$SR=994.000_0.000$FL=2999.000$RL=2325.333");
    }
}
