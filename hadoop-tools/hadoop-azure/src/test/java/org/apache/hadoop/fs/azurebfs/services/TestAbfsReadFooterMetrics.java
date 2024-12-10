/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.Before;

/**
 * Unit test for Abfs read footer metrics
 */
public class TestAbfsReadFooterMetrics {
    private static final long CONTENT_LENGTH = 50000;
    private static final int LENGTH = 10000;
    private static final int NEXT_READ_POS = 30000;
    private static final String TEST_FILE1 = "TestFile";
    private static final String TEST_FILE2 = "TestFile2";
    private AbfsReadFooterMetrics metrics;

    @Before
    public void setUp() {
        metrics = new AbfsReadFooterMetrics();
    }

    /**
     * Tests that metrics are updated correctly for the first read of a file.
     */
    @Test
    public void metricsUpdateForFirstRead() {
        metrics.updateReadMetrics(TEST_FILE1, LENGTH, CONTENT_LENGTH, NEXT_READ_POS);
        Assertions.assertThat(metrics.getTotalFiles())
                .describedAs("Total number of files")
                .isEqualTo(0);
    }

    /**
     * Tests that metrics are updated correctly for the second read of the same file.
     */
    @Test
    public void metricsUpdateForSecondRead() {
        metrics.updateReadMetrics(TEST_FILE1, LENGTH, CONTENT_LENGTH, NEXT_READ_POS);
        metrics.updateReadMetrics(TEST_FILE1, LENGTH, CONTENT_LENGTH, NEXT_READ_POS+LENGTH);
        Assertions.assertThat(metrics.getTotalFiles())
                .describedAs("Total number of files")
                .isEqualTo(1);
    }

    /**
     * Tests that metrics are updated correctly for multiple reads in one files.
     */
    @Test
    public void metricsUpdateForOneFile() {
        metrics.updateReadMetrics(TEST_FILE1, LENGTH, CONTENT_LENGTH, NEXT_READ_POS);
        metrics.updateReadMetrics(TEST_FILE1, LENGTH, CONTENT_LENGTH, NEXT_READ_POS+LENGTH);
        metrics.updateReadMetrics(TEST_FILE1, LENGTH, CONTENT_LENGTH, NEXT_READ_POS+2*LENGTH);
        Assertions.assertThat(metrics.getTotalFiles())
                .describedAs("Total number of files")
                .isEqualTo(1);
        Assertions.assertThat(metrics.toString())
                .describedAs("Metrics after reading 3 reads of the same file")
                .isEqualTo("$NON_PARQUET:$FR=10000.000_20000.000$SR=10000.000_10000.000$FL=50000.000$RL=10000.000");
    }

    /**
     * Tests that the getReadFooterMetrics method returns the correct metrics after multiple reads on different files.
     */
    @Test
    public void metricsUpdateForMultipleFiles() {
        metrics.updateReadMetrics(TEST_FILE1, LENGTH, CONTENT_LENGTH, NEXT_READ_POS);
        metrics.updateReadMetrics(TEST_FILE1, LENGTH, CONTENT_LENGTH, NEXT_READ_POS+LENGTH);
        metrics.updateReadMetrics(TEST_FILE1, LENGTH, CONTENT_LENGTH, NEXT_READ_POS+2*LENGTH);
        metrics.updateReadMetrics(TEST_FILE2, LENGTH, CONTENT_LENGTH/2, NEXT_READ_POS);
        metrics.updateReadMetrics(TEST_FILE2, LENGTH, CONTENT_LENGTH/2, NEXT_READ_POS+LENGTH);
        metrics.updateReadMetrics(TEST_FILE2, LENGTH, CONTENT_LENGTH/2, NEXT_READ_POS+2*LENGTH);
        Assertions.assertThat(metrics.getTotalFiles())
                .describedAs("Total number of files")
                .isEqualTo(2);
        Assertions.assertThat(metrics.toString())
                .describedAs("Metrics after reading 3 reads of the same file")
                .isEqualTo("$NON_PARQUET:$FR=10000.000_12500.000$SR=10000.000_10000.000$FL=37500.000$RL=10000.000");
    }
}
