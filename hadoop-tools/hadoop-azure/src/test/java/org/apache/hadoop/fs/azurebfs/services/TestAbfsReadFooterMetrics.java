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
