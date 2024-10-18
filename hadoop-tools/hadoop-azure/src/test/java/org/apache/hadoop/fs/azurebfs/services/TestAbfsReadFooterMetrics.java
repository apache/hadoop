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

/**
 * Unit test for Abfs read footer metrics
 */
public class TestAbfsReadFooterMetrics {
    @Test
    public void testReadFooterMetrics() throws Exception {
        AbfsReadFooterMetrics metrics = new AbfsReadFooterMetrics();
        metrics.checkMetricUpdate("Test",
                Integer.parseInt("1000"), Long.parseLong("4000"), Long.parseLong("20"));
        metrics.checkMetricUpdate("Test1",
                Integer.parseInt("988"), Long.parseLong("1998"), Long.parseLong("20"));
        Assertions.assertThat(metrics.toString())
                .describedAs("Abfs read footer metrics value")
                .isEmpty();
    }
}
