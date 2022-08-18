/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.impl.prefetch;

import org.junit.Test;

import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestRetryer extends AbstractHadoopTestBase {

  @Test
  public void testArgChecks() throws Exception {
    // Should not throw.
    new Retryer(10, 50, 500);

    // Verify it throws correctly.

    intercept(IllegalArgumentException.class,
        "'perRetryDelay' must be a positive integer",
        () -> new Retryer(-1, 50, 500));

    intercept(IllegalArgumentException.class,
        "'perRetryDelay' must be a positive integer",
        () -> new Retryer(0, 50, 500));

    intercept(IllegalArgumentException.class,
        "'maxDelay' (5) must be greater than 'perRetryDelay' (10)",
        () -> new Retryer(10, 5, 500));

    intercept(IllegalArgumentException.class,
        "'statusUpdateInterval' must be a positive integer",
        () -> new Retryer(10, 50, -1));

    intercept(IllegalArgumentException.class,
        "'statusUpdateInterval' must be a positive integer",
        () -> new Retryer(10, 50, 0));

  }

  @Test
  public void testRetry() {
    int perRetryDelay = 1;
    int statusUpdateInterval = 3;
    int maxDelay = 10;

    Retryer retryer =
        new Retryer(perRetryDelay, maxDelay, statusUpdateInterval);
    for (int t = 1; t <= maxDelay; t++) {
      assertTrue(retryer.continueRetry());
      if (t % statusUpdateInterval == 0) {
        assertTrue(retryer.updateStatus());
      } else {
        assertFalse(retryer.updateStatus());
      }
    }

    assertFalse(retryer.continueRetry());
  }
}
