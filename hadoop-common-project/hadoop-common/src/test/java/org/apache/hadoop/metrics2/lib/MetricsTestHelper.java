/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.metrics2.lib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * A helper class that can provide test cases access to package-private
 * methods.
 */
public final class MetricsTestHelper {
  public static final Logger LOG =
      LoggerFactory.getLogger(MetricsTestHelper.class);

  private MetricsTestHelper() {
    //not called
  }

  /**
   * Replace the rolling averages windows for a
   * {@link MutableRollingAverages} metric.
   *
   */
  public static void replaceRollingAveragesScheduler(
      MutableRollingAverages mutableRollingAverages,
      int numWindows, long interval, TimeUnit timeUnit) {
    mutableRollingAverages.replaceScheduledTask(
        numWindows, interval, timeUnit);
  }
}
