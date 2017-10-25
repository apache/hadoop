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
package org.apache.hadoop.ozone.scm.container.placement.metrics;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

/**
 * This class is for maintaining StorageContainerManager statistics.
 */
@Metrics(about="Storage Container Manager Metrics", context="dfs")
public class SCMMetrics {
  public static final String SOURCE_NAME =
      SCMMetrics.class.getSimpleName();

  /**
   * Container stat metrics, the meaning of following metrics
   * can be found in {@link ContainerStat}.
   */
  @Metric private MutableGaugeLong lastContainerReportSize;
  @Metric private MutableGaugeLong lastContainerReportUsed;
  @Metric private MutableGaugeLong lastContainerReportKeyCount;
  @Metric private MutableGaugeLong lastContainerReportReadBytes;
  @Metric private MutableGaugeLong lastContainerReportWriteBytes;
  @Metric private MutableGaugeLong lastContainerReportReadCount;
  @Metric private MutableGaugeLong lastContainerReportWriteCount;

  public SCMMetrics() {
  }

  public static SCMMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME, "Storage Container Manager Metrics",
        new SCMMetrics());
  }

  public void setLastContainerReportSize(long size) {
    this.lastContainerReportSize.set(size);
  }

  public void setLastContainerReportUsed(long used) {
    this.lastContainerReportUsed.set(used);
  }

  public void setLastContainerReportKeyCount(long keyCount) {
    this.lastContainerReportKeyCount.set(keyCount);
  }

  public void setLastContainerReportReadBytes(long readBytes) {
    this.lastContainerReportReadBytes.set(readBytes);
  }

  public void setLastContainerReportWriteBytes(long writeBytes) {
    this.lastContainerReportWriteBytes.set(writeBytes);
  }

  public void setLastContainerReportReadCount(long readCount) {
    this.lastContainerReportReadCount.set(readCount);
  }

  public void setLastContainerReportWriteCount(long writeCount) {
    this.lastContainerReportWriteCount.set(writeCount);
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }
}
