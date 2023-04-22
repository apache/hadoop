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

package org.apache.hadoop.hdfs.server.protocol;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Outlier detection metrics - median, median absolute deviation, upper latency limit,
 * actual latency etc.
 */
@InterfaceAudience.Private
public class OutlierMetrics {

  private final Double median;
  private final Double mad;
  private final Double upperLimitLatency;
  private final Double actualLatency;

  public OutlierMetrics(Double median, Double mad, Double upperLimitLatency,
      Double actualLatency) {
    this.median = median;
    this.mad = mad;
    this.upperLimitLatency = upperLimitLatency;
    this.actualLatency = actualLatency;
  }

  public Double getMedian() {
    return median;
  }

  public Double getMad() {
    return mad;
  }

  public Double getUpperLimitLatency() {
    return upperLimitLatency;
  }

  public Double getActualLatency() {
    return actualLatency;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OutlierMetrics that = (OutlierMetrics) o;

    return new EqualsBuilder()
        .append(median, that.median)
        .append(mad, that.mad)
        .append(upperLimitLatency, that.upperLimitLatency)
        .append(actualLatency, that.actualLatency)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(median)
        .append(mad)
        .append(upperLimitLatency)
        .append(actualLatency)
        .toHashCode();
  }
}
