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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation;

import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.api.ContainerLogAggregationPolicy;
import org.apache.hadoop.yarn.server.api.ContainerLogContext;
import org.apache.hadoop.yarn.server.api.ContainerType;

/**
 * The sample policy samples logs of successful worker containers to aggregate.
 * It always aggregates AM container and failed/killed worker
 * containers' logs. To make sure small applications have enough logs, it only
 * applies sampling beyond minimal number of containers. The parameters can be
 * configured by SAMPLE_RATE and MIN_THRESHOLD. For example if SAMPLE_RATE is
 * 0.2 and MIN_THRESHOLD is 20, for an application with 100 successful
 * worker containers, 20 + (100-20) * 0.2 = 36 containers's logs will be
 * aggregated.
 */
@Private
public class SampleContainerLogAggregationPolicy implements
    ContainerLogAggregationPolicy  {
  private static final Logger LOG =
      LoggerFactory.getLogger(SampleContainerLogAggregationPolicy.class);

  static String SAMPLE_RATE = "SR";
  public static final float DEFAULT_SAMPLE_RATE = 0.2f;

  static String MIN_THRESHOLD = "MIN";
  public static final int DEFAULT_SAMPLE_MIN_THRESHOLD = 20;

  private float sampleRate = DEFAULT_SAMPLE_RATE;
  private int minThreshold = DEFAULT_SAMPLE_MIN_THRESHOLD;

  static public String buildParameters(float sampleRate, int minThreshold) {
    StringBuilder sb = new StringBuilder();
    sb.append(SAMPLE_RATE).append(":").append(sampleRate).append(",").
        append(MIN_THRESHOLD).append(":").append(minThreshold);
    return sb.toString();
  }

  // Parameters are comma separated properties, for example
  // "SR:0.5,MIN:50"
  public void parseParameters(String parameters) {
    Collection<String> params = StringUtils.getStringCollection(parameters);
    for(String param : params) {
      // The first element is the property name.
      // The second element is the property value.
      String[] property = StringUtils.getStrings(param, ":");
      if (property == null || property.length != 2) {
        continue;
      }
      if (property[0].equals(SAMPLE_RATE)) {
        try {
          float sampleRate = Float.parseFloat(property[1]);
          if (sampleRate >= 0.0 && sampleRate <= 1.0) {
            this.sampleRate = sampleRate;
          } else {
            LOG.warn("The format isn't valid. Sample rate falls back to the " +
                "default value " + DEFAULT_SAMPLE_RATE);
          }
        } catch (NumberFormatException nfe) {
          LOG.warn("The format isn't valid. Sample rate falls back to the " +
              "default value " + DEFAULT_SAMPLE_RATE);
        }
      } else if (property[0].equals(MIN_THRESHOLD)) {
        try {
          int minThreshold = Integer.parseInt(property[1]);
          if (minThreshold >= 0) {
            this.minThreshold = minThreshold;
          } else {
            LOG.warn("The format isn't valid. Min threshold falls back to " +
                "the default value " + DEFAULT_SAMPLE_MIN_THRESHOLD);
          }
        } catch (NumberFormatException nfe) {
          LOG.warn("The format isn't valid. Min threshold falls back to the " +
              "default value " + DEFAULT_SAMPLE_MIN_THRESHOLD);
        }
      }
    }
  }

  public boolean shouldDoLogAggregation(ContainerLogContext logContext) {
    if (logContext.getContainerType() ==
        ContainerType.APPLICATION_MASTER || logContext.getExitCode() != 0) {
      // If it is AM or failed or killed container, enable log aggregation.
      return true;
    }

    // Only sample log aggregation for large applications.
    // We assume the container id is continuously allocated from number 1 and
    // Worker containers start from id 2. So logs of worker containers with ids
    // in [2, minThreshold + 1] will be aggregated.
    if ((logContext.getContainerId().getContainerId() &
        ContainerId.CONTAINER_ID_BITMASK) < minThreshold + 2) {
      return true;
    }

    // Sample log aggregation for the rest of successful worker containers
    return (sampleRate != 0 &&
        logContext.getContainerId().hashCode() % (1/sampleRate) == 0);
  }
}