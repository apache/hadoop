/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.service.timelineservice;

import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Write the metrics to a ATSv2. Generally, this class is instantiated via
 * hadoop-metrics2 property files. Specifically, you would create this class by
 * adding the following to by This would actually be set as: <code>
 * [prefix].sink.[some instance name].class
 * =org.apache.hadoop.yarn.service.timelineservice.ServiceMetricsSink
 * </code>, where <tt>prefix</tt> is "atsv2": and <tt>some instance name</tt> is
 * just any unique name, so properties can be differentiated if there are
 * multiple sinks of the same type created
 */
public class ServiceMetricsSink implements MetricsSink {

  private static final Logger log =
      LoggerFactory.getLogger(ServiceMetricsSink.class);

  private ServiceTimelinePublisher serviceTimelinePublisher;

  public ServiceMetricsSink() {

  }

  public ServiceMetricsSink(ServiceTimelinePublisher publisher) {
    serviceTimelinePublisher = publisher;
  }

  /**
   * Publishes service and component metrics to ATS.
   */
  @Override
  public void putMetrics(MetricsRecord record) {
    if (serviceTimelinePublisher.isStopped()) {
      log.warn("ServiceTimelinePublisher has stopped. "
          + "Not publishing any more metrics to ATS.");
      return;
    }

    boolean isServiceMetrics = false;
    boolean isComponentMetrics = false;
    String appId = null;
    for (MetricsTag tag : record.tags()) {
      if (tag.name().equals("type") && tag.value().equals("service")) {
        isServiceMetrics = true;
      } else if (tag.name().equals("type") && tag.value().equals("component")) {
        isComponentMetrics = true;
        break; // if component metrics, no more information required from tag so
               // break the loop
      } else if (tag.name().equals("appId")) {
        appId = tag.value();
      }
    }

    if (isServiceMetrics && appId != null) {
      log.debug("Publishing service metrics. {}", record);
      serviceTimelinePublisher.publishMetrics(record.metrics(), appId,
          ServiceTimelineEntityType.SERVICE_ATTEMPT.toString(),
          record.timestamp());
    } else if (isComponentMetrics) {
      log.debug("Publishing Component metrics. {}", record);
      serviceTimelinePublisher.publishMetrics(record.metrics(), record.name(),
          ServiceTimelineEntityType.COMPONENT.toString(), record.timestamp());
    }
  }

  @Override
  public void init(SubsetConfiguration conf) {
  }

  @Override
  public void flush() {
  }
}
