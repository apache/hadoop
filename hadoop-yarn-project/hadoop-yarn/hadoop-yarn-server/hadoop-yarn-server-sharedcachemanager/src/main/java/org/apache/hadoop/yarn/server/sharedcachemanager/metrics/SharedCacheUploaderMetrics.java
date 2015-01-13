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
package org.apache.hadoop.yarn.server.sharedcachemanager.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

/**
 * This class is for maintaining shared cache uploader requests metrics
 * and publishing them through the metrics interfaces.
 */
@Private
@Evolving
@Metrics(about="shared cache upload metrics", context="yarn")
public class SharedCacheUploaderMetrics {

  static final Log LOG =
      LogFactory.getLog(SharedCacheUploaderMetrics.class);
  final MetricsRegistry registry;
  private final static SharedCacheUploaderMetrics INSTANCE = create();

  private SharedCacheUploaderMetrics() {
    registry = new MetricsRegistry("SharedCacheUploaderRequests");
    LOG.debug("Initialized "+ registry);
  }

  public static SharedCacheUploaderMetrics getInstance() {
    return INSTANCE;
  }

  static SharedCacheUploaderMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();

    SharedCacheUploaderMetrics metrics =
        new SharedCacheUploaderMetrics();
    ms.register("SharedCacheUploaderRequests", null, metrics);
    return metrics;
  }

  @Metric("Number of accepted uploads") MutableCounterLong acceptedUploads;
  @Metric("Number of rejected uploads") MutableCounterLong rejectedUploads;

  /**
   * One accepted upload event
   */
  public void incAcceptedUploads() {
    acceptedUploads.incr();
  }

  /**
   * One rejected upload event
   */
  public void incRejectedUploads() {
    rejectedUploads.incr();
  }

  public long getAcceptedUploads() { return acceptedUploads.value(); }
  public long getRejectUploads() { return rejectedUploads.value(); }
}
