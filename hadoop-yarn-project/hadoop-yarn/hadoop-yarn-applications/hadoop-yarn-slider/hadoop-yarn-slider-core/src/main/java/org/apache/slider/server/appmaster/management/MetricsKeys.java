/*
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

package org.apache.slider.server.appmaster.management;

public interface MetricsKeys {

  /**
   * Prefix for metrics configuration options: {@value}
   */
  String METRICS_PREFIX = "slider.metrics.";
  
  /**
   * Boolean to enable Ganglia metrics reporting
   * {@value}
   */
  String METRICS_GANGLIA_ENABLED =
      METRICS_PREFIX + "ganglia.enabled";
  /**
   * {@value}
   */
  String METRICS_GANGLIA_HOST = METRICS_PREFIX + "ganglia.host";
  /**
   * {@value}
   */
  String METRICS_GANGLIA_PORT = METRICS_PREFIX + "ganglia.port";
  /**
   * {@value}
   */
  String METRICS_GANGLIA_VERSION_31 = METRICS_PREFIX + "ganglia.version-31";
  /**
   * {@value}
   */
  String METRICS_GANGLIA_REPORT_INTERVAL = METRICS_PREFIX + "ganglia.report.interval";
  /**
   * {@value}
   */
  int DEFAULT_GANGLIA_PORT = 8649;


  /**
   * Boolean to enable Logging metrics reporting
   * {@value}
   */
  String METRICS_LOGGING_ENABLED =
      METRICS_PREFIX + "logging.enabled";
  
  /**
   * String name of log to log to
   * {@value}
   */
  String METRICS_LOGGING_LOG =
      METRICS_PREFIX + "logging.log.name";

  /**
   * Default log name: {@value}
   */
  String METRICS_DEFAULT_LOG = 
      "org.apache.slider.metrics.log";


  /**
   * Int log interval in seconds
   * {@value}
   */
  String METRICS_LOGGING_LOG_INTERVAL =
      METRICS_PREFIX + "logging.interval.minutes";


  /**
   * Default log interval: {@value}.
   * This is a big interval as in a long lived service, log overflows are easy
   * to create. 
   */
  int METRICS_DEFAULT_LOG_INTERVAL = 60;
  
}
