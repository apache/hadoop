/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.insight;

import java.util.List;
import java.util.Map;

/**
 * Definition of a specific insight points.
 */
public interface InsightPoint {

  /**
   * Human readdable description.
   */
  String getDescription();

  /**
   * List of the related loggers.
   */
  List<LoggerSource> getRelatedLoggers(boolean verbose,
      Map<String, String> filters);

  /**
   * List of the related metrics.
   */
  List<MetricGroupDisplay> getMetrics();

  /**
   * List of the configuration classes.
   */
  List<Class> getConfigurationClasses();

  /**
   * Decide if the specific log should be displayed or not..
   */
  boolean filterLog(Map<String, String> filters, String logLine);

}
