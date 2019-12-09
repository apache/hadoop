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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsAnnotations;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MetricsSourceBuilder;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is for maintaining the various Cleaner activity statistics and
 * publishing them through the metrics interfaces.
 */
@Private
@Evolving
@Metrics(name = "CleanerActivity", about = "Cleaner service metrics", context = "yarn")
public class CleanerMetrics {
  public static final Logger LOG =
      LoggerFactory.getLogger(CleanerMetrics.class);
  private final MetricsRegistry registry = new MetricsRegistry("cleaner");
  private final static CleanerMetrics INSTANCE = create();
  
  public static CleanerMetrics getInstance() {
    return INSTANCE;
  }

  @Metric("number of deleted files over all runs")
  private MutableCounterLong totalDeletedFiles;

  public long getTotalDeletedFiles() {
    return totalDeletedFiles.value();
  }

  private @Metric("number of deleted files in the last run")
  MutableGaugeLong deletedFiles;

  public long getDeletedFiles() {
    return deletedFiles.value();
  }

  private @Metric("number of processed files over all runs")
  MutableCounterLong totalProcessedFiles;

  public long getTotalProcessedFiles() {
    return totalProcessedFiles.value();
  }

  private @Metric("number of processed files in the last run")
  MutableGaugeLong processedFiles;

  public long getProcessedFiles() {
    return processedFiles.value();
  }

  @Metric("number of file errors over all runs")
  private MutableCounterLong totalFileErrors;

  public long getTotalFileErrors() {
    return totalFileErrors.value();
  }

  private @Metric("number of file errors in the last run")
  MutableGaugeLong fileErrors;

  public long getFileErrors() {
    return fileErrors.value();
  }

  private CleanerMetrics() {
  }

  /**
   * The metric source obtained after parsing the annotations
   */
  MetricsSource metricSource;

  static CleanerMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();

    CleanerMetrics metricObject = new CleanerMetrics();
    MetricsSourceBuilder sb = MetricsAnnotations.newSourceBuilder(metricObject);
    final MetricsSource s = sb.build();
    ms.register("cleaner", "The cleaner service of truly shared cache", s);
    metricObject.metricSource = s;
    return metricObject;
  }

  /**
   * Report a delete operation at the current system time
   */
  public void reportAFileDelete() {
    totalProcessedFiles.incr();
    processedFiles.incr();
    totalDeletedFiles.incr();
    deletedFiles.incr();
  }

  /**
   * Report a process operation at the current system time
   */
  public void reportAFileProcess() {
    totalProcessedFiles.incr();
    processedFiles.incr();
  }

  /**
   * Report a process operation error at the current system time
   */
  public void reportAFileError() {
    totalProcessedFiles.incr();
    processedFiles.incr();
    totalFileErrors.incr();
    fileErrors.incr();
  }

  /**
   * Report the start a new run of the cleaner.
   *
   */
  public void reportCleaningStart() {
    processedFiles.set(0);
    deletedFiles.set(0);
    fileErrors.set(0);
  }

}
