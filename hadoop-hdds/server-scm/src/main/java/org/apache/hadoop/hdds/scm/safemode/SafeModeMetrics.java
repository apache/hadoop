/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.safemode;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

/**
 * This class is used for maintaining SafeMode metric information, which can
 * be used for monitoring during SCM startup when SCM is still in SafeMode.
 */
public class SafeModeMetrics {
  private static final String SOURCE_NAME =
      SafeModeMetrics.class.getSimpleName();


  // These all values will be set to some values when safemode is enabled.
  private @Metric MutableCounterLong
      numContainerWithOneReplicaReportedThreshold;
  private @Metric MutableCounterLong
      currentContainersWithOneReplicaReportedCount;

  // When hdds.scm.safemode.pipeline-availability.check is set then only
  // below metrics will have some values, otherwise they will be zero.
  private @Metric MutableCounterLong numHealthyPipelinesThreshold;
  private @Metric MutableCounterLong currentHealthyPipelinesCount;
  private @Metric MutableCounterLong
      numPipelinesWithAtleastOneReplicaReportedThreshold;
  private @Metric MutableCounterLong
      currentPipelinesWithAtleastOneReplicaReportedCount;

  public static SafeModeMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
        "SCM Safemode Metrics",
        new SafeModeMetrics());
  }

  public void setNumHealthyPipelinesThreshold(long val) {
    this.numHealthyPipelinesThreshold.incr(val);
  }

  public void incCurrentHealthyPipelinesCount() {
    this.currentHealthyPipelinesCount.incr();
  }

  public void setNumPipelinesWithAtleastOneReplicaReportedThreshold(long val) {
    this.numPipelinesWithAtleastOneReplicaReportedThreshold.incr(val);
  }

  public void incCurrentHealthyPipelinesWithAtleastOneReplicaReportedCount() {
    this.currentPipelinesWithAtleastOneReplicaReportedCount.incr();
  }

  public void setNumContainerWithOneReplicaReportedThreshold(long val) {
    this.numContainerWithOneReplicaReportedThreshold.incr(val);
  }

  public void incCurrentContainersWithOneReplicaReportedCount() {
    this.currentContainersWithOneReplicaReportedCount.incr();
  }

  public MutableCounterLong getNumHealthyPipelinesThreshold() {
    return numHealthyPipelinesThreshold;
  }

  public MutableCounterLong getCurrentHealthyPipelinesCount() {
    return currentHealthyPipelinesCount;
  }

  public MutableCounterLong
      getNumPipelinesWithAtleastOneReplicaReportedThreshold() {
    return numPipelinesWithAtleastOneReplicaReportedThreshold;
  }

  public MutableCounterLong getCurrentPipelinesWithAtleastOneReplicaCount() {
    return currentPipelinesWithAtleastOneReplicaReportedCount;
  }

  public MutableCounterLong getNumContainerWithOneReplicaReportedThreshold() {
    return numContainerWithOneReplicaReportedThreshold;
  }

  public MutableCounterLong getCurrentContainersWithOneReplicaReportedCount() {
    return currentContainersWithOneReplicaReportedCount;
  }


  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }
}
