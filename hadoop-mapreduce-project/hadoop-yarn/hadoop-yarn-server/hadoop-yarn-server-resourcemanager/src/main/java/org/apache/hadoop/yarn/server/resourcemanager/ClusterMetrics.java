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

package org.apache.hadoop.yarn.server.resourcemanager;

import static org.apache.hadoop.metrics2.lib.Interns.info;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;

@InterfaceAudience.Private
@Metrics(context="yarn")
public class ClusterMetrics {
  
  @Metric("# of NMs") MutableGaugeInt numNMs;
  @Metric("# of decommissioned NMs") MutableCounterInt numDecommissionedNMs;
  @Metric("# of lost NMs") MutableCounterInt numLostNMs;
  @Metric("# of unhealthy NMs") MutableGaugeInt numUnhealthyNMs;
  @Metric("# of Rebooted NMs") MutableGaugeInt numRebootedNMs;
  
  private static final MetricsInfo RECORD_INFO = info("ClusterMetrics",
  "Metrics for the Yarn Cluster");
  
  private static volatile ClusterMetrics INSTANCE = null;
  private static MetricsRegistry registry;
  
  public static ClusterMetrics getMetrics() {
    if(INSTANCE == null){
      synchronized (ClusterMetrics.class) {
        if(INSTANCE == null){
          INSTANCE = new ClusterMetrics();
          registerMetrics();
        }
      }
    }
    return INSTANCE;
  }

  private static void registerMetrics() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "ResourceManager");
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms != null) {
      ms.register("ClusterMetrics", "Metrics for the Yarn Cluster", INSTANCE);
    }
  }
  
  //Total Nodemanagers
  public int getNumNMs() {
    return numNMs.value();
  }
  
  //Decommisioned NMs
  public int getNumDecommisionedNMs() {
    return numDecommissionedNMs.value();
  }

  public void incrDecommisionedNMs() {
    numDecommissionedNMs.incr();
  }
  
  //Lost NMs
  public int getNumLostNMs() {
    return numLostNMs.value();
  }

  public void incrNumLostNMs() {
    numLostNMs.incr();
  }
  
  //Unhealthy NMs
  public int getUnhealthyNMs() {
    return numUnhealthyNMs.value();
  }

  public void incrNumUnhealthyNMs() {
    numUnhealthyNMs.incr();
  }
  
  public void decrNumUnhealthyNMs() {
    numUnhealthyNMs.decr();
  }
  
  //Rebooted NMs
  public int getNumRebootedNMs() {
    return numRebootedNMs.value();
  }
  
  public void incrNumRebootedNMs() {
    numRebootedNMs.incr();
  }
  
  public void removeNode(RMNodeEventType nodeEventType) {
    numNMs.decr();
    switch(nodeEventType){
    case DECOMMISSION: incrDecommisionedNMs(); break;
    case EXPIRE: incrNumLostNMs();break;
    case REBOOTING: incrNumRebootedNMs();break;
    }
  }
  
  public void addNode() {
    numNMs.incr();
  }
}
