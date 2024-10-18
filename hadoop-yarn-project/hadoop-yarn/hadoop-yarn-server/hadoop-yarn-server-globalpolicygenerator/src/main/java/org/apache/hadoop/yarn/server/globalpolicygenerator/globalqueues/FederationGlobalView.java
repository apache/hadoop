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

package org.apache.hadoop.yarn.server.globalpolicygenerator.globalqueues;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a set of root queues (one for each sub-cluster) of a
 * federation.
 */
public class FederationGlobalView implements Cloneable {
  private ResourceCalculator rc;

  public static final Logger LOG =
      LoggerFactory.getLogger(FederationGlobalView.class);

  private String name;
  private FederationQueue global;
  private List<FederationQueue> subClusters;
  private Configuration conf;

  public FederationGlobalView(){
    subClusters = new ArrayList<>();
  }

  public FederationGlobalView(Configuration config, ResourceCalculator rc) {
    this();
    this.conf=config;
    this.rc=rc;
  }

  public FederationGlobalView(Configuration config, ResourceCalculator rc,
      List<FederationQueue> subClusters) {
    this(config, rc);
    setSubClusters(subClusters);
    globalFromLocal();
  }

  /**
   * This method checks that certain queue invariants are respected.
   *
   * @throws FederationGlobalQueueValidationException upon violation.
   */
  public void validate() throws FederationGlobalQueueValidationException {
    try {
      if (global != null) {
        global.validate();
      }
      for (FederationQueue f : subClusters) {
        f.validate();
      }
    } catch(FederationGlobalQueueValidationException f) {
      LOG.error("Error in validating " + this.toQuickString());
      throw f;
    }
  }

  /**
   * Returns a FederationQueue matching the queueName
   * from the specified subClusters.
   *
   * @param queueName queue name.
   * @param subClusterName subCluster name.
   * @return FederationQueue corresponding to the queueName and subCluster
   */
  public FederationQueue getQueue(String queueName, String subClusterName) {
    for (FederationQueue f : subClusters) {
      if (f.getSubClusterId().equals(
          SubClusterId.newInstance(subClusterName))) {
        return f.getChildByName(queueName);
      }
    }
    return null;
  }

  /**
   * Get name of the FederationGlobalView.
   * @return name of the global view
   */
  public String getName() {
    return name;
  }

  /**
   * Set name of the FederationGlobalView.
   * @param name global view name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Get global view subclusters.
   * @return subclusters associated with global view
   */
  public List<FederationQueue> getSubClusters() {
    return subClusters;
  }

  /**
   * Set global view subclusters.
   * @param subClusters subclusters associated with global view
   */
  public void setSubClusters(List<FederationQueue> subClusters) {
    this.subClusters = subClusters;
  }

  /**
   * Creates a global queue by merging queues of all subclusters.
   */
  public void globalFromLocal() {
    // filling out the global object and propagating totCap
    FederationQueue globalQueue = FederationQueue.mergeQueues(
        this.getSubClusters(), SubClusterId.newInstance("global"));
    Resource totCap =
        Resources.componentwiseMax(globalQueue.getGuarCap(),
            globalQueue.getMaxCap());
    globalQueue.setTotCap(totCap);
    globalQueue.propagateCapacities();
    this.setGlobal(globalQueue);
  }

  public String toString() {
    return toQuickString();
  }

  /**
   * Produces a quick String representation of all the queues associated
   * with view.
   * Good for printing.
   * @return quick String representation of all the queues associated with view.
   */
  public String toQuickString() {
    StringBuilder sb = new StringBuilder();
    subClusters.forEach(sc -> sb.append(sc.toQuickString()).append("\n"));

    return sb.toString();
  }

  /**
   * Returns global queue associated with the view.
   * @return global queue.
   */
  public FederationQueue getGlobal() {
    return global;
  }

  /**
   * Set global queue for FederationGlobalView.
   * @param global queue for FederationGlobalView
   */
  public void setGlobal(FederationQueue global) {
    this.global = global;
  }

  // simply initialize the root to zero preemption
  protected void initializeRootPreemption() {
    global.setToBePreempted(Resource.newInstance(0, 0));
    for (FederationQueue lr : subClusters) {
      lr.setToBePreempted(Resource.newInstance(0, 0));
    }
  }

  public FederationGlobalView clone() throws CloneNotSupportedException {
    FederationGlobalView copy = (FederationGlobalView) super.clone();
    copy.setGlobal(global.clone(true));
    List<FederationQueue> clonedSubClusters = new ArrayList<>();
    for (FederationQueue localRoot : getSubClusters()) {
      clonedSubClusters.add(localRoot.clone(true));
    }
    copy.setSubClusters(clonedSubClusters);
    return copy;
  }
}
