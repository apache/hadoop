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

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DEFAULT_RESOURCE_CALCULATOR_CLASS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class represents a tree of queues in a sub-cluster YarnRM.
 * Useful to communicate with GPG and global policies.
 */
public class FederationQueue implements Iterable<FederationQueue> {

  private String queueName;
  private String queueType;

  // sub-cluster to which queue belongs.
  private SubClusterId subClusterId;

  // capacities associated with queue.
  private Resource totCap;
  private Resource guarCap;
  private Resource maxCap;
  private Resource usedCap;
  private Resource demandCap;
  private Resource idealAlloc;

  //resource that can be preempted in this queue.
  private Resource toBePreempted;

  // Used only for testing (to embed expected behavior)
  private Resource testExpectedIdealAlloc;

  private Map<String, FederationQueue> children;
  private ResourceCalculator rc;
  private Resource totalUnassigned;

  public static final Logger LOG =
      LoggerFactory.getLogger(FederationQueue.class);

  private Configuration conf;

  public FederationQueue() {
    this(new Configuration());
  }

  public FederationQueue(Configuration conf) {
    this(conf,
        ReflectionUtils.newInstance(
            conf.getClass(RESOURCE_CALCULATOR_CLASS,
                DEFAULT_RESOURCE_CALCULATOR_CLASS, ResourceCalculator.class),
            conf));
  }

  public FederationQueue(Configuration conf, ResourceCalculator rc) {
    this.conf = conf;
    children = new HashMap<>();
    this.rc = rc;
  }

  public FederationQueue(String queuename, SubClusterId subClusterId,
      Resource guar, Resource max, Resource used, Resource pending) {
    this.conf = new Configuration();
    this.rc =
        ReflectionUtils.newInstance(
            conf.getClass(RESOURCE_CALCULATOR_CLASS,
                DEFAULT_RESOURCE_CALCULATOR_CLASS, ResourceCalculator.class),
            conf);
    this.queueName = queuename;
    this.subClusterId = subClusterId;
    this.guarCap = guar;
    this.maxCap = max;
    this.usedCap = used;
    this.demandCap = pending;
    this.totCap = Resources.clone(guar);
    this.children = new HashMap<>();
  }

  /**
   * This method propagates from leaf to root all metrics, and pushes down the
   * total capacity from the root.
   */
  public void propagateCapacities() {
    rollDownCapacityFromRoot(totCap);
    rollUpMetricsFromChildren();
  }

  private void rollDownCapacityFromRoot(Resource rootCap) {
    totCap = rootCap;
    for (FederationQueue c: children.values()) {
      c.rollDownCapacityFromRoot(rootCap);
    }
  }

  private void rollUpMetricsFromChildren() {
    Resource childGuar = Resources.createResource(0L, 0);
    Resource childMax = Resources.createResource(0L, 0);
    Resource childSumOfMax = Resources.createResource(0L, 0);
    Resource childUsed = Resources.createResource(0L, 0);
    Resource childDem = Resources.createResource(0L, 0);

    // this pull the leaf data up
    for (FederationQueue c : children.values()) {
      c.rollUpMetricsFromChildren();
      if (c.getGuarCap() != null) {
        Resources.addTo(childGuar, c.getGuarCap());
      }
      if (c.getMaxCap() != null) {
        Resources.addTo(childSumOfMax, c.getMaxCap());
        childMax = Resources.max(rc, totCap, childMax, c.getMaxCap());
      }
      if (c.getUsedCap() != null) {
        Resources.addTo(childUsed, c.getUsedCap());
      }
      if (c.getDemandCap() != null) {
        Resources.addTo(childDem, c.getDemandCap());
      }
    }
    if (children.size() > 0) {
      setGuarCap(childGuar);
      setMaxCap(Resources.componentwiseMin(
          Resources.componentwiseMax(childMax, childGuar), totCap));
      setUsedCap(childUsed);
      setDemandCap(childDem);
    }
  }

  /**
   * This method checks that certain queue invariants are respected.
   *
   * @throws FederationGlobalQueueValidationException upon violation.
   */
  public void validate() throws FederationGlobalQueueValidationException {

    if (totCap == null) {
      throw new FederationGlobalQueueValidationException(
          "Total capacity must be configured");
    }

    if (Resources.lessThan(rc, totCap, usedCap, Resources.none())) {
      throw new FederationGlobalQueueValidationException(
          "usedCap (" + usedCap + ") exceeds totCap (" + totCap + ") for queue "
              + this.getQueueName() + "@" + this.getSubClusterId());
    }

    if (!Resources.fitsIn(guarCap, totCap)) {
      throw new FederationGlobalQueueValidationException(
          "guarCap (" + guarCap + ") exceeds total capacity (" + totCap
              + "  for queue " + this.getQueueName() +
              "@" + this.getSubClusterId());
    }

    if (Resources.lessThan(rc, totCap, guarCap, Resources.none())) {
      throw new FederationGlobalQueueValidationException(
          "guarCap (" + guarCap + ") is outside [0,+inf] range for queue "
              + this.getQueueName() +
              "@" + this.getSubClusterId());
    }

    if (!Resources.fitsIn(guarCap, maxCap)) {
      throw new FederationGlobalQueueValidationException("maxCap (" + maxCap
          + ") is outside [" + guarCap + ",+inf] range for queue "
          + this.getQueueName() +
          "@" + this.getSubClusterId());

    }

    if (Resources.lessThan(rc, totCap, usedCap, Resources.none())
        || !Resources.fitsIn(usedCap, maxCap)) {
      throw new FederationGlobalQueueValidationException("usedCap (" + usedCap
          + ") is outside [0," + maxCap + "] range for queue "
          + this.getQueueName() +
          "@" + this.getSubClusterId());
    }

    if (Resources.lessThan(rc, totCap, demandCap, Resources.none())) {
      throw new FederationGlobalQueueValidationException(
          "demandCap (" + demandCap + ") is outside [0,+inf] range for queue "
              + this.getQueueName() +
              "@" + this.getSubClusterId());
    }
    if (idealAlloc != null && !Resources.fitsIn(idealAlloc, totCap)) {
      throw new FederationGlobalQueueValidationException(
          "idealAlloc (" + idealAlloc + ") is greter than totCap (" + totCap
              + ") for queue " + this.getQueueName() +
              "@" + this.getSubClusterId());
    }

    if (children != null && children.size() > 0) {
      Resource childGuar = Resources.createResource(0L, 0);
      Resource childMax = Resources.createResource(0L, 0);
      Resource childUsed = Resources.createResource(0L, 0);
      Resource childDem = Resources.createResource(0L, 0);
      Resource childIdealAlloc = Resources.createResource(0, 0);

      for (FederationQueue c : children.values()) {
        Resources.addTo(childGuar, c.getGuarCap());
        Resources.addTo(childUsed, c.getUsedCap());
        Resources.addTo(childDem, c.getDemandCap());
        if (c.idealAlloc != null) {
          Resources.addTo(childIdealAlloc, c.getIdealAlloc());
        }
        if (!Resources.lessThanOrEqual(rc, totCap, childMax, maxCap)) {
          throw new FederationGlobalQueueValidationException(
              "Sum of children maxCap (" + childMax
                  + ") mismatched with parent maxCap (" + maxCap
                  + ") for queue " + this.getQueueName() + "@"
                  + this.getSubClusterId());
        }

        c.validate();
      }

      if (!Resources.equals(childGuar, guarCap)) {
        throw new FederationGlobalQueueValidationException(
            "Sum of children guarCap (" + childGuar
                + ") mismatched with parent guarCap (" + guarCap
                + ") for queue " + this.getQueueName() +
                "@" + this.getSubClusterId());
      }

      if (!Resources.equals(childUsed, usedCap)) {
        throw new FederationGlobalQueueValidationException(
            "Sum of children usedCap (" + childUsed
                + ") mismatched with parent usedCap (" + usedCap
                + ") for queue " + this.getQueueName() +
                "@" + this.getSubClusterId());
      }

      if (!Resources.equals(childDem, demandCap)) {
        throw new FederationGlobalQueueValidationException(
            "Sum of children demandCap (" + childGuar
                + ") mismatched with parent demandCap (" + demandCap
                + ") for queue " + this.getQueueName() +
                "@" + this.getSubClusterId());
      }

      if (idealAlloc != null
          && !Resources.fitsIn(childIdealAlloc, idealAlloc)) {
        throw new FederationGlobalQueueValidationException(
            "Sum of children idealAlloc (" + childIdealAlloc
                + ") exceed the parent idealAlloc (" + idealAlloc
                + ") for queue " + this.getQueueName() +
                "@" + this.getSubClusterId());
      }
    }
  }

  /**
   * This method clones the FederationQueue.
   * @param recursive whether to clone recursively.
   * @return cloned object of Federation Queue.
   */
  public FederationQueue clone(boolean recursive) {
    FederationQueue metoo = new FederationQueue(this.conf, rc);
    metoo.queueName = queueName;
    metoo.subClusterId = subClusterId;
    metoo.totCap = Resources.clone(totCap);
    metoo.guarCap = Resources.clone(guarCap);
    metoo.maxCap = Resources.clone(maxCap);
    metoo.usedCap = Resources.clone(usedCap);
    metoo.demandCap = Resources.clone(demandCap);
    metoo.idealAlloc =
        (idealAlloc != null) ? Resources.clone(idealAlloc) : null;
    metoo.toBePreempted =
        (toBePreempted != null) ? Resources.clone(toBePreempted) : null;
    metoo.testExpectedIdealAlloc = (testExpectedIdealAlloc != null)
        ? Resources.clone(testExpectedIdealAlloc) : null;
    for (Map.Entry<String, FederationQueue> c : children.entrySet()) {
      if (recursive) {
        metoo.children.put(c.getKey(), c.getValue().clone(true));
      } else {
        metoo.children.put(c.getKey(), c.getValue());
      }
    }
    return metoo;
  }

  /**
   * This operation combine every level of the queue and produces a merged tree.
   *
   * @param subClusterQueues the input queues to merge
   * @param newScope subClusterId
   * @return the root of the merged FederationQueue tree
   */
  public static FederationQueue mergeQueues(
      List<FederationQueue> subClusterQueues, SubClusterId newScope) {

    FederationQueue combined = null;

    for (FederationQueue root : subClusterQueues) {
      if (combined == null) {
        combined = root.clone(false);
        combined.setSubClusterId(newScope);
        continue;
      }
      combined.setTotCap(Resources
          .clone(Resources.add(combined.getTotCap(), root.getTotCap())));
      combined.setGuarCap(Resources
          .clone(Resources.add(combined.getGuarCap(), root.getGuarCap())));
      combined.setMaxCap(
          Resources.clone(Resources.componentwiseMax(combined.getTotCap(),
              Resources.add(combined.getMaxCap(), root.getMaxCap()))));
      combined.setUsedCap(Resources
          .clone(Resources.add(combined.getUsedCap(), root.getUsedCap())));
      combined.setDemandCap(Resources
          .clone(Resources.add(combined.getDemandCap(), root.getDemandCap())));

      Map<String, FederationQueue> newChildren = new HashMap<>();
      for (Map.Entry<String, FederationQueue> mychild :
          combined.children.entrySet()) {
        FederationQueue theirchild = root.getChildren().get(mychild.getKey());
        List<FederationQueue> mergelist = new ArrayList<>();
        mergelist.add(mychild.getValue());
        mergelist.add(theirchild);
        newChildren.put(mychild.getKey(), mergeQueues(mergelist, newScope));
      }
      combined.children = newChildren;
    }

    combined.propagateCapacities();
    return combined;
  }

  /**
   * Get child FederationQueue by name.
   * @param pQueueName name of the queue.
   * @return children FederationQueue.
   */
  public FederationQueue getChildByName(String pQueueName) {
    return recursiveChildByName(this, pQueueName);
  }

  private static FederationQueue recursiveChildByName(FederationQueue f,
      String a) {
    if (f == null) {
      return null;
    }
    if (f.getQueueName() != null && f.getQueueName().equals(a)) {
      return f;
    }
    if (f.getChildren().get(a) != null) {
      return f.getChildren().get(a);
    }

    for (FederationQueue c : f.getChildren().values()) {
      FederationQueue ret = recursiveChildByName(c, a);
      if (ret != null) {
        return ret;
      }
    }
    return null;
  }

  /**
   * Sets total capacity for FederationQueue and children.
   * @param totCapacity total capacity of the queue.
   */
  public void recursiveSetOfTotCap(Resource totCapacity) {
    this.setTotCap(totCapacity);
    for (FederationQueue child : this.getChildren().values()) {
      child.recursiveSetOfTotCap(totCapacity);
    }
  }

  /**
   * Get the queue total unassigned resources.
   * @return queue unassigned resources.
   */
  public Resource getTotalUnassigned() {
    return totalUnassigned;
  }

  /**
   * Set the queue total unassigned resources.
   * @param totalUnassigned queue totalUnassigned resources.
   */
  public void setTotalUnassigned(Resource totalUnassigned) {
    this.totalUnassigned = totalUnassigned;
  }

  /**
   * Get the queue configuration.
   * @return queue configuration
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Set the queue configuration.
   * @param conf queue configuration
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Get queue guaranteed capacity.
   * @return queue guaranteed capacity
   */
  public Resource getGuarCap() {
    return guarCap;
  }

  /**
   * Set queue guaranteed capacity.
   * @param guarCap queue guaranteed capacity
   */
  public void setGuarCap(Resource guarCap) {
    this.guarCap = guarCap;
  }

  /**
   * Get queue max capacity.
   * @return queue max capacity
   */
  public Resource getMaxCap() {
    return maxCap;
  }

  /**
   * Set queue max capacity.
   * @param maxCap max capacity of the queue
   */
  public void setMaxCap(Resource maxCap) {
    this.maxCap = maxCap;
  }

  /**
   * Get queue used capacity.
   * @return queue used capacity
   */
  public Resource getUsedCap() {
    return usedCap;
  }

  /**
   * Set queue used capacity.
   * @param usedCap queue used capacity
   */
  public void setUsedCap(Resource usedCap) {
    this.usedCap = usedCap;
  }

  /**
   * Get queue demand capacity.
   * @return queue demand capacity
   */
  public Resource getDemandCap() {
    return demandCap;
  }

  /**
   * Set queue demand capacity.
   * @param demandCap queue demand capacity
   */
  public void setDemandCap(Resource demandCap) {
    this.demandCap = demandCap;
  }

  /**
   * Get queue children.
   * @return queue children
   */
  public Map<String, FederationQueue> getChildren() {
    return children;
  }

  /**
   * Set queue children.
   * @param children queue children
   */
  public void setChildren(Map<String, FederationQueue> children) {
    this.children = children;
  }

  /**
   * Get queue name.
   * @return queue name
   */
  public String getQueueName() {
    return queueName;
  }

  /**
   * Get queue total capacity.
   * @return queue total capacity
   */
  public Resource getTotCap() {
    return totCap;
  }

  /**
   * Set queue total capacity.
   * @param totCap queue total capacity
   */
  public void setTotCap(Resource totCap) {
    this.totCap = totCap;
  }

  /**
   * Set queue name.
   * @param queueName queue name
   */
  public void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  /**
   * Get queue ideal allocation.
   * @return queue ideal allocation
   */
  public Resource getIdealAlloc() {
    return idealAlloc;
  }

  /**
   * Set queue ideal allocation.
   * @param idealAlloc queue ideal allocation
   */
  public void setIdealAlloc(Resource idealAlloc) {
    this.idealAlloc = idealAlloc;
  }

  /**
   * Get queue resources to be preempted.
   * @return queue resources to be preempted
   */
  public Resource getToBePreempted() {
    return toBePreempted;
  }

  /**
   * Set queue resources to be preempted.
   * @param toBePreempted queue resources to be preempted
   */
  public void setToBePreempted(Resource toBePreempted) {
    this.toBePreempted = toBePreempted;
  }

  /**
   * Get queue subcluster id.
   * @return queue subcluster id
   */
  public SubClusterId getSubClusterId() {
    return subClusterId;
  }

  /**
   * Set queue subcluster id.
   * @param subClusterId queue subcluster id
   */
  public void setSubClusterId(SubClusterId subClusterId) {
    this.subClusterId = subClusterId;
  }

  /**
   * Set queue type.
   * @param queueType queue type
   */
  public void setQueueType(String queueType) {
    this.queueType = queueType;
  }

  /**
   * Get queue type.
   * @return queue type
   */
  public String getQueueType() {
    return queueType;
  }

  /**
   * Get queue expected ideal allocation.
   * @return queue ideal allocation
   */
  public Resource getExpectedIdealAlloc() {
    return testExpectedIdealAlloc;
  }

  public String toString() {
    return toQuickString();
  }

  /**
   * Produces a quick String representation of the queue rooted at this node.
   * Good for printing.
   * @return quick String representation of the queue rooted at this node.
   */
  public String toQuickString() {
    return this.appendToSB(new StringBuilder(), 0).toString();
  }

  /**
   * Append queue hierarchy rooted at this node to the given StringBuilder.
   */
  private StringBuilder appendToSB(StringBuilder sb, int depth) {
    sb.append("\n").append(String.join("", Collections.nCopies(depth, "\t")))
        .append(queueName);
    if (depth == 0) {
      sb.append("(" + subClusterId + ")");
    }
    sb.append(" [g: ").append(guarCap.getMemorySize()).append("/")
        .append(guarCap.getVirtualCores()).append(", m: ")
        .append(maxCap.getMemorySize()).append("/")
        .append(maxCap.getVirtualCores()).append(", u: ")
        .append(usedCap.getMemorySize()).append("/")
        .append(usedCap.getVirtualCores()).append(", d: ")
        .append(demandCap.getMemorySize()).append("/")
        .append(demandCap.getVirtualCores()).append(", t: ")
        .append(totCap.getMemorySize()).append("/")
        .append(totCap.getVirtualCores());
    if (idealAlloc != null) {
      sb.append(", i: ").append(idealAlloc.getMemorySize()).append("/")
          .append(idealAlloc.getVirtualCores());
    }
    sb.append("]");
    if (children != null && !children.isEmpty()) {
      children.values().forEach(c -> c.appendToSB(sb, depth + 1));
    }
    return sb;
  }

  /**
   * Count the total child queues.
   * @return total child queues
   */
  public long countQueues() {
    long count = 1;
    for (FederationQueue q : getChildren().values()) {
      count += q.countQueues();
    }
    return count;
  }

  /**
   * Checks whether queue is leaf queue.
   * @return is queue leaf queue
   */
  public boolean isLeaf() {
    return this.getChildren() == null || this.getChildren().isEmpty();
  }

  /**
   * Get queue number of children.
   * @return number of queue children
   */
  public int childrenNum() {
    return this.getChildren() != null ? this.getChildren().size() : 0;
  }

  /**
   * @return True if the sum of used and pending resources for this queue are smaller
   * than the guaranteed resources.
   */
  public boolean isUnderutilized() {
    return Resources.fitsIn(
        Resources.add(this.getUsedCap(), this.getDemandCap()),
        this.getGuarCap());
  }

  /**
   * @return Return a stream of the current FederationQueue (uses the FedQueueIterator).
   */
  public Stream<FederationQueue> stream() {
    return StreamSupport.stream(this.spliterator(), false);
  }

  /**
   * @return Stream all leaf nodes of the FederationQueue hierarchy.
   */
  public Stream<FederationQueue> streamLeafQs() {
    return this.stream().filter(FederationQueue::isLeaf);
  }

  /**
   * @return Stream all leaf nodes that have non-zero guaranteed capacity.
   */
  public Stream<FederationQueue> streamNonEmptyLeafQs() {
    return this.streamLeafQs()
        .filter(leafQ -> leafQ.getGuarCap().getMemorySize() > 0);
  }

  /**
   * @return Stream all inner nodes of the FederationQueue hierarchy.
   */
  public Stream<FederationQueue> streamInnerQs() {
    return this.stream().filter(q -> !q.isLeaf());
  }

  @Override
  public Iterator<FederationQueue> iterator() {
    return new FedQueueIterator(this);
  }

  /**
   * Iterator for FederationQueue.
   */
  private static final class FedQueueIterator implements
      Iterator<FederationQueue> {

    private Deque<FederationQueue> state;
    private FederationQueue crt;

    FedQueueIterator(FederationQueue root) {
      this.state = new ArrayDeque<>();
      state.push(root);
    }

    @Override
    public boolean hasNext() {
      return !state.isEmpty();
    }

    @Override
    public FederationQueue next() {
      crt = state.pop();
      if (crt.getChildren() != null && !crt.getChildren().isEmpty()) {
        state.addAll(crt.getChildren().values());
      }
      return crt;
    }
  }
}
