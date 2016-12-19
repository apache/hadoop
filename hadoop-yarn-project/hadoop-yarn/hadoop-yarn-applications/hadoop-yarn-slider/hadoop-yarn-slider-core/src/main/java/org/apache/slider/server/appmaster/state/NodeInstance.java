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

package org.apache.slider.server.appmaster.state;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.slider.api.types.NodeInformation;
import org.apache.slider.common.tools.Comparators;
import org.apache.slider.common.tools.SliderUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * A node instance -stores information about a node in the cluster.
 * <p>
 * Operations on the array/set of roles are synchronized.
 */
public class NodeInstance {

  public final String hostname;

  /**
   * last state of node. Starts off as {@link NodeState#RUNNING},
   * on the assumption that it is live.
   */
  private NodeState nodeState = NodeState.RUNNING;

  /**
   * Last node report. If null: none
   */
  private NodeReport nodeReport = null;

  /**
   * time of state update
   */
  private long nodeStateUpdateTime = 0;

  /**
   * Node labels.
   *
   * IMPORTANT: we assume that there is one label/node, which is the policy
   * for Hadoop as of November 2015
   */
  private String nodeLabels = "";

  /**
   * An unordered list of node entries of specific roles. There's nothing
   * indexed so as to support sparser datastructures.
   */
  private final List<NodeEntry> nodeEntries;

  /**
   * Create an instance and the (empty) array of nodes
   * @param roles role count -the no. of roles
   */
  public NodeInstance(String hostname, int roles) {
    this.hostname = hostname;
    nodeEntries = new ArrayList<>(roles);
  }

  /**
   * Update the node status.
   * The return code is true if the node state changed enough to
   * trigger a re-evaluation of pending requests. That is, either a node
   * became available when it was previously not, or the label changed
   * on an available node.
   *
   * Transitions of a node from live to dead aren't treated as significant,
   * nor label changes on a dead node.
   *
   * @param report latest node report
   * @return true if the node state changed enough for a request evaluation.
   */
  public synchronized boolean updateNode(NodeReport report) {
    nodeStateUpdateTime = report.getLastHealthReportTime();
    nodeReport = report;
    NodeState oldState = nodeState;
    boolean oldStateUnusable = oldState.isUnusable();
    nodeState = report.getNodeState();
    boolean newUsable = !nodeState.isUnusable();
    boolean nodeNowAvailable = oldStateUnusable && newUsable;
    String labels = this.nodeLabels;
    nodeLabels = SliderUtils.extractNodeLabel(report);
    return nodeNowAvailable
        || newUsable && !this.nodeLabels.equals(labels);
  }

  public String getNodeLabels() {
    return nodeLabels;
  }

  /**
   * Get the entry for a role -if present
   * @param role role index
   * @return the entry
   * null if the role is out of range
   */
  public synchronized NodeEntry get(int role) {
    for (NodeEntry nodeEntry : nodeEntries) {
      if (nodeEntry.rolePriority == role) {
        return nodeEntry;
      }
    }
    return null;
  }
  
  /**
   * Get the entry for a role -if present
   * @param role role index
   * @return the entry
   * @throws ArrayIndexOutOfBoundsException if the role is out of range
   */
  public synchronized NodeEntry getOrCreate(int role) {
    NodeEntry entry = get(role);
    if (entry == null) {
      entry = new NodeEntry(role);
      nodeEntries.add(entry);
    }
    return entry;
  }

  /**
   * Get the node entry matching a container on this node
   * @param container container
   * @return matching node instance for the role
   */
  public NodeEntry getOrCreate(Container container) {
    return getOrCreate(ContainerPriority.extractRole(container));
  }

  /**
   * Count the number of active role instances on this node
   * @param role role index
   * @return 0 if there are none, otherwise the #of nodes that are running and
   * not being released already.
   */
  public int getActiveRoleInstances(int role) {
    NodeEntry nodeEntry = get(role);
    return (nodeEntry != null ) ? nodeEntry.getActive() : 0;
  }
  
  /**
   * Count the number of live role instances on this node
   * @param role role index
   * @return 0 if there are none, otherwise the #of nodes that are running 
   */
  public int getLiveRoleInstances(int role) {
    NodeEntry nodeEntry = get(role);
    return (nodeEntry != null ) ? nodeEntry.getLive() : 0;
  }

  /**
   * Is the node considered online
   * @return the node
   */
  public boolean isOnline() {
    return !nodeState.isUnusable();
  }

  /**
   * Query for a node being considered unreliable
   * @param role role key
   * @param threshold threshold above which a node is considered unreliable
   * @return true if the node is considered unreliable
   */
  public boolean isConsideredUnreliable(int role, int threshold) {
    NodeEntry entry = get(role);
    return entry != null && entry.getFailedRecently() > threshold;
  }

  /**
   * Get the entry for a role -and remove it if present
   * @param role the role index
   * @return the entry that WAS there
   */
  public synchronized NodeEntry remove(int role) {
    NodeEntry nodeEntry = get(role);
    if (nodeEntry != null) {
      nodeEntries.remove(nodeEntry);
    }
    return nodeEntry;
  }

  public synchronized void set(int role, NodeEntry nodeEntry) {
    remove(role);
    nodeEntries.add(nodeEntry);
  }

  /**
   * run through each entry; gc'ing & removing old ones that don't have
   * a recent failure count (we care about those)
   * @param absoluteTime age in millis
   * @return true if there are still entries left
   */
  public synchronized boolean purgeUnusedEntries(long absoluteTime) {
    boolean active = false;
    ListIterator<NodeEntry> entries = nodeEntries.listIterator();
    while (entries.hasNext()) {
      NodeEntry entry = entries.next();
      if (entry.notUsedSince(absoluteTime) && entry.getFailedRecently() == 0) {
        entries.remove();
      } else {
        active = true;
      }
    }
    return active;
  }


  /**
   * run through each entry resetting the failure count
   */
  public synchronized void resetFailedRecently() {
    for (NodeEntry entry : nodeEntries) {
      entry.resetFailedRecently();
    }
  }
  
  @Override
  public String toString() {
    return hostname;
  }

  /**
   * Full dump of entry including children
   * @return a multi-line description fo the node
   */
  public String toFullString() {
    final StringBuilder sb =
      new StringBuilder(toString());
    sb.append("{ ");
    for (NodeEntry entry : nodeEntries) {
      sb.append(String.format("%n  [%02d]  ", entry.rolePriority));
        sb.append(entry.toString());
    }
    sb.append("} ");
    return sb.toString();
  }

  /**
   * Equality test is purely on the hostname of the node address
   * @param o other
   * @return true if the hostnames are equal
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NodeInstance that = (NodeInstance) o;
    return hostname.equals(that.hostname);
  }

  @Override
  public int hashCode() {
    return hostname.hashCode();
  }


  /**
   * Predicate to query if the number of recent failures of a role
   * on this node exceeds that role's failure threshold.
   * If there is no record of a deployment of that role on this
   * node, the failure count is taken as "0".
   * @param role role to look up
   * @return true if the failure rate is above the threshold.
   */
  public boolean exceedsFailureThreshold(RoleStatus role) {
    NodeEntry entry = get(role.getKey());
    int numFailuresOnLastHost = entry != null ? entry.getFailedRecently() : 0;
    int failureThreshold = role.getNodeFailureThreshold();
    return failureThreshold < 0 || numFailuresOnLastHost > failureThreshold;
  }

  /**
   * Produced a serialized form which can be served up as JSON
   * @param naming map of priority -> value for naming entries
   * @return a summary of the current role status.
   */
  public synchronized NodeInformation serialize(Map<Integer, String> naming) {
    NodeInformation info = new NodeInformation();
    info.hostname = hostname;
    // null-handling state constructor
    info.state = "" + nodeState;
    info.lastUpdated = nodeStateUpdateTime;
    info.labels = nodeLabels;
    if (nodeReport != null) {
      info.httpAddress = nodeReport.getHttpAddress();
      info.rackName = nodeReport.getRackName();
      info.healthReport = nodeReport.getHealthReport();
    }
    info.entries = new HashMap<>(nodeEntries.size());
    for (NodeEntry nodeEntry : nodeEntries) {
      String name = naming.get(nodeEntry.rolePriority);
      if (name == null) {
        name = Integer.toString(nodeEntry.rolePriority);
      }
      info.entries.put(name, nodeEntry.serialize());
    }
    return info;
  }

  /**
   * Is this node instance a suitable candidate for the specific role?
   * @param role role ID
   * @param label label which must match, or "" for no label checks
   * @return true if the node has space for this role, is running and the labels
   * match.
   */
  public boolean canHost(int role, String label) {
    return isOnline()
        && (SliderUtils.isUnset(label) || label.equals(nodeLabels))   // label match
        && getOrCreate(role).isAvailable();                          // no live role
  }

  /**
   * A comparator for sorting entries where the node is preferred over another.
   *
   * The exact algorithm may change: current policy is "most recent first", so sorted
   * on the lastUsed
   *
   * the comparision is a positive int if left is preferred to right;
   * negative if right over left, 0 for equal
   */
  public static class Preferred implements Comparator<NodeInstance>, Serializable {

    private static final Comparators.InvertedLongComparator comparator =
        new Comparators.InvertedLongComparator();
    private final int role;

    public Preferred(int role) {
      this.role = role;
    }

    @Override
    public int compare(NodeInstance o1, NodeInstance o2) {
      NodeEntry left = o1.get(role);
      NodeEntry right = o2.get(role);
      long ageL = left != null ? left.getLastUsed() : -1;
      long ageR = right != null ? right.getLastUsed() : -1;
      return comparator.compare(ageL, ageR);
    }
  }

  /**
   * A comparator for sorting entries where the role is newer than
   * the other. 
   * This sort only compares the lastUsed field, not whether the
   * node is in use or not
   */
  public static class MoreActiveThan implements Comparator<NodeInstance>,
                                           Serializable {

    private final int role;

    public MoreActiveThan(int role) {
      this.role = role;
    }

    @Override
    public int compare(NodeInstance left, NodeInstance right) {
      int activeLeft = left.getActiveRoleInstances(role);
      int activeRight = right.getActiveRoleInstances(role);
      return activeRight - activeLeft;
    }
  }
  /**
   * A comparator for sorting entries alphabetically
   */
  public static class CompareNames implements Comparator<NodeInstance>,
                                           Serializable {

    public CompareNames() {
    }

    @Override
    public int compare(NodeInstance left, NodeInstance right) {
      return left.hostname.compareTo(right.hostname);
    }
  }


}
