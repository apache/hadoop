/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.util.MonotonicClock;

/**
 * DecommissioningNodesWatcher is used by ResourceTrackerService to track
 * DECOMMISSIONING nodes to decide when, after all running containers on
 * the node have completed, will be transitioned into DECOMMISSIONED state
 * (NodeManager will be told to shutdown).
 * Under MR application, a node, after completes all its containers,
 * may still serve it map output data during the duration of the application
 * for reducers. A fully graceful mechanism would keep such DECOMMISSIONING
 * nodes until all involved applications complete. It could be however
 * undesirable under long-running applications scenario where a bunch
 * of "idle" nodes would stay around for long period of time.
 *
 * DecommissioningNodesWatcher balance such concern with a timeout policy ---
 * a DECOMMISSIONING node will be DECOMMISSIONED no later than
 * DECOMMISSIONING_TIMEOUT regardless of running containers or applications.
 *
 * DecommissioningNodesWatcher basically is no cost when no node is
 * DECOMMISSIONING.
 */
public class DecommissioningNodesWatcher {
  private static final Logger LOG =
      LoggerFactory.getLogger(DecommissioningNodesWatcher.class);

  private final RMContext rmContext;

  // Once a RMNode is observed in DECOMMISSIONING state,
  // All its ContainerStatus update are tracked inside DecomNodeContext.
  class DecommissioningNodeContext {
    private final NodeId nodeId;

    // Last known NodeState.
    private NodeState nodeState;

    // The moment node is observed in DECOMMISSIONING state.
    private final long decommissioningStartTime;

    private long lastContainerFinishTime;

    // number of running containers at the moment.
    private int numActiveContainers;

    // All applications run on the node.
    private List<ApplicationId> appIds;

    // First moment the node is observed in DECOMMISSIONED state.
    private long decommissionedTime;

    // Timeout in millis for this decommissioning node.
    // This value could be dynamically updated with new value from RMNode.
    private long timeoutMs;

    private long lastUpdateTime;

    public DecommissioningNodeContext(NodeId nodeId, int timeoutSec) {
      this.nodeId = nodeId;
      this.appIds = new ArrayList<>();
      this.decommissioningStartTime = mclock.getTime();
      this.timeoutMs = 1000L * timeoutSec;
    }

    void updateTimeout(int timeoutSec) {
      this.timeoutMs = 1000L * timeoutSec;
    }
  }

  // All DECOMMISSIONING nodes to track.
  private HashMap<NodeId, DecommissioningNodeContext> decomNodes =
      new HashMap<NodeId, DecommissioningNodeContext>();

  private Timer pollTimer;
  private MonotonicClock mclock;

  public DecommissioningNodesWatcher(RMContext rmContext) {
    this.rmContext = rmContext;
    pollTimer = new Timer(true);
    mclock = new MonotonicClock();
  }

  public void init(Configuration conf) {
    int v = conf.getInt(
        YarnConfiguration.RM_DECOMMISSIONING_NODES_WATCHER_POLL_INTERVAL,
        YarnConfiguration
          .DEFAULT_RM_DECOMMISSIONING_NODES_WATCHER_POLL_INTERVAL);
    pollTimer.schedule(new PollTimerTask(rmContext), 0, (1000L * v));
  }

  /**
   * Update rmNode decommissioning status based on NodeStatus.
   * @param rmNode The node
   * @param remoteNodeStatus latest NodeStatus
   */
  public synchronized void update(RMNode rmNode, NodeStatus remoteNodeStatus) {
    DecommissioningNodeContext context = decomNodes.get(rmNode.getNodeID());
    long now = mclock.getTime();
    if (rmNode.getState() == NodeState.DECOMMISSIONED) {
      if (context == null) {
        return;
      }
      context.nodeState = rmNode.getState();
      // keep DECOMMISSIONED node for a while for status log, so that such
      // host will appear as DECOMMISSIONED instead of quietly disappears.
      if (context.decommissionedTime == 0) {
        context.decommissionedTime = now;
      } else if (now - context.decommissionedTime > 60000L) {
        decomNodes.remove(rmNode.getNodeID());
      }
    } else if (rmNode.getState() == NodeState.DECOMMISSIONING) {
      if (context == null) {
        context = new DecommissioningNodeContext(rmNode.getNodeID(),
            rmNode.getDecommissioningTimeout());
        decomNodes.put(rmNode.getNodeID(), context);
        context.nodeState = rmNode.getState();
        context.decommissionedTime = 0;
      }
      context.updateTimeout(rmNode.getDecommissioningTimeout());
      context.lastUpdateTime = now;

      context.appIds = rmNode.getRunningApps();

      // Count number of active containers.
      int numActiveContainers = 0;
      for (ContainerStatus cs : remoteNodeStatus.getContainersStatuses()) {
        ContainerState newState = cs.getState();
        if (newState == ContainerState.RUNNING ||
            newState == ContainerState.NEW) {
          numActiveContainers++;
        }
      }
      context.numActiveContainers = numActiveContainers;

      // maintain lastContainerFinishTime.
      if (context.numActiveContainers == 0 &&
          context.lastContainerFinishTime == 0) {
        context.lastContainerFinishTime = now;
      }
    } else {
      // remove node in other states
      if (context != null) {
        decomNodes.remove(rmNode.getNodeID());
      }
    }
  }

  public synchronized void remove(NodeId nodeId) {
    DecommissioningNodeContext context = decomNodes.get(nodeId);
    if (context != null) {
      LOG.info("remove " + nodeId + " in " + context.nodeState);
      decomNodes.remove(nodeId);
    }
  }

  public void stop() {
    pollTimer.cancel();
    pollTimer = null;
  }

  /**
   * Status about a specific decommissioning node.
   *
   */
  public enum DecommissioningNodeStatus {
    // Node is not in DECOMMISSIONING state.
    NONE,

    // wait for running containers to complete
    WAIT_CONTAINER,

    // wait for running application to complete (after all containers complete);
    WAIT_APP,

    // Timeout waiting for either containers or applications to complete.
    TIMEOUT,

    // nothing to wait, ready to be decommissioned
    READY,

    // The node has already been decommissioned
    DECOMMISSIONED,
  }

  public boolean checkReadyToBeDecommissioned(NodeId nodeId) {
    DecommissioningNodeStatus s = checkDecommissioningStatus(nodeId);
    return (s == DecommissioningNodeStatus.READY ||
            s == DecommissioningNodeStatus.TIMEOUT);
  }

  public DecommissioningNodeStatus checkDecommissioningStatus(NodeId nodeId) {
    DecommissioningNodeContext context = decomNodes.get(nodeId);
    if (context == null) {
      return DecommissioningNodeStatus.NONE;
    }

    if (context.nodeState == NodeState.DECOMMISSIONED) {
      return DecommissioningNodeStatus.DECOMMISSIONED;
    }

    long waitTime = mclock.getTime() - context.decommissioningStartTime;
    if (context.numActiveContainers > 0) {
      return (context.timeoutMs < 0 || waitTime < context.timeoutMs)?
          DecommissioningNodeStatus.WAIT_CONTAINER :
          DecommissioningNodeStatus.TIMEOUT;
    }

    if (context.appIds.size() == 0) {
      return DecommissioningNodeStatus.READY;
    } else {
      return (context.timeoutMs < 0 || waitTime < context.timeoutMs)?
          DecommissioningNodeStatus.WAIT_APP :
          DecommissioningNodeStatus.TIMEOUT;
    }
  }

  /**
   * PollTimerTask periodically:
   *   1. log status of all DECOMMISSIONING nodes;
   *   2. identify and taken care of stale DECOMMISSIONING nodes
   *      (for example, node already terminated).
   */
  class PollTimerTask extends TimerTask {
    private final RMContext rmContext;

    public PollTimerTask(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    public void run() {
      logDecommissioningNodesStatus();
      long now = mclock.getTime();
      Set<NodeId> staleNodes = new HashSet<NodeId>();

      for (Iterator<Map.Entry<NodeId, DecommissioningNodeContext>> it =
          decomNodes.entrySet().iterator(); it.hasNext();) {
        Map.Entry<NodeId, DecommissioningNodeContext> e = it.next();
        DecommissioningNodeContext d = e.getValue();
        // Skip node recently updated (NM usually updates every second).
        if (now - d.lastUpdateTime < 5000L) {
          continue;
        }
        // Remove stale non-DECOMMISSIONING node
        if (d.nodeState != NodeState.DECOMMISSIONING) {
          LOG.debug("remove {} {}", d.nodeState, d.nodeId);
          it.remove();
          continue;
        } else if (now - d.lastUpdateTime > 60000L) {
          // Node DECOMMISSIONED could become stale, remove as necessary.
          RMNode rmNode = getRmNode(d.nodeId);
          if (rmNode != null &&
              rmNode.getState() == NodeState.DECOMMISSIONED) {
            LOG.debug("remove {} {}", rmNode.getState(), d.nodeId);
            it.remove();
            continue;
          }
        }
        if (d.timeoutMs >= 0 &&
            d.decommissioningStartTime + d.timeoutMs < now) {
          staleNodes.add(d.nodeId);
          LOG.debug("Identified stale and timeout node {}", d.nodeId);
        }
      }

      for (NodeId nodeId : staleNodes) {
        RMNode rmNode = this.rmContext.getRMNodes().get(nodeId);
        if (rmNode == null || rmNode.getState() != NodeState.DECOMMISSIONING) {
          remove(nodeId);
          continue;
        }
        if (rmNode.getState() == NodeState.DECOMMISSIONING &&
            checkReadyToBeDecommissioned(rmNode.getNodeID())) {
          LOG.info("DECOMMISSIONING " + nodeId + " timeout");
          this.rmContext.getDispatcher().getEventHandler().handle(
              new RMNodeEvent(nodeId, RMNodeEventType.DECOMMISSION));
        }
      }
    }
  }

  private RMNode getRmNode(NodeId nodeId) {
    RMNode rmNode = this.rmContext.getRMNodes().get(nodeId);
    if (rmNode == null) {
      rmNode = this.rmContext.getInactiveRMNodes().get(nodeId);
    }
    return rmNode;
  }

  // Time in second to be decommissioned.
  private int getTimeoutInSec(DecommissioningNodeContext context) {
    if (context.nodeState == NodeState.DECOMMISSIONED) {
      return 0;
    } else if (context.nodeState != NodeState.DECOMMISSIONING) {
      return -1;
    }
    if (context.appIds.size() == 0 && context.numActiveContainers == 0) {
      return 0;
    }
    // negative timeout value means no timeout (infinite timeout).
    if (context.timeoutMs < 0) {
      return -1;
    }

    long now = mclock.getTime();
    long timeout = context.decommissioningStartTime + context.timeoutMs - now;
    return Math.max(0, (int)(timeout / 1000));
  }

  private void logDecommissioningNodesStatus() {
    if (!LOG.isDebugEnabled() || decomNodes.size() == 0) {
      return;
    }
    long now = mclock.getTime();
    for (DecommissioningNodeContext d : decomNodes.values()) {
      StringBuilder sb = new StringBuilder();
      DecommissioningNodeStatus s = checkDecommissioningStatus(d.nodeId);
      sb.append(String.format(
          "%n  %-34s %4ds fresh:%3ds containers:%2d %14s",
          d.nodeId.getHost(),
          (now - d.decommissioningStartTime) / 1000,
          (now - d.lastUpdateTime) / 1000,
          d.numActiveContainers,
          s));
      if (s == DecommissioningNodeStatus.WAIT_APP ||
          s == DecommissioningNodeStatus.WAIT_CONTAINER) {
        sb.append(String.format(" timeout:%4ds", getTimeoutInSec(d)));
      }
      for (ApplicationId aid : d.appIds) {
        sb.append("\n    " + aid);
        RMApp rmApp = rmContext.getRMApps().get(aid);
        if (rmApp != null) {
          sb.append(String.format(
              " %s %9s %5.2f%% %5ds",
              rmApp.getState(),
              (rmApp.getApplicationType() == null)?
                  "" : rmApp.getApplicationType(),
              100.0 * rmApp.getProgress(),
              (System.currentTimeMillis() - rmApp.getStartTime()) / 1000));
        }
      }
      LOG.debug("Decommissioning node: " + sb.toString());
    }
  }
}
