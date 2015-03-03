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

package org.apache.hadoop.yarn.server.resourcemanager.nodelabels;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.nodelabels.NodeLabel;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeLabelsUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.collect.ImmutableSet;

public class RMNodeLabelsManager extends CommonNodeLabelsManager {
  
  protected static class Queue {
    protected Set<String> acccessibleNodeLabels;
    protected Resource resource;

    protected Queue() {
      acccessibleNodeLabels =
          Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
      resource = Resource.newInstance(0, 0);
    }
  }

  ConcurrentMap<String, Queue> queueCollections =
      new ConcurrentHashMap<String, Queue>();
  private YarnAuthorizationProvider authorizer;
  private RMContext rmContext = null;
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    authorizer = YarnAuthorizationProvider.getInstance(conf);
  }

  @Override
  public void addLabelsToNode(Map<NodeId, Set<String>> addedLabelsToNode)
      throws IOException {    
    try {
      writeLock.lock();

      // get nodesCollection before edition
      Map<String, Host> before = cloneNodeMap(addedLabelsToNode.keySet());

      super.addLabelsToNode(addedLabelsToNode);

      // get nodesCollection after edition
      Map<String, Host> after = cloneNodeMap(addedLabelsToNode.keySet());

      // update running nodes resources
      updateResourceMappings(before, after);
    } finally {
      writeLock.unlock();
    }
  }

  protected void checkRemoveFromClusterNodeLabelsOfQueue(
      Collection<String> labelsToRemove) throws IOException {
    // Check if label to remove doesn't existed or null/empty, will throw
    // exception if any of labels to remove doesn't meet requirement
    for (String label : labelsToRemove) {
      label = normalizeLabel(label);

      // check if any queue contains this label
      for (Entry<String, Queue> entry : queueCollections.entrySet()) {
        String queueName = entry.getKey();
        Set<String> queueLabels = entry.getValue().acccessibleNodeLabels;
        if (queueLabels.contains(label)) {
          throw new IOException("Cannot remove label=" + label
              + ", because queue=" + queueName + " is using this label. "
              + "Please remove label on queue before remove the label");
        }
      }
    }
  }

  @Override
  public void removeFromClusterNodeLabels(Collection<String> labelsToRemove)
      throws IOException {
    try {
      writeLock.lock();

      checkRemoveFromClusterNodeLabelsOfQueue(labelsToRemove);

      // copy before NMs
      Map<String, Host> before = cloneNodeMap();

      super.removeFromClusterNodeLabels(labelsToRemove);

      updateResourceMappings(before, nodeCollections);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void
      removeLabelsFromNode(Map<NodeId, Set<String>> removeLabelsFromNode)
          throws IOException {
    try {
      writeLock.lock();

      // get nodesCollection before edition
      Map<String, Host> before =
          cloneNodeMap(removeLabelsFromNode.keySet());

      super.removeLabelsFromNode(removeLabelsFromNode);

      // get nodesCollection before edition
      Map<String, Host> after = cloneNodeMap(removeLabelsFromNode.keySet());

      // update running nodes resources
      updateResourceMappings(before, after);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void replaceLabelsOnNode(Map<NodeId, Set<String>> replaceLabelsToNode)
      throws IOException {
    try {
      writeLock.lock();
      
      // get nodesCollection before edition
      Map<String, Host> before = cloneNodeMap(replaceLabelsToNode.keySet());

      super.replaceLabelsOnNode(replaceLabelsToNode);

      // get nodesCollection after edition
      Map<String, Host> after = cloneNodeMap(replaceLabelsToNode.keySet());

      // update running nodes resources
      updateResourceMappings(before, after);
    } finally {
      writeLock.unlock();
    }
  }
  

  /*
   * Following methods are used for setting if a node is up and running, and it
   * will update running nodes resource
   */
  public void activateNode(NodeId nodeId, Resource resource) {
    try {
      writeLock.lock();
      
      // save if we have a node before
      Map<String, Host> before = cloneNodeMap(ImmutableSet.of(nodeId));
      
      createHostIfNonExisted(nodeId.getHost());
      try {
        createNodeIfNonExisted(nodeId);
      } catch (IOException e) {
        LOG.error("This shouldn't happen, cannot get host in nodeCollection"
            + " associated to the node being activated");
        return;
      }

      Node nm = getNMInNodeSet(nodeId);
      nm.resource = resource;
      nm.running = true;

      // Add node in labelsCollection
      Set<String> labelsForNode = getLabelsByNode(nodeId);
      if (labelsForNode != null) {
        for (String label : labelsForNode) {
          NodeLabel labelInfo = labelCollections.get(label);
          if(labelInfo != null) {
            labelInfo.addNodeId(nodeId);
          }
        }
      }
      
      // get the node after edition
      Map<String, Host> after = cloneNodeMap(ImmutableSet.of(nodeId));
      
      updateResourceMappings(before, after);
    } finally {
      writeLock.unlock();
    }
  }
  
  /*
   * Following methods are used for setting if a node unregistered to RM
   */
  public void deactivateNode(NodeId nodeId) {
    try {
      writeLock.lock();
      
      // save if we have a node before
      Map<String, Host> before = cloneNodeMap(ImmutableSet.of(nodeId));
      Node nm = getNMInNodeSet(nodeId);
      if (null != nm) {
        if (null == nm.labels) {
          // When node deactivated, remove the nm from node collection if no
          // labels explicitly set for this particular nm

          // Save labels first, we need to remove label->nodes relation later
          Set<String> savedNodeLabels = getLabelsOnNode(nodeId);
          
          // Remove this node in nodes collection
          nodeCollections.get(nodeId.getHost()).nms.remove(nodeId);
          
          // Remove this node in labels->node
          removeNodeFromLabels(nodeId, savedNodeLabels);
        } else {
          // set nm is not running, and its resource = 0
          nm.running = false;
          nm.resource = Resource.newInstance(0, 0);
        }
      }
      
      // get the node after edition
      Map<String, Host> after = cloneNodeMap(ImmutableSet.of(nodeId));
      
      updateResourceMappings(before, after);
    } finally {
      writeLock.unlock();
    }
  }

  public void updateNodeResource(NodeId node, Resource newResource) throws IOException {
    deactivateNode(node);
    activateNode(node, newResource);
  }

  public void reinitializeQueueLabels(Map<String, Set<String>> queueToLabels) {
    try {
      writeLock.lock();
      // clear before set
      this.queueCollections.clear();

      for (Entry<String, Set<String>> entry : queueToLabels.entrySet()) {
        String queue = entry.getKey();
        Queue q = new Queue();
        this.queueCollections.put(queue, q);

        Set<String> labels = entry.getValue();
        if (labels.contains(ANY)) {
          continue;
        }

        q.acccessibleNodeLabels.addAll(labels);
        for (Host host : nodeCollections.values()) {
          for (Entry<NodeId, Node> nentry : host.nms.entrySet()) {
            NodeId nodeId = nentry.getKey();
            Node nm = nentry.getValue();
            if (nm.running && isNodeUsableByQueue(getLabelsByNode(nodeId), q)) {
              Resources.addTo(q.resource, nm.resource);
            }
          }
        }
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  public Resource getQueueResource(String queueName, Set<String> queueLabels,
      Resource clusterResource) {
    try {
      readLock.lock();
      if (queueLabels.contains(ANY)) {
        return clusterResource;
      }
      Queue q = queueCollections.get(queueName);
      if (null == q) {
        return Resources.none();
      }
      return q.resource;
    } finally {
      readLock.unlock();
    }
  }
  
  public Set<String> getLabelsOnNode(NodeId nodeId) {
    try {
      readLock.lock();
      Set<String> nodeLabels = getLabelsByNode(nodeId);
      return Collections.unmodifiableSet(nodeLabels);
    } finally {
      readLock.unlock();
    }
  }
  
  public boolean containsNodeLabel(String label) {
    try {
      readLock.lock();
      return label != null
          && (label.isEmpty() || labelCollections.containsKey(label));
    } finally {
      readLock.unlock();
    }
  }

  private Map<String, Host> cloneNodeMap(Set<NodeId> nodesToCopy) {
    Map<String, Host> map = new HashMap<String, Host>();
    for (NodeId nodeId : nodesToCopy) {
      if (!map.containsKey(nodeId.getHost())) {
        Host originalN = nodeCollections.get(nodeId.getHost());
        if (null == originalN) {
          continue;
        }
        Host n = originalN.copy();
        n.nms.clear();
        map.put(nodeId.getHost(), n);
      }

      Host n = map.get(nodeId.getHost());
      if (WILDCARD_PORT == nodeId.getPort()) {
        for (Entry<NodeId, Node> entry : nodeCollections
            .get(nodeId.getHost()).nms.entrySet()) {
          n.nms.put(entry.getKey(), entry.getValue().copy());
        }
      } else {
        Node nm = getNMInNodeSet(nodeId);
        if (null != nm) {
          n.nms.put(nodeId, nm.copy());
        }
      }
    }
    return map;
  }

  @SuppressWarnings("unchecked")
  private void updateResourceMappings(Map<String, Host> before,
      Map<String, Host> after) {
    // Get NMs in before only
    Set<NodeId> allNMs = new HashSet<NodeId>();
    for (Entry<String, Host> entry : before.entrySet()) {
      allNMs.addAll(entry.getValue().nms.keySet());
    }
    for (Entry<String, Host> entry : after.entrySet()) {
      allNMs.addAll(entry.getValue().nms.keySet());
    }
    
    // Map used to notify RM
    Map<NodeId, Set<String>> newNodeToLabelsMap =
        new HashMap<NodeId, Set<String>>();

    // traverse all nms
    for (NodeId nodeId : allNMs) {
      Node oldNM;
      if ((oldNM = getNMInNodeSet(nodeId, before, true)) != null) {
        Set<String> oldLabels = getLabelsByNode(nodeId, before);
        // no label in the past
        if (oldLabels.isEmpty()) {
          // update labels
          NodeLabel label = labelCollections.get(NO_LABEL);
          label.removeNode(oldNM.resource);

          // update queues, all queue can access this node
          for (Queue q : queueCollections.values()) {
            Resources.subtractFrom(q.resource, oldNM.resource);
          }
        } else {
          // update labels
          for (String labelName : oldLabels) {
            NodeLabel label = labelCollections.get(labelName);
            if (null == label) {
              continue;
            }
            label.removeNode(oldNM.resource);
          }

          // update queues, only queue can access this node will be subtract
          for (Queue q : queueCollections.values()) {
            if (isNodeUsableByQueue(oldLabels, q)) {
              Resources.subtractFrom(q.resource, oldNM.resource);
            }
          }
        }
      }

      Node newNM;
      if ((newNM = getNMInNodeSet(nodeId, after, true)) != null) {
        Set<String> newLabels = getLabelsByNode(nodeId, after);
        
        newNodeToLabelsMap.put(nodeId, ImmutableSet.copyOf(newLabels));
        
        // no label in the past
        if (newLabels.isEmpty()) {
          // update labels
          NodeLabel label = labelCollections.get(NO_LABEL);
          label.addNode(newNM.resource);

          // update queues, all queue can access this node
          for (Queue q : queueCollections.values()) {
            Resources.addTo(q.resource, newNM.resource);
          }
        } else {
          // update labels
          for (String labelName : newLabels) {
            NodeLabel label = labelCollections.get(labelName);
            label.addNode(newNM.resource);
          }

          // update queues, only queue can access this node will be subtract
          for (Queue q : queueCollections.values()) {
            if (isNodeUsableByQueue(newLabels, q)) {
              Resources.addTo(q.resource, newNM.resource);
            }
          }
        }
      }
    }
    
    // Notify RM
    if (rmContext != null && rmContext.getDispatcher() != null) {
      rmContext.getDispatcher().getEventHandler().handle(
          new NodeLabelsUpdateSchedulerEvent(newNodeToLabelsMap));
    }
  }
  
  public Resource getResourceByLabel(String label, Resource clusterResource) {
    label = normalizeLabel(label);
    try {
      readLock.lock();
      if (null == labelCollections.get(label)) {
        return Resources.none();
      }
      return labelCollections.get(label).getResource();
    } finally {
      readLock.unlock();
    }
  }

  private boolean isNodeUsableByQueue(Set<String> nodeLabels, Queue q) {
    // node without any labels can be accessed by any queue
    if (nodeLabels == null || nodeLabels.isEmpty()
        || (nodeLabels.size() == 1 && nodeLabels.contains(NO_LABEL))) {
      return true;
    }

    for (String label : nodeLabels) {
      if (q.acccessibleNodeLabels.contains(label)) {
        return true;
      }
    }

    return false;
  }

  private Map<String, Host> cloneNodeMap() {
    Set<NodeId> nodesToCopy = new HashSet<NodeId>();
    for (String nodeName : nodeCollections.keySet()) {
      nodesToCopy.add(NodeId.newInstance(nodeName, WILDCARD_PORT));
    }
    return cloneNodeMap(nodesToCopy);
  }

  public boolean checkAccess(UserGroupInformation user) {
    // make sure only admin can invoke
    // this method
    if (authorizer.isAdmin(user)) {
      return true;
    }
    return false;
  }
  
  public void setRMContext(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  public List<NodeLabel> pullRMNodeLabelsInfo() {
    try {
      readLock.lock();
      List<NodeLabel> infos = new ArrayList<NodeLabel>();

      for (Entry<String, NodeLabel> entry : labelCollections.entrySet()) {
        NodeLabel label = entry.getValue();
        infos.add(label.getCopy());
      }

      Collections.sort(infos);
      return infos;
    } finally {
      readLock.unlock();
    }
  }
}
