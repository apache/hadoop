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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeKey;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.nodelabels.AttributeValue;
import org.apache.hadoop.yarn.nodelabels.NodeAttributeStore;
import org.apache.hadoop.yarn.nodelabels.NodeAttributesManager;
import org.apache.hadoop.yarn.nodelabels.NodeLabelUtil;
import org.apache.hadoop.yarn.nodelabels.RMNodeAttribute;
import org.apache.hadoop.yarn.nodelabels.StringAttributeValue;
import org.apache.hadoop.yarn.server.api.protocolrecords.AttributeMappingOperationType;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeToAttributes;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAttributesUpdateSchedulerEvent;

import org.apache.hadoop.thirdparty.com.google.common.base.Strings;

/**
 * Manager holding the attributes to Labels.
 */
public class NodeAttributesManagerImpl extends NodeAttributesManager {
  protected static final Logger LOG =
      LoggerFactory.getLogger(NodeAttributesManagerImpl.class);
  /**
   * If a user doesn't specify value for a label, then empty string is
   * considered as default.
   */
  public static final String EMPTY_ATTRIBUTE_VALUE = "";

  Dispatcher dispatcher;
  NodeAttributeStore store;

  // TODO may be we can have a better collection here.
  // this will be updated to get the attributeName to NM mapping
  private ConcurrentHashMap<NodeAttributeKey, RMNodeAttribute> clusterAttributes
      = new ConcurrentHashMap<>();

  // hostname -> (Map (attributeName -> NodeAttribute))
  // Instead of NodeAttribute, plan to have it in future as AttributeValue
  // AttributeValue
  // / \
  // StringNodeAttributeValue LongAttributeValue
  // and convert the configured value to the specific type so that the
  // expression evaluations are faster
  private ConcurrentMap<String, Host> nodeCollections =
      new ConcurrentHashMap<>();

  private final ReadLock readLock;
  private final WriteLock writeLock;
  private RMContext rmContext = null;

  public NodeAttributesManagerImpl() {
    super("NodeAttributesManagerImpl");
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  protected void initDispatcher(Configuration conf) {
    // create async handler
    dispatcher = new AsyncDispatcher("AttributeNodeLabelsManager dispatcher");
    AsyncDispatcher asyncDispatcher = (AsyncDispatcher) dispatcher;
    asyncDispatcher.init(conf);
    asyncDispatcher.setDrainEventsOnStop();
  }

  protected void startDispatcher() {
    // start dispatcher
    AsyncDispatcher asyncDispatcher = (AsyncDispatcher) dispatcher;
    asyncDispatcher.start();
  }

  @Override
  protected void serviceStart() throws Exception {
    initNodeAttributeStore(getConfig());
    // init dispatcher only when service start, because recover will happen in
    // service init, we don't want to trigger any event handling at that time.
    initDispatcher(getConfig());

    if (null != dispatcher) {
      dispatcher.register(NodeAttributesStoreEventType.class,
          new ForwardingEventHandler());
    }

    startDispatcher();
    super.serviceStart();
  }

  protected void initNodeAttributeStore(Configuration conf) throws Exception {
    this.store = getAttributeStoreClass(conf);
    this.store.init(conf, this);
    this.store.recover();
  }

  private NodeAttributeStore getAttributeStoreClass(Configuration conf) {
    try {
      return ReflectionUtils.newInstance(
          conf.getClass(YarnConfiguration.FS_NODE_ATTRIBUTE_STORE_IMPL_CLASS,
              FileSystemNodeAttributeStore.class, NodeAttributeStore.class),
          conf);
    } catch (Exception e) {
      throw new YarnRuntimeException(
          "Could not instantiate Node Attribute Store ", e);
    }
  }

  @VisibleForTesting
  protected void internalUpdateAttributesOnNodes(
      Map<String, Map<NodeAttribute, AttributeValue>> nodeAttributeMapping,
      AttributeMappingOperationType op,
      Map<NodeAttributeKey, RMNodeAttribute> newAttributesToBeAdded,
      String attributePrefix) {
    writeLock.lock();
    try {
      // shows node->attributes Mapped as part of this operation.
      StringBuilder logMsg = new StringBuilder(op.name());
      logMsg.append(" attributes on nodes:");
      // do update labels from nodes
      for (Entry<String, Map<NodeAttribute, AttributeValue>> entry :
          nodeAttributeMapping.entrySet()) {
        String nodeHost = entry.getKey();
        Map<NodeAttribute, AttributeValue> attributes = entry.getValue();

        Host node = nodeCollections.get(nodeHost);
        if (node == null) {
          node = new Host(nodeHost);
          nodeCollections.put(nodeHost, node);
        }
        switch (op) {
        case REMOVE:
          removeNodeFromAttributes(nodeHost, attributes.keySet());
          node.removeAttributes(attributes);
          break;
        case ADD:
          clusterAttributes.putAll(newAttributesToBeAdded);
          addNodeToAttribute(nodeHost, attributes);
          node.addAttributes(attributes);
          break;
        case REPLACE:
          clusterAttributes.putAll(newAttributesToBeAdded);
          replaceNodeToAttribute(nodeHost, attributePrefix,
              node.getAttributes(), attributes);
          node.replaceAttributes(attributes, attributePrefix);
          break;
        default:
          break;
        }
        logMsg.append(" NM = ")
            .append(entry.getKey())
            .append(", attributes=[ ")
            .append(StringUtils.join(entry.getValue().keySet(), ","))
            .append("] ,");
      }
      LOG.debug("{}", logMsg);

      if (null != dispatcher && NodeAttribute.PREFIX_CENTRALIZED
          .equals(attributePrefix)) {
        dispatcher.getEventHandler()
            .handle(new NodeAttributesStoreEvent(nodeAttributeMapping, op));
      }

      // Map used to notify RM
      Map<String, Set<NodeAttribute>> newNodeToAttributesMap =
          new HashMap<String, Set<NodeAttribute>>();
      nodeAttributeMapping.forEach((k, v) -> {
        Host node = nodeCollections.get(k);
        newNodeToAttributesMap.put(k, node.attributes.keySet());
      });

      // Notify RM
      if (rmContext != null && rmContext.getDispatcher() != null) {
        LOG.info("Updated NodeAttribute event to RM:"
            + newNodeToAttributesMap);
        rmContext.getDispatcher().getEventHandler().handle(
            new NodeAttributesUpdateSchedulerEvent(newNodeToAttributesMap));
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void removeNodeFromAttributes(String nodeHost,
      Set<NodeAttribute> attributeMappings) {
    for (NodeAttribute rmAttribute : attributeMappings) {
      RMNodeAttribute host =
          clusterAttributes.get(rmAttribute.getAttributeKey());
      if (host != null) {
        host.removeNode(nodeHost);
        // If there is no other host has such attribute,
        // remove it from the global mapping.
        if (host.getAssociatedNodeIds().isEmpty()) {
          clusterAttributes.remove(rmAttribute.getAttributeKey());
        }
      }
    }
  }

  private void addNodeToAttribute(String nodeHost,
      Map<NodeAttribute, AttributeValue> attributeMappings) {
    for (Entry<NodeAttribute, AttributeValue> attributeEntry : attributeMappings
        .entrySet()) {

      RMNodeAttribute rmNodeAttribute =
          clusterAttributes.get(attributeEntry.getKey().getAttributeKey());
      if (rmNodeAttribute != null) {
        rmNodeAttribute.addNode(nodeHost, attributeEntry.getValue());
      } else {
        clusterAttributes.put(attributeEntry.getKey().getAttributeKey(),
            new RMNodeAttribute(attributeEntry.getKey()));
      }
    }
  }

  private void replaceNodeToAttribute(String nodeHost, String prefix,
      Map<NodeAttribute, AttributeValue> oldAttributeMappings,
      Map<NodeAttribute, AttributeValue> newAttributeMappings) {
    if (oldAttributeMappings != null) {
      Set<NodeAttribute> toRemoveAttributes =
          NodeLabelUtil.filterAttributesByPrefix(
              oldAttributeMappings.keySet(), prefix);
      removeNodeFromAttributes(nodeHost, toRemoveAttributes);
    }
    addNodeToAttribute(nodeHost, newAttributeMappings);
  }

  /**
   * @param nodeAttributeMapping
   * @param newAttributesToBeAdded
   * @param isRemoveOperation : to indicate whether its a remove operation.
   * @return Map of String to Map of NodeAttribute to AttributeValue
   * @throws IOException : on invalid mapping in the current request or against
   *           already existing NodeAttributes.
   */
  protected Map<String, Map<NodeAttribute, AttributeValue>> validate(
      Map<String, Set<NodeAttribute>> nodeAttributeMapping,
      Map<NodeAttributeKey, RMNodeAttribute> newAttributesToBeAdded,
      boolean isRemoveOperation) throws IOException {
    Map<String, Map<NodeAttribute, AttributeValue>> nodeToAttributesMap =
        new TreeMap<>();
    Map<NodeAttribute, AttributeValue> attributesValues;
    Set<Entry<String, Set<NodeAttribute>>> entrySet =
        nodeAttributeMapping.entrySet();
    for (Entry<String, Set<NodeAttribute>> nodeToAttrMappingEntry : entrySet) {
      attributesValues = new HashMap<>();
      String node = nodeToAttrMappingEntry.getKey().trim();
      if (nodeToAttrMappingEntry.getValue().isEmpty()) {
        // no attributes to map mostly remove operation
        continue;
      }

      // validate for attributes
      for (NodeAttribute attribute : nodeToAttrMappingEntry.getValue()) {
        NodeAttributeKey attributeKey = attribute.getAttributeKey();
        String attributeName = attributeKey.getAttributeName().trim();
        NodeLabelUtil.checkAndThrowAttributeName(attributeName);
        NodeLabelUtil
            .checkAndThrowAttributePrefix(attributeKey.getAttributePrefix());
        NodeLabelUtil
            .checkAndThrowAttributeValue(attribute.getAttributeValue());

        // ensure trimmed values are set back
        attributeKey.setAttributeName(attributeName);
        attributeKey
            .setAttributePrefix(attributeKey.getAttributePrefix().trim());

        // verify for type against prefix/attributeName
        if (validateForAttributeTypeMismatch(isRemoveOperation, attribute,
            newAttributesToBeAdded)) {
          newAttributesToBeAdded.put(attribute.getAttributeKey(),
              new RMNodeAttribute(attribute));
        }
        // TODO type based value setting needs to be done using a factory
        StringAttributeValue value = new StringAttributeValue();
        value.validateAndInitializeValue(
            normalizeAttributeValue(attribute.getAttributeValue()));
        attributesValues.put(attribute, value);
      }
      nodeToAttributesMap.put(node, attributesValues);
    }
    return nodeToAttributesMap;
  }

  /**
   *
   * @param isRemoveOperation
   * @param attribute
   * @param newAttributes
   * @return Whether its a new Attribute added
   * @throws IOException
   */
  private boolean validateForAttributeTypeMismatch(boolean isRemoveOperation,
      NodeAttribute attribute,
      Map<NodeAttributeKey, RMNodeAttribute> newAttributes)
      throws IOException {
    NodeAttributeKey attributeKey = attribute.getAttributeKey();
    if (isRemoveOperation
        && !clusterAttributes.containsKey(attributeKey)) {
      // no need to validate anything as its remove operation and attribute
      // doesn't exist.
      return false; // no need to add as its remove operation
    } else {
      // already existing or attribute is mapped to another Node in the
      // current command, then check whether the attribute type is matching
      NodeAttribute existingAttribute =
          (clusterAttributes.containsKey(attributeKey)
              ? clusterAttributes.get(attributeKey).getAttribute()
              : (newAttributes.containsKey(attributeKey)
                  ? newAttributes.get(attributeKey).getAttribute()
                  : null));
      if (existingAttribute == null) {
        return true;
      } else if (existingAttribute.getAttributeType() != attribute
          .getAttributeType()) {
        throw new IOException("Attribute name - type is not matching with "
            + "already configured mapping for the attribute "
            + attributeKey + " existing : "
            + existingAttribute.getAttributeType() + ", new :"
            + attribute.getAttributeType());
      }
      return false;
    }
  }

  protected String normalizeAttributeValue(String value) {
    if (value != null) {
      return value.trim();
    }
    return EMPTY_ATTRIBUTE_VALUE;
  }

  @Override
  public Set<NodeAttribute> getClusterNodeAttributes(
      Set<String> prefix) {
    Set<NodeAttribute> attributes = new HashSet<>();
    Set<Entry<NodeAttributeKey, RMNodeAttribute>> allAttributes =
        clusterAttributes.entrySet();
    // Return all if prefix is not given.
    boolean forAllPrefix = prefix == null || prefix.isEmpty();
    // Try search attributes by prefix and return valid ones.
    Iterator<Entry<NodeAttributeKey, RMNodeAttribute>> iterator =
        allAttributes.iterator();
    while (iterator.hasNext()) {
      Entry<NodeAttributeKey, RMNodeAttribute> current = iterator.next();
      NodeAttributeKey attrID = current.getKey();
      RMNodeAttribute rmAttr = current.getValue();
      if (forAllPrefix || prefix.contains(attrID.getAttributePrefix())) {
        attributes.add(rmAttr.getAttribute());
      }
    }
    return attributes;
  }

  @Override
  public Map<NodeAttributeKey,
      Map<String, AttributeValue>> getAttributesToNodes(
      Set<NodeAttributeKey> attributes) {
    readLock.lock();
    try {
      boolean fetchAllAttributes = (attributes == null || attributes.isEmpty());
      Map<NodeAttributeKey, Map<String, AttributeValue>> attributesToNodes =
          new HashMap<>();
      for (Entry<NodeAttributeKey, RMNodeAttribute> attributeEntry :
          clusterAttributes.entrySet()) {
        if (fetchAllAttributes
            || attributes.contains(attributeEntry.getKey())) {
          attributesToNodes.put(attributeEntry.getKey(),
              attributeEntry.getValue().getAssociatedNodeIds());
        }
      }
      return attributesToNodes;
    } finally {
      readLock.unlock();
    }
  }

  public Resource getResourceByAttribute(NodeAttribute attribute) {
    readLock.lock();
    try {
      return clusterAttributes.containsKey(attribute.getAttributeKey())
          ? clusterAttributes.get(attribute.getAttributeKey()).getResource()
          : Resource.newInstance(0, 0);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Map<NodeAttribute, AttributeValue> getAttributesForNode(
      String hostName) {
    readLock.lock();
    try {
      return nodeCollections.containsKey(hostName)
          ? nodeCollections.get(hostName).getAttributes()
          : new HashMap<>();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public List<NodeToAttributes> getNodeToAttributes(Set<String> prefix) {
    readLock.lock();
    try {
      List<NodeToAttributes> nodeToAttributes = new ArrayList<>();
      nodeCollections.forEach((k, v) -> {
        List<NodeAttribute> attrs;
        if (prefix == null || prefix.isEmpty()) {
          attrs = new ArrayList<>(v.getAttributes().keySet());
        } else {
          attrs = new ArrayList<>();
          for (Entry<NodeAttribute, AttributeValue> nodeAttr : v.attributes
              .entrySet()) {
            if (prefix.contains(
                nodeAttr.getKey().getAttributeKey().getAttributePrefix())) {
              attrs.add(nodeAttr.getKey());
            }
          }
        }
        nodeToAttributes.add(NodeToAttributes.newInstance(k, attrs));
      });
      return nodeToAttributes;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Map<String, Set<NodeAttribute>> getNodesToAttributes(
      Set<String> hostNames) {
    readLock.lock();
    try {
      boolean fetchAllNodes = (hostNames == null || hostNames.isEmpty());
      Map<String, Set<NodeAttribute>> nodeToAttrs = new HashMap<>();
      if (fetchAllNodes) {
        nodeCollections.forEach((key, value) -> nodeToAttrs
            .put(key, value.getAttributes().keySet()));
      } else {
        for (String hostName : hostNames) {
          Host host = nodeCollections.get(hostName);
          if (host != null) {
            nodeToAttrs.put(hostName, host.getAttributes().keySet());
          }
        }
      }
      return nodeToAttrs;
    } finally {
      readLock.unlock();
    }
  }

  public void activateNode(NodeId nodeId, Resource resource) {
    writeLock.lock();
    try {
      String hostName = nodeId.getHost();
      Host host = nodeCollections.get(hostName);
      if (host == null) {
        host = new Host(hostName);
        nodeCollections.put(hostName, host);
      }
      host.activateNode(resource);
      for (NodeAttribute attribute : host.getAttributes().keySet()) {
        clusterAttributes.get(attribute.getAttributeKey()).removeNode(resource);
      }
    } finally {
      writeLock.unlock();
    }
  }

  public void deactivateNode(NodeId nodeId) {
    writeLock.lock();
    try {
      Host host = nodeCollections.get(nodeId.getHost());
      for (NodeAttribute attribute : host.getAttributes().keySet()) {
        clusterAttributes.get(attribute.getAttributeKey())
            .removeNode(host.getResource());
      }
      host.deactivateNode();
    } finally {
      writeLock.unlock();
    }
  }

  public void updateNodeResource(NodeId node, Resource newResource) {
    deactivateNode(node);
    activateNode(node, newResource);
  }

  /**
   * A <code>Host</code> can have multiple <code>Node</code>s.
   */
  public static class Host {
    private String hostName;
    private Map<NodeAttribute, AttributeValue> attributes;
    private Resource resource;
    private boolean isActive;

    private Map<NodeAttribute, AttributeValue> getAttributes() {
      return attributes;
    }

    public void setAttributes(Map<NodeAttribute, AttributeValue> attributes) {
      this.attributes = attributes;
    }

    public void removeAttributes(
        Map<NodeAttribute, AttributeValue> attributesMapping) {
      for (NodeAttribute attribute : attributesMapping.keySet()) {
        this.attributes.remove(attribute);
      }
    }

    public void replaceAttributes(
        Map<NodeAttribute, AttributeValue> attributesMapping, String prefix) {
      if (Strings.isNullOrEmpty(prefix)) {
        this.attributes.clear();
      } else {
        Iterator<Entry<NodeAttribute, AttributeValue>> it =
            this.attributes.entrySet().iterator();
        while (it.hasNext()) {
          Entry<NodeAttribute, AttributeValue> current = it.next();
          if (prefix.equals(
              current.getKey().getAttributeKey().getAttributePrefix())) {
            it.remove();
          }
        }
      }
      this.attributes.putAll(attributesMapping);
    }

    public void addAttributes(
        Map<NodeAttribute, AttributeValue> attributesMapping) {
      this.attributes.putAll(attributesMapping);
    }

    public Resource getResource() {
      return resource;
    }

    public void setResource(Resource resourceParam) {
      this.resource = resourceParam;
    }

    public boolean isActive() {
      return isActive;
    }

    public void deactivateNode() {
      this.isActive = false;
      this.resource = Resource.newInstance(0, 0);
    }

    public void activateNode(Resource r) {
      this.isActive = true;
      this.resource = r;
    }

    public String getHostName() {
      return hostName;
    }

    public void setHostName(String hostName) {
      this.hostName = hostName;
    }

    public Host(String hostName) {
      this(hostName, new HashMap<NodeAttribute, AttributeValue>());
    }

    public Host(String hostName,
        Map<NodeAttribute, AttributeValue> attributes) {
      this(hostName, attributes, Resource.newInstance(0, 0), false);
    }

    public Host(String hostName, Map<NodeAttribute, AttributeValue> attributes,
        Resource resource, boolean isActive) {
      super();
      this.attributes = attributes;
      this.resource = resource;
      this.isActive = isActive;
      this.hostName = hostName;
    }
  }

  private final class ForwardingEventHandler
      implements EventHandler<NodeAttributesStoreEvent> {

    @Override
    public void handle(NodeAttributesStoreEvent event) {
      handleStoreEvent(event);
    }
  }

  // Dispatcher related code
  protected void handleStoreEvent(NodeAttributesStoreEvent event) {
    List<NodeToAttributes> mappingList = new ArrayList<>();
    Map<String, Map<NodeAttribute, AttributeValue>> nodeToAttr =
        event.getNodeAttributeMappingList();
    nodeToAttr.forEach((k, v) -> mappingList
        .add(NodeToAttributes.newInstance(k, new ArrayList<>(v.keySet()))));
    try {
      switch (event.getOperation()) {
      case REPLACE:
        store.replaceNodeAttributes(mappingList);
        break;
      case ADD:
        store.addNodeAttributes(mappingList);
        break;
      case REMOVE:
        store.removeNodeAttributes(mappingList);
        break;
      default:
        LOG.warn("Unsupported operation");
      }
    } catch (IOException e) {
      LOG.error("Failed to store attribute modification to storage");
      throw new YarnRuntimeException(e);
    }
  }

  @Override
  public void replaceNodeAttributes(String prefix,
      Map<String, Set<NodeAttribute>> nodeAttributeMapping) throws IOException {
    processMapping(nodeAttributeMapping,
        AttributeMappingOperationType.REPLACE, prefix);
  }

  @Override
  public void addNodeAttributes(
      Map<String, Set<NodeAttribute>> nodeAttributeMapping) throws IOException {
    processMapping(nodeAttributeMapping, AttributeMappingOperationType.ADD);
  }

  @Override
  public void removeNodeAttributes(
      Map<String, Set<NodeAttribute>> nodeAttributeMapping) throws IOException {
    processMapping(nodeAttributeMapping, AttributeMappingOperationType.REMOVE);
  }

  private void processMapping(
      Map<String, Set<NodeAttribute>> nodeAttributeMapping,
      AttributeMappingOperationType mappingType) throws IOException {
    processMapping(nodeAttributeMapping, mappingType,
        NodeAttribute.PREFIX_CENTRALIZED);
  }

  private void processMapping(
      Map<String, Set<NodeAttribute>> nodeAttributeMapping,
      AttributeMappingOperationType mappingType, String attributePrefix)
      throws IOException {
    Map<NodeAttributeKey, RMNodeAttribute> newAttributesToBeAdded =
        new HashMap<>();
    Map<String, Map<NodeAttribute, AttributeValue>> validMapping =
        validate(nodeAttributeMapping, newAttributesToBeAdded, false);
    if (validMapping.size() > 0) {
      internalUpdateAttributesOnNodes(validMapping, mappingType,
          newAttributesToBeAdded, attributePrefix);
    }
  }

  protected void stopDispatcher() {
    AsyncDispatcher asyncDispatcher = (AsyncDispatcher) dispatcher;
    if (null != asyncDispatcher) {
      asyncDispatcher.stop();
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    // finalize store
    stopDispatcher();

    // only close store when we enabled store persistent
    if (null != store) {
      store.close();
    }
  }

  public void setRMContext(RMContext context) {
    this.rmContext  = context;
  }

  /**
   * Refresh node attributes on a given node during RM recovery.
   * @param nodeId Node Id
   */
  public void refreshNodeAttributesToScheduler(NodeId nodeId) {
    String hostName = nodeId.getHost();
    Map<String, Set<NodeAttribute>> newNodeToAttributesMap =
        new HashMap<>();
    Host host = nodeCollections.get(hostName);
    if (host == null || host.attributes == null) {
      return;
    }
    newNodeToAttributesMap.put(hostName, host.attributes.keySet());

    // Notify RM
    if (rmContext != null && rmContext.getDispatcher() != null) {
      LOG.info("Updated NodeAttribute event to RM:" + newNodeToAttributesMap);
      rmContext.getDispatcher().getEventHandler().handle(
          new NodeAttributesUpdateSchedulerEvent(newNodeToAttributesMap));
    }
  }
}
