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
package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeManagerInfo;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationMasterPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeManagerInfoPBImpl;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationMasterProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationSubmissionContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeManagerInfoProto;
import org.apache.hadoop.yarn.server.resourcemanager.RMConfig;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.RMResourceTrackerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeManagerImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZKStore implements Store {
  private final Configuration conf;
  private final ZooKeeper zkClient;
  private static final Log LOG = LogFactory.getLog(ZKStore.class);
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private static final String NODES = "nodes/";
  private static final String APPS = "apps/";
  private static final String ZK_PATH_SEPARATOR = "/";
  private static final String NODE_ID = "nodeid";
  private static final String APP_MASTER = "master";
  private static final String APP_MASTER_CONTAINER = "mastercontainer";
  private final String ZK_ADDRESS;
  private final int ZK_TIMEOUT;
  private boolean doneWithRecovery = false;
  
  /** TODO make this generic **/
  private NodeIdPBImpl nodeId = new NodeIdPBImpl();

  /**
   * TODO fix this for later to handle all kinds of events 
   * of connection and session events.
   *
   */
  private class ZKWatcher implements Watcher {
    @Override
    public void process(WatchedEvent arg0) {
    }
  }

  public ZKStore(Configuration conf) throws IOException {
    this.conf = conf;
    this.ZK_ADDRESS = conf.get(RMConfig.ZK_ADDRESS);
    this.ZK_TIMEOUT = conf.getInt(RMConfig.ZK_SESSION_TIMEOUT,
        RMConfig.DEFAULT_ZK_TIMEOUT);
    zkClient = new ZooKeeper(this.ZK_ADDRESS, 
        this.ZK_TIMEOUT,
        createZKWatcher() 
    );
    this.nodeId.setId(0);
  }

  protected Watcher createZKWatcher() {
    return new ZKWatcher();   
  }

  private NodeManagerInfoPBImpl createNodeManagerInfo(NodeManager nodeInfo) {
    NodeManagerInfo node = 
      recordFactory.newRecordInstance(NodeManagerInfo.class);
    node.setNodeAddress(nodeInfo.getNodeAddress());
    node.setRackName(nodeInfo.getRackName());
    node.setCapability(nodeInfo.getTotalCapability());
    node.setUsed(nodeInfo.getUsedResource());
    node.setNumContainers(nodeInfo.getNumContainers());
    return (NodeManagerInfoPBImpl)node;
  }

  @Override
  public synchronized void storeNode(NodeManager node) throws IOException {
    /** create a storage node and store it in zk **/
    if (!doneWithRecovery) return;
    NodeManagerInfoPBImpl nodeManagerInfo = createNodeManagerInfo(node);
    byte[] bytes = nodeManagerInfo.getProto().toByteArray();
    try {
      zkClient.create(NODES + Integer.toString(node.getNodeID().getId()), bytes, null,
          CreateMode.PERSISTENT);
    } catch(InterruptedException ie) {
      LOG.info("Interrupted", ie);
      throw new InterruptedIOException("Interrupted");
    } catch(KeeperException ke) {
      LOG.info("Keeper exception", ke);
      throw convertToIOException(ke);
    }
  }

  @Override
  public synchronized void removeNode(NodeManager node) throws IOException {
    if (!doneWithRecovery) return;
    
    /** remove a storage node **/
    try {
      zkClient.delete(NODES + Integer.toString(node.getNodeID().getId()), -1);
    } catch(InterruptedException ie) {
      LOG.info("Interrupted", ie);
      throw new InterruptedIOException("Interrupted");
    } catch(KeeperException ke) {
      LOG.info("Keeper exception", ke);
      throw convertToIOException(ke);
    }

  }

  private static IOException convertToIOException(KeeperException ke) {
    IOException io = new IOException();
    io.setStackTrace(ke.getStackTrace());
    return io;
  }

  @Override
  public synchronized NodeId getNextNodeId() throws IOException {
    int num = nodeId.getId();
    num++;
    nodeId.setId(num);
    try {
      zkClient.setData(NODES + NODE_ID, nodeId.getProto().toByteArray() , -1);
    } catch(InterruptedException ie) {
      LOG.info("Interrupted", ie);
      throw new InterruptedIOException(ie.getMessage());
    } catch(KeeperException ke) {
      throw convertToIOException(ke);
    }
    return nodeId;
  }

  private String containerPathFromContainerId(ContainerId containerId) {
    String appString = ConverterUtils.toString(containerId.getAppId());
    return appString + "/" + containerId.getId();
  }

  private class ZKApplicationStore implements ApplicationStore {
    private final ApplicationId applicationId;

    public ZKApplicationStore(ApplicationId applicationId) {
      this.applicationId = applicationId;
    }

    @Override
    public void storeMasterContainer(Container container) throws IOException {
      if (!doneWithRecovery) return;
      
      ContainerPBImpl containerPBImpl = (ContainerPBImpl) container;
      try {
        zkClient.setData(APPS + ConverterUtils.toString(container.getId().getAppId()) +
            ZK_PATH_SEPARATOR + APP_MASTER_CONTAINER
            , containerPBImpl.getProto().toByteArray(), -1);
      } catch(InterruptedException ie) {
        LOG.info("Interrupted", ie);
        throw new InterruptedIOException(ie.getMessage());
      } catch(KeeperException ke) {
        LOG.info("Keeper exception", ke);
        throw convertToIOException(ke);
      }
    }
    @Override
    public synchronized void storeContainer(Container container) throws IOException {
      if (!doneWithRecovery) return;
      
      ContainerPBImpl containerPBImpl = (ContainerPBImpl) container;
      try {
        zkClient.create(APPS + containerPathFromContainerId(container.getId())
            , containerPBImpl.getProto().toByteArray(), null, CreateMode.PERSISTENT);
      } catch(InterruptedException ie) {
        LOG.info("Interrupted", ie);
        throw new InterruptedIOException(ie.getMessage());
      } catch(KeeperException ke) {
        LOG.info("Keeper exception", ke);
        throw convertToIOException(ke);
      }
    }

    @Override
    public synchronized void removeContainer(Container container) throws IOException {
      if (!doneWithRecovery) return;
      
      ContainerPBImpl containerPBImpl = (ContainerPBImpl) container;
      try { 
        zkClient.delete(APPS + containerPathFromContainerId(container.getId()),
            -1);
      } catch(InterruptedException ie) {
        throw new InterruptedIOException(ie.getMessage());
      } catch(KeeperException ke) {
        LOG.info("Keeper exception", ke);
        throw convertToIOException(ke);
      }
    }

    @Override
    public void updateApplicationState(
        ApplicationMaster master) throws IOException {
      if (!doneWithRecovery) return;
      
      String appString = APPS + ConverterUtils.toString(applicationId);
      ApplicationMasterPBImpl masterPBImpl = (ApplicationMasterPBImpl) master;
      try {
        zkClient.setData(appString, masterPBImpl.getProto().toByteArray(), -1);
      } catch(InterruptedException ie) {
        LOG.info("Interrupted", ie);
        throw new InterruptedIOException(ie.getMessage());
      } catch(KeeperException ke) {
        LOG.info("Keeper exception", ke);
        throw convertToIOException(ke);
      }
    }

    @Override
    public boolean isLoggable() {
      return doneWithRecovery;
    }
  }

  @Override
  public synchronized ApplicationStore createApplicationStore(ApplicationId application, 
      ApplicationSubmissionContext context) throws IOException {
    if (!doneWithRecovery) return new ZKApplicationStore(application);
    
    ApplicationSubmissionContextPBImpl contextPBImpl = (ApplicationSubmissionContextPBImpl) context;
    String appString = APPS + ConverterUtils.toString(application);
   
    ApplicationMasterPBImpl masterPBImpl = new ApplicationMasterPBImpl();
    ContainerPBImpl container = new ContainerPBImpl();
    try {
      zkClient.create(appString, contextPBImpl.getProto()
          .toByteArray(), null, CreateMode.PERSISTENT);
      zkClient.create(appString + ZK_PATH_SEPARATOR + APP_MASTER, 
          masterPBImpl.getProto().toByteArray(), null, CreateMode.PERSISTENT);
      zkClient.create(appString + ZK_PATH_SEPARATOR + APP_MASTER_CONTAINER, 
          container.getProto().toByteArray(), null, CreateMode.PERSISTENT);
    } catch(InterruptedException ie) {
      LOG.info("Interrupted", ie);
      throw new InterruptedIOException(ie.getMessage());
    } catch(KeeperException ke) {
      LOG.info("Keeper exception", ke);
      throw convertToIOException(ke);
    }
    return new ZKApplicationStore(application);
  }

  @Override
  public synchronized void removeApplication(ApplicationId application) throws IOException {
    if (!doneWithRecovery) return;
    
    try {
      zkClient.delete(APPS + ConverterUtils.toString(application), -1);
    } catch(InterruptedException ie) {
      LOG.info("Interrupted", ie);
      throw new InterruptedIOException(ie.getMessage());
    } catch(KeeperException ke) {
      LOG.info("Keeper Exception", ke);
      throw convertToIOException(ke);
    }
  }

  @Override
  public boolean isLoggable() {
    return doneWithRecovery;
  }

  @Override
  public void doneWithRecovery() {
    this.doneWithRecovery = true;
  }

  
  @Override
  public synchronized RMState restore() throws IOException {
    ZKRMState rmState = new ZKRMState();
    rmState.load();
    return rmState;
  }  

  private class ApplicationInfoImpl implements ApplicationInfo {
    private ApplicationMaster master;
    private Container masterContainer;

    private final ApplicationSubmissionContext context;
    private final List<Container> containers = new ArrayList<Container>();

    public ApplicationInfoImpl(ApplicationSubmissionContext context) {
      this.context = context;
    }

    public void setApplicationMaster(ApplicationMaster master) {
      this.master = master;
    }

    public void setMasterContainer(Container container) {
      this.masterContainer = container;
    }

    @Override
    public ApplicationMaster getApplicationMaster() {
      return this.master;
    }

    @Override
    public ApplicationSubmissionContext getApplicationSubmissionContext() {
      return this.context;
    }

    @Override
    public Container getMasterContainer() {
      return this.masterContainer;
    }

    @Override
    public List<Container> getContainers() {
      return this.containers;
    }

    public void addContainer(Container container) {
      containers.add(container);
    }
  }

  private class ZKRMState implements RMState {
    private List<NodeManager> nodeManagers = new ArrayList<NodeManager>();
    private Map<ApplicationId, ApplicationInfo> applications = new 
    HashMap<ApplicationId, ApplicationInfo>();

    public ZKRMState() {
      LOG.info("Restoring RM state from ZK");
    }

    private synchronized List<NodeManagerInfo> listStoredNodes() throws IOException {
      /** get the list of nodes stored in zk **/
      //TODO PB
      List<NodeManagerInfo> nodes = new ArrayList<NodeManagerInfo>();
      Stat stat = new Stat();
      try {
        List<String> children = zkClient.getChildren(NODES, false);
        for (String child: children) {
          byte[] data = zkClient.getData(NODES + child, false, stat);
          NodeManagerInfoPBImpl nmImpl = new NodeManagerInfoPBImpl(
              NodeManagerInfoProto.parseFrom(data));
          nodes.add(nmImpl);
        }
      } catch (InterruptedException ie) {
        LOG.info("Interrupted" , ie);
        throw new InterruptedIOException("Interrupted");
      } catch(KeeperException ke) {
        LOG.error("Failed to list nodes", ke);
        throw convertToIOException(ke);
      }
      return nodes;
    }

    @Override
    public List<NodeManager> getStoredNodeManagers()  {
      return nodeManagers;
    }

    @Override
    public NodeId getLastLoggedNodeId() {
      return nodeId;
    }

    private void readLastNodeId() throws IOException {
      Stat stat = new Stat();
      try {
        byte[] data = zkClient.getData(NODES + NODE_ID, false, stat);
        nodeId = new NodeIdPBImpl(NodeIdProto.parseFrom(data));
      } catch(InterruptedException ie) {
        LOG.info("Interrupted", ie);
        throw new InterruptedIOException(ie.getMessage());
      } catch(KeeperException ke) {
        LOG.info("Keeper Exception", ke);
        throw convertToIOException(ke);
      }
    }

    private ApplicationInfo getAppInfo(String app) throws IOException {
      ApplicationInfoImpl info = null;
      Stat stat = new Stat();
      try {
        ApplicationSubmissionContext context = null;
        byte[] data = zkClient.getData(APPS + app, false, stat);
        context = new ApplicationSubmissionContextPBImpl(
            ApplicationSubmissionContextProto.parseFrom(data));
        info = new ApplicationInfoImpl(context);
        List<String> children = zkClient.getChildren(APPS + app, false, stat);
        ApplicationMaster master = null;
        for (String child: children) {
          byte[] childdata = zkClient.getData(APPS + app + ZK_PATH_SEPARATOR + child, false, stat);
          if (APP_MASTER.equals(child)) {
            master = new ApplicationMasterPBImpl(ApplicationMasterProto.parseFrom(childdata));
            info.setApplicationMaster(master);
          } else if (APP_MASTER_CONTAINER.equals(child)) {
            Container masterContainer = new ContainerPBImpl(ContainerProto.parseFrom(data));
            info.setMasterContainer(masterContainer);
          } else {
            Container container = new ContainerPBImpl(ContainerProto.parseFrom(data));
            info.addContainer(container);
          }
        }
      } catch(InterruptedException ie) {
        LOG.info("Interrupted", ie);
        throw new InterruptedIOException(ie.getMessage());
      } catch(KeeperException ke) {
        throw convertToIOException(ke);
      }
      return info;
    }

    private void load() throws IOException {
      List<NodeManagerInfo> nodeInfos = listStoredNodes();
      final Pattern trackerPattern = Pattern.compile(".*:.*");
      final Matcher m = trackerPattern.matcher("");
      for (NodeManagerInfo node: nodeInfos) {
        m.reset(node.getNodeAddress());
        if (!m.find()) {
          LOG.info("Skipping node, bad node-address " + node.getNodeAddress());
          continue;
        }
        String hostName = m.group(0);
        int cmPort = Integer.valueOf(m.group(1));
        m.reset(node.getHttpAddress());
        if (!m.find()) {
          LOG.info("Skipping node, bad http-address " + node.getHttpAddress());
          continue;
        }
        int httpPort = Integer.valueOf(m.group(1));
        NodeManager nm = new NodeManagerImpl(node.getNodeId(),
            hostName, cmPort, httpPort,
            RMResourceTrackerImpl.resolve(node.getNodeAddress()), 
            node.getCapability());
        nodeManagers.add(nm);
      }
      readLastNodeId();
      /* make sure we get all the applications */
      List<String> apps = null;
      try {
        apps = zkClient.getChildren(APPS, false);
      } catch(InterruptedException ie) {
        LOG.info("Interrupted", ie);
        throw new InterruptedIOException(ie.getMessage());
      } catch(KeeperException ke) {
        throw convertToIOException(ke);
      }
      for (String app: apps) {
        ApplicationInfo info = getAppInfo(app);
        applications.put(info.getApplicationMaster().getApplicationId(), info);
      }
    }

    @Override
    public Map<ApplicationId, ApplicationInfo> getStoredApplications() {
      return applications;
    }
  }
}