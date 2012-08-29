package org.apache.hadoop.mapreduce.v2.app2.rm.node;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;

public class AMNodeMap extends AbstractService implements
    EventHandler<AMNodeEvent> {

  static final Log LOG = LogFactory.getLog(AMNodeMap.class);
  
  private final ConcurrentHashMap<NodeId, AMNode> nodeMap;
  // TODO XXX -> blacklistMap is also used for computing forcedUnblacklisting.
  private final ConcurrentHashMap<String, ArrayList<NodeId>> blacklistMap;
  private final EventHandler<?> eventHandler;
  private final AppContext appContext;
  private int maxTaskFailuresPerNode;
  private boolean nodeBlacklistingEnabled;
  private int blacklistDisablePercent;
  
  public AMNodeMap(EventHandler<?> eventHandler, AppContext appContext) {
    super("AMNodeMap");
    this.nodeMap = new ConcurrentHashMap<NodeId, AMNode>();
    this.blacklistMap = new ConcurrentHashMap<String, ArrayList<NodeId>>();
    this.eventHandler = eventHandler;
    this.appContext = appContext;
  
    // TODO XXX: Get a handle of allowed failures.
  }
  
  @Override
  public synchronized void init(Configuration config) {
    Configuration conf = new Configuration(config);
    this.maxTaskFailuresPerNode = conf.getInt(
        MRJobConfig.MAX_TASK_FAILURES_PER_TRACKER, 3);
    this.nodeBlacklistingEnabled = config.getBoolean(
        MRJobConfig.MR_AM_JOB_NODE_BLACKLISTING_ENABLE, true);
    this.blacklistDisablePercent = config.getInt(
          MRJobConfig.MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERECENT,
          MRJobConfig.DEFAULT_MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERCENT);
    
    LOG.info("maxTaskFailuresPerNode is " + maxTaskFailuresPerNode);
    if (blacklistDisablePercent < -1 || blacklistDisablePercent > 100) {
      throw new YarnException("Invalid blacklistDisablePercent: "
          + blacklistDisablePercent
          + ". Should be an integer between 0 and 100 or -1 to disabled");
    }
    LOG.info("blacklistDisablePercent is " + blacklistDisablePercent);
    
    super.init(conf);
  }
  
  
  public void nodeSeen(NodeId nodeId) {
    nodeMap.putIfAbsent(nodeId, new AMNodeImpl(
        nodeId, maxTaskFailuresPerNode, eventHandler, appContext));
  }
  
  public boolean isHostBlackListed(String hostname) {
    if (!nodeBlacklistingEnabled) {
      return false;
    }
    
    return blacklistMap.containsKey(hostname);
  }
  
  private void addToBlackList(NodeId nodeId) {
    String host = nodeId.getHost();
    ArrayList<NodeId> nodes;
    
    if (!blacklistMap.containsKey(host)) {
      nodes = new ArrayList<NodeId>();
      blacklistMap.put(host, nodes);
    } else {
      nodes = blacklistMap.get(host);
    }
    
    if (!nodes.contains(nodeId)) {
      nodes.add(nodeId);
    }
  }
  
  // TODO: Currently, un-blacklisting feature is not supported.
  /*
  private void removeFromBlackList(NodeId nodeId) {
    String host = nodeId.getHost();
    if (blacklistMap.containsKey(host)) {
      ArrayList<NodeId> nodes = blacklistMap.get(host);
      nodes.remove(nodeId);
    }
  }
  */
  
  public void handle(AMNodeEvent event) {
    if (event.getType() == AMNodeEventType.N_NODE_WAS_BLACKLISTED) {
      NodeId nodeId = event.getNodeId();
      addToBlackList(nodeId);
    } else {
      NodeId nodeId = event.getNodeId();
      nodeMap.get(nodeId).handle(event);
    }
  }
  
  public AMNode get(NodeId nodeId) {
    return nodeMap.get(nodeId);
  }
  
  public int size() {
    return nodeMap.size();
  }
  
}
