package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.util.Records;

public class MockNM {

  private int responseId;
  private NodeId nodeId;
  private final String nodeIdStr;
  private final int memory;
  private final ResourceTrackerService resourceTracker;

  MockNM(String nodeIdStr, int memory, ResourceTrackerService resourceTracker) {
    this.nodeIdStr = nodeIdStr;
    this.memory = memory;
    this.resourceTracker = resourceTracker;
  }

  public void containerStatus(Container container) throws Exception {
    Map<ApplicationId, List<Container>> conts = new HashMap<ApplicationId, List<Container>>();
    conts.put(container.getId().getAppId(), Arrays.asList(new Container[]{}));
    nodeHeartbeat(conts, true);
  }

  public NodeId registerNode() throws Exception {
    String[] splits = nodeIdStr.split(":");
    nodeId = Records.newRecord(NodeId.class);
    nodeId.setHost(splits[0]);
    nodeId.setPort(Integer.parseInt(splits[1]));
    RegisterNodeManagerRequest req = Records.newRecord(
        RegisterNodeManagerRequest.class);
    req.setNodeId(nodeId);
    req.setHttpPort(2);
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(memory);
    req.setResource(resource);
    resourceTracker.registerNodeManager(req);
    return nodeId;
  }

  public HeartbeatResponse nodeHeartbeat(boolean b) throws Exception {
    return nodeHeartbeat(new HashMap<ApplicationId, List<Container>>(), b);
  }

  public HeartbeatResponse nodeHeartbeat(Map<ApplicationId, 
      List<Container>> conts, boolean isHealthy) throws Exception {
    NodeHeartbeatRequest req = Records.newRecord(NodeHeartbeatRequest.class);
    NodeStatus status = Records.newRecord(NodeStatus.class);
    status.setNodeId(nodeId);
    for (Map.Entry<ApplicationId, List<Container>> entry : conts.entrySet()) {
      status.setContainers(entry.getKey(), entry.getValue());
    }
    NodeHealthStatus healthStatus = Records.newRecord(NodeHealthStatus.class);
    healthStatus.setHealthReport("");
    healthStatus.setIsNodeHealthy(isHealthy);
    healthStatus.setLastHealthReportTime(1);
    status.setNodeHealthStatus(healthStatus);
    status.setResponseId(++responseId);
    req.setNodeStatus(status);
    return resourceTracker.nodeHeartbeat(req).getHeartbeatResponse();
  }

}
