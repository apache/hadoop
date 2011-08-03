package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationIdRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationIdResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEvent;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.ams.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.StoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptLaunchFailedEvent;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class MockRM extends ResourceManager {

  private RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private Map<NodeId, Integer> responseIds = new HashMap<NodeId, Integer>();
  private Map<ApplicationAttemptId, Integer> AMResponseIds = new HashMap<ApplicationAttemptId, Integer>();

  public MockRM() {
    this(new Configuration());
  }

  public MockRM(Configuration conf) {
    super(StoreFactory.getStore(conf));
    init(conf);
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
  }

  public void waitForState(ApplicationId appId, RMAppState finalState) 
      throws Exception {
    RMApp app = getRMContext().getRMApps().get(appId);
    int timeoutSecs = 0;
    while (!finalState.equals(app.getState()) &&
        timeoutSecs++ < 20) {
      System.out.println("App State is : " + app.getState() +
          " Waiting for state : " + finalState);
      Thread.sleep(500);
    }
    System.out.println("App State is : " + app.getState());
    Assert.assertEquals("App state is not correct (timedout)",
        finalState, app.getState());
  }

  public void waitForState(ApplicationAttemptId attemptId, RMAppAttemptState finalState)
      throws Exception {
    RMApp app = getRMContext().getRMApps().get(attemptId.getApplicationId());
    RMAppAttempt attempt = app.getRMAppAttempt(attemptId);
    int timeoutSecs = 0;
    while (!finalState.equals(attempt.getAppAttemptState())
        && timeoutSecs++ < 20) {
      System.out
          .println("AppAttempt State is : " + attempt.getAppAttemptState()
              + " Waiting for state : " + finalState);
      Thread.sleep(500);
    }
    System.out.println("AppAttempt State is : " + attempt.getAppAttemptState());
    Assert.assertEquals("AppAttempt state is not correct (timedout)",
        finalState, attempt.getAppAttemptState());
  }

  //client
  public RMApp submitApp(int masterMemory) throws Exception {
    ClientRMProtocol client = getClientRMService();
    GetNewApplicationIdResponse resp = client.getNewApplicationId(recordFactory.newRecordInstance(GetNewApplicationIdRequest.class));
    ApplicationId appId = resp.getApplicationId();
    
    SubmitApplicationRequest req = recordFactory.newRecordInstance(SubmitApplicationRequest.class);
    ApplicationSubmissionContext sub = recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    sub.setApplicationId(appId);
    sub.setApplicationName("");
    sub.setUser("");
    Resource capability = recordFactory.newRecordInstance(Resource.class);
    capability.setMemory(masterMemory);
    sub.setMasterCapability(capability);
    req.setApplicationSubmissionContext(sub);
    
    client.submitApplication(req);
    waitForState(appId, RMAppState.ACCEPTED);
    return getRMContext().getRMApps().get(appId);
  }

  public void killApp(ApplicationId appId) throws Exception {
    ClientRMProtocol client = getClientRMService();
    FinishApplicationRequest req = recordFactory.newRecordInstance(FinishApplicationRequest.class);
    req.setApplicationId(appId);
    client.finishApplication(req);
  }

  //from AMLauncher
  public void sendAMLaunched(ApplicationAttemptId appAttemptId) throws Exception {
    waitForState(appAttemptId, RMAppAttemptState.ALLOCATED);
    getRMContext().getDispatcher().getEventHandler().handle(
        new RMAppAttemptEvent(appAttemptId, RMAppAttemptEventType.LAUNCHED));
  }

  public void sendAMLaunchFailed(ApplicationAttemptId appAttemptId) throws Exception {
    waitForState(appAttemptId, RMAppAttemptState.ALLOCATED);
    getRMContext().getDispatcher().getEventHandler().handle(
        new RMAppAttemptLaunchFailedEvent(appAttemptId, "Failed"));
  }

  //from AMS
  public void registerAppAttempt(ApplicationAttemptId attemptId) throws Exception {
    waitForState(attemptId, RMAppAttemptState.LAUNCHED);
    AMResponseIds.put(attemptId, 0);
    RegisterApplicationMasterRequest req = recordFactory.newRecordInstance(RegisterApplicationMasterRequest.class);
    req.setApplicationAttemptId(attemptId);
    req.setHost("");
    req.setRpcPort(1);
    req.setTrackingUrl("");
    masterService.registerApplicationMaster(req);
  }

  public List<Container> allocate(ApplicationAttemptId attemptId, 
      String host, int memory, int numContainers, 
      List<ContainerId> releases) throws Exception {
    List reqs = createReq(host, memory, 1, numContainers);
    List<Container> toRelease = new ArrayList<Container>();
    for (ContainerId id : releases) {
      Container cont = recordFactory.newRecordInstance(Container.class);
      cont.setId(id);
      //TOOD: set all fields
    }
    return allocate(attemptId, toRelease, reqs);
  }

  private List<ResourceRequest> createReq(String host, int memory, int priority, 
      int containers) throws Exception {
    ResourceRequest hostReq = createResourceReq(host, memory, priority, 
        containers);
    ResourceRequest rackReq = createResourceReq("default-rack", memory, 
        priority, containers);
    ResourceRequest offRackReq = createResourceReq("*", memory, priority, 
        containers);
    return Arrays.asList(new ResourceRequest[] {hostReq, rackReq, offRackReq});
    
  }
  private ResourceRequest createResourceReq(String resource, int memory, int priority, 
      int containers) throws Exception {
    ResourceRequest req = recordFactory.newRecordInstance(ResourceRequest.class);
    req.setHostName(resource);
    req.setNumContainers(containers);
    Priority pri = recordFactory.newRecordInstance(Priority.class);
    pri.setPriority(1);
    req.setPriority(pri);
    Resource capability = recordFactory.newRecordInstance(Resource.class);
    capability.setMemory(memory);
    req.setCapability(capability);
    return req;
  }

  public List<Container> allocate(ApplicationAttemptId attemptId, 
      List<Container> releases, List<ResourceRequest> resourceRequest) 
      throws Exception {
    AllocateRequest req = recordFactory.newRecordInstance(AllocateRequest.class);
    int responseId = AMResponseIds.remove(attemptId) + 1;
    AMResponseIds.put(attemptId, responseId);
    req.setResponseId(responseId);
    req.setApplicationAttemptId(attemptId);
    req.addAllAsks(resourceRequest);
    req.addAllReleases(releases);
    AllocateResponse resp = masterService.allocate(req);
    return resp.getAMResponse().getContainerList();
  }

  public void unregisterAppAttempt(ApplicationAttemptId attemptId) throws Exception {
    AMResponseIds.remove(attemptId);
    waitForState(attemptId, RMAppAttemptState.RUNNING);
    FinishApplicationMasterRequest req = recordFactory.newRecordInstance(FinishApplicationMasterRequest.class);
    req.setAppAttemptId(attemptId);
    req.setDiagnostics("");
    req.setFinalState("");
    req.setTrackingUrl("");
    masterService.finishApplicationMaster(req);
  }

  //from Node
  public void containerStatus(Container container, NodeId nodeId) throws Exception {
    Map<ApplicationId, List<Container>> conts = new HashMap<ApplicationId, List<Container>>();
    conts.put(container.getId().getAppId(), Arrays.asList(new Container[]{}));
    nodeHeartbeat(nodeId, conts, true);
  }

  public void registerNode(String nodeIdStr, int memory) throws Exception {
    String[] splits = nodeIdStr.split(":");
    NodeId nodeId = recordFactory.newRecordInstance(NodeId.class);
    nodeId.setHost(splits[0]);
    nodeId.setPort(Integer.parseInt(splits[1]));
    RegisterNodeManagerRequest req = recordFactory
        .newRecordInstance(RegisterNodeManagerRequest.class);
    req.setNodeId(nodeId);
    req.setHttpPort(2);
    Resource resource = recordFactory.newRecordInstance(Resource.class);
    resource.setMemory(memory);
    req.setResource(resource);
    getResourceTrackerService().registerNodeManager(req);
    responseIds.put(nodeId, 0);
  }

  public HeartbeatResponse nodeHeartbeat(String nodeIdStr, boolean b) throws Exception {
    String[] splits = nodeIdStr.split(":");
    NodeId nodeId = recordFactory.newRecordInstance(NodeId.class);
    nodeId.setHost(splits[0]);
    nodeId.setPort(Integer.parseInt(splits[1]));
    return nodeHeartbeat(nodeId, new HashMap<ApplicationId, List<Container>>(), b);
  }

  public HeartbeatResponse nodeHeartbeat(NodeId nodeId, Map<ApplicationId, 
      List<Container>> conts, boolean isHealthy) throws Exception {
    NodeHeartbeatRequest req = recordFactory.newRecordInstance(NodeHeartbeatRequest.class);
    NodeStatus status = recordFactory.newRecordInstance(NodeStatus.class);
    status.setNodeId(nodeId);
    for (Map.Entry<ApplicationId, List<Container>> entry : conts.entrySet()) {
      status.setContainers(entry.getKey(), entry.getValue());
    }
    NodeHealthStatus healthStatus = recordFactory.newRecordInstance(NodeHealthStatus.class);
    healthStatus.setHealthReport("");
    healthStatus.setIsNodeHealthy(isHealthy);
    healthStatus.setLastHealthReportTime(1);
    status.setNodeHealthStatus(healthStatus);
    int responseId = responseIds.remove(nodeId) + 1;
    responseIds.put(nodeId, responseId);
    status.setResponseId(responseId);
    req.setNodeStatus(status);
    return getResourceTrackerService().nodeHeartbeat(req).getHeartbeatResponse();
  }

  @Override
  protected ClientRMService createClientRMService() {
    return new ClientRMService(getRMContext(), amLivelinessMonitor,
        clientToAMSecretManager, getResourceScheduler()) {
      @Override
      public void start() {
        //override to not start rpc handler
      }
      @Override
      public void stop() {
        // don't do anything
      }
    };
  }

  @Override
  protected ResourceTrackerService createResourceTrackerService() {
    return new ResourceTrackerService(getRMContext(), nodesListManager,
        this.nmLivelinessMonitor, this.containerTokenSecretManager){
      @Override
      public void start() {
        //override to not start rpc handler
      }
      @Override
      public void stop() {
        // don't do anything
      }
    };
  }

  @Override
  protected ApplicationMasterService createApplicationMasterService() {
    return new ApplicationMasterService(getRMContext(),
        this.amLivelinessMonitor, this.appTokenSecretManager, scheduler){
      @Override
      public void start() {
        //override to not start rpc handler
      }
      @Override
      public void stop() {
        // don't do anything
      }
    };
  }

  @Override
  protected ApplicationMasterLauncher createAMLauncher() {
    return new ApplicationMasterLauncher(
        this.appTokenSecretManager, this.clientToAMSecretManager,
        getRMContext()) {
      @Override
      public void start() {
        //override to not start rpc handler
      }
      @Override
      public void  handle(AMLauncherEvent appEvent) {
        //don't do anything
      }
      @Override
      public void stop() {
        // don't do anything
      }
    };
  }

  protected AdminService createAdminService() {
    return new AdminService(getConfig(), scheduler, getRMContext(), 
        this.nodesListManager){
      @Override
      public void start() {
        //override to not start rpc handler
      }
      @Override
      public void stop() {
        // don't do anything
      }
    };
  }

  @Override
  protected void startWepApp() {
    //override to disable webapp
  }

}
