package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.Records;

public class MockAM {

  private volatile int responseId = 0;
  private final ApplicationAttemptId attemptId;
  private final RMContext context;
  private final AMRMProtocol amRMProtocol;
  
  private final List<ResourceRequest> requests = new ArrayList<ResourceRequest>();
  private final List<Container> releases = new ArrayList<Container>();

  MockAM(RMContext context, AMRMProtocol amRMProtocol, 
      ApplicationAttemptId attemptId) {
    this.context = context;
    this.amRMProtocol = amRMProtocol;
    this.attemptId = attemptId;
  }

  public void waitForState(RMAppAttemptState finalState) throws Exception {
    RMApp app = context.getRMApps().get(attemptId.getApplicationId());
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

  public void registerAppAttempt() throws Exception {
    waitForState(RMAppAttemptState.LAUNCHED);
    responseId = 0;
    RegisterApplicationMasterRequest req = Records.newRecord(RegisterApplicationMasterRequest.class);
    req.setApplicationAttemptId(attemptId);
    req.setHost("");
    req.setRpcPort(1);
    req.setTrackingUrl("");
    amRMProtocol.registerApplicationMaster(req);
  }

  public void addRequests(String[] hosts, int memory, int priority, 
      int containers) throws Exception {
    requests.addAll(createReq(hosts, memory, priority, containers));
  }

  public AMResponse schedule() throws Exception {
    AMResponse response = allocate(requests, releases);
    requests.clear();
    releases.clear();
    return response;
  }

  public AMResponse allocate( 
      String host, int memory, int numContainers, 
      List<ContainerId> releases) throws Exception {
    List reqs = createReq(new String[]{host}, memory, 1, numContainers);
    List<Container> toRelease = new ArrayList<Container>();
    for (ContainerId id : releases) {
      Container cont = Records.newRecord(Container.class);
      cont.setId(id);
      //TOOD: set all fields
    }
    return allocate(reqs, toRelease);
  }

  public List<ResourceRequest> createReq(String[] hosts, int memory, int priority, 
      int containers) throws Exception {
    List<ResourceRequest> reqs = new ArrayList<ResourceRequest>();
    for (String host : hosts) {
      ResourceRequest hostReq = createResourceReq(host, memory, priority, 
          containers);
      reqs.add(hostReq);
      ResourceRequest rackReq = createResourceReq("default-rack", memory, 
          priority, containers);
      reqs.add(rackReq);
    }
    
    ResourceRequest offRackReq = createResourceReq("*", memory, priority, 
        containers);
    reqs.add(offRackReq);
    return reqs;
    
  }

  public ResourceRequest createResourceReq(String resource, int memory, int priority, 
      int containers) throws Exception {
    ResourceRequest req = Records.newRecord(ResourceRequest.class);
    req.setHostName(resource);
    req.setNumContainers(containers);
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(1);
    req.setPriority(pri);
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(memory);
    req.setCapability(capability);
    return req;
  }

  public AMResponse allocate(
      List<ResourceRequest> resourceRequest, List<Container> releases) 
      throws Exception {
    AllocateRequest req = Records.newRecord(AllocateRequest.class);
    req.setResponseId(++responseId);
    req.setApplicationAttemptId(attemptId);
    req.addAllAsks(resourceRequest);
    req.addAllReleases(releases);
    AllocateResponse resp = amRMProtocol.allocate(req);
    return resp.getAMResponse();
  }

  public void unregisterAppAttempt() throws Exception {
    waitForState(RMAppAttemptState.RUNNING);
    FinishApplicationMasterRequest req = Records.newRecord(FinishApplicationMasterRequest.class);
    req.setAppAttemptId(attemptId);
    req.setDiagnostics("");
    req.setFinalState("");
    req.setTrackingUrl("");
    amRMProtocol.finishApplicationMaster(req);
  }
}
