package org.apache.hadoop.yarn.server.resourcemanager;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationIdRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationIdResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEvent;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.ams.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.StoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptLaunchFailedEvent;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class MockRM extends ResourceManager {

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

  //client
  public RMApp submitApp(int masterMemory) throws Exception {
    ClientRMProtocol client = getClientRMService();
    GetNewApplicationIdResponse resp = client.getNewApplicationId(Records.newRecord(GetNewApplicationIdRequest.class));
    ApplicationId appId = resp.getApplicationId();
    
    SubmitApplicationRequest req = Records.newRecord(SubmitApplicationRequest.class);
    ApplicationSubmissionContext sub = Records.newRecord(ApplicationSubmissionContext.class);
    sub.setApplicationId(appId);
    sub.setApplicationName("");
    sub.setUser("");
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(masterMemory);
    sub.setMasterCapability(capability);
    req.setApplicationSubmissionContext(sub);
    
    client.submitApplication(req);
    waitForState(appId, RMAppState.ACCEPTED);
    return getRMContext().getRMApps().get(appId);
  }

  public MockNM registerNode(String nodeIdStr, int memory) throws Exception {
    MockNM nm = new MockNM(nodeIdStr, memory, getResourceTrackerService());
    nm.registerNode();
    return nm;
  }

  public void killApp(ApplicationId appId) throws Exception {
    ClientRMProtocol client = getClientRMService();
    FinishApplicationRequest req = Records.newRecord(FinishApplicationRequest.class);
    req.setApplicationId(appId);
    client.finishApplication(req);
  }

  //from AMLauncher
  public MockAM sendAMLaunched(ApplicationAttemptId appAttemptId) throws Exception {
    MockAM am = new MockAM(getRMContext(), masterService, appAttemptId);
    am.waitForState(RMAppAttemptState.ALLOCATED);
    getRMContext().getDispatcher().getEventHandler().handle(
        new RMAppAttemptEvent(appAttemptId, RMAppAttemptEventType.LAUNCHED));
    return am;
  }

  
  public void sendAMLaunchFailed(ApplicationAttemptId appAttemptId) throws Exception {
    MockAM am = new MockAM(getRMContext(), masterService, appAttemptId);
    am.waitForState(RMAppAttemptState.ALLOCATED);
    getRMContext().getDispatcher().getEventHandler().handle(
        new RMAppAttemptLaunchFailedEvent(appAttemptId, "Failed"));
  }

  @Override
  protected ClientRMService createClientRMService() {
    return new ClientRMService(getRMContext(),
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
        this.appTokenSecretManager, scheduler) {
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
