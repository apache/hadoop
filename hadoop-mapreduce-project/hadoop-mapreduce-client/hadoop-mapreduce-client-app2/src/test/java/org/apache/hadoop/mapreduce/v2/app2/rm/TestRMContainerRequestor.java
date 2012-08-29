package org.apache.hadoop.mapreduce.v2.app2.rm;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Set;

import junit.framework.Assert;

import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.ControlledClock;
import org.apache.hadoop.mapreduce.v2.app2.job.Job;
import org.apache.hadoop.mapreduce.v2.app2.rm.RMContainerRequestor.ContainerRequest;
import org.apache.hadoop.mapreduce.v2.app2.rm.node.AMNodeMap;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Test;

public class TestRMContainerRequestor {
  
  @Test
  public void testFailedAllocate() throws Exception{
    AppContext appContext = setupDefaultTestContext();
    AMRMProtocolForFailedAllocate amrm = createAMRMProtocolForFailedAllocate();
    RMContainerRequestorForTest rmComm = new RMContainerRequestorForTest(appContext, amrm);
    amrm.setRmCommunicator(rmComm);
    rmComm.init(new YarnConfiguration());
    rmComm.start();
    
    Resource resource = BuilderUtils.newResource(512);
    String [] hosts = new String[]{"host1", "host2"};
    String [] racks = new String[]{"rack1"};
    Priority priority = BuilderUtils.newPriority(5);
    ContainerRequest cr1 = new ContainerRequest(resource, hosts, racks, priority);
    ContainerRequest cr2 = new ContainerRequest(resource, new String[]{"host1"}, racks, priority);
    
    rmComm.addContainerReq(cr1);
    rmComm.addContainerReq(cr2);
    

    // Set containerRequest to be decremented.
    amrm.setIncContainerRequest(cr1);
    amrm.setDecContainerRequest(cr2);
    
    // Verify initial ask.
    Set<ResourceRequest> askSet = null;
    askSet = rmComm.getAskSet();
    assertEquals(4, askSet.size()); //2 hosts. 1 rack. *
    verifyAsks(askSet, 2, 1, 2, 2);
    
    //First heartbeat
    rmComm.heartbeat();
    //Verify empty ask.
    askSet = rmComm.getAskSet();
    assertEquals(0, askSet.size()); //2 hosts. 1 rack. *
    
    // Add 2 more container requests.
    rmComm.addContainerReq(cr1);
    rmComm.addContainerReq(cr2);
    
    //Verify ask
    askSet = rmComm.getAskSet();
    assertEquals(4, askSet.size());
    verifyAsks(askSet, 4, 2, 4, 4);
    
    try {
      rmComm.heartbeat();
      Assert.fail("Second heartbeat was expected to fail");
    } catch (YarnRemoteException yre) {
    }
    
    // Verify ask. Should factor in +cr1 = 5 3 5 5, -cr2 = 4 3 4 4
    assertEquals(4, askSet.size());
    verifyAsks(askSet, 4, 3, 4, 4);
  }
  
  private void verifyAsks(Set<ResourceRequest> askSet, int host1, int host2, int rack1, int generic) {
    for (ResourceRequest rr : askSet) {
      if (rr.getHostName().equals("*")) {
        assertEquals(generic, rr.getNumContainers());
      } else if (rr.getHostName().equals("host1")) {
        assertEquals(host1, rr.getNumContainers());
      } else if (rr.getHostName().equals("host2")) {
        assertEquals(host2, rr.getNumContainers());
      } else if (rr.getHostName().equals("rack1")) {
        assertEquals(rack1, rr.getNumContainers());
      }
    }
  }
  
  private AMRMProtocolForFailedAllocate createAMRMProtocolForFailedAllocate() {
    AMResponse amResponse = BuilderUtils
        .newAMResponse(new ArrayList<Container>(),
            BuilderUtils.newResource(1024), new ArrayList<ContainerStatus>(),
            false, 1, new ArrayList<NodeReport>());
    AllocateResponse allocateResponse = BuilderUtils.newAllocateResponse(
        amResponse, 2);
    return new AMRMProtocolForFailedAllocate(allocateResponse);
  }

  class AMRMProtocolForFailedAllocate implements AMRMProtocol {
    private AllocateResponse allocateResponse;
    private RMContainerRequestor rmComm;
    private ContainerRequest crInc;
    private ContainerRequest crDec;

    AMRMProtocolForFailedAllocate(AllocateResponse response) {
      allocateResponse = response;
    }
    
    void setRmCommunicator(RMContainerRequestor rmComm) {
      this.rmComm = rmComm;
    }
    
    void setIncContainerRequest(ContainerRequest cr) {
      this.crInc = cr;
    }
    
    void setDecContainerRequest(ContainerRequest cr) {
      this.crDec = cr;
    }

    @Override
    public RegisterApplicationMasterResponse registerApplicationMaster(
        RegisterApplicationMasterRequest request) throws YarnRemoteException {
      return null;
    }

    @Override
    public FinishApplicationMasterResponse finishApplicationMaster(
        FinishApplicationMasterRequest request) throws YarnRemoteException {
      return null;
    }

    @Override
    public AllocateResponse allocate(AllocateRequest request)
        throws YarnRemoteException {
      if (request.getResponseId() == 0) {
        return allocateResponse;
      } else if (request.getResponseId() == 1) {
        // Change the table before throwing the exception.
        rmComm.addContainerReq(crInc);
        rmComm.decContainerReq(crDec);
        throw RPCUtil.getRemoteException("MockRpcError");
      }
      return null;
    }
  }

  class RMContainerRequestorForTest extends RMContainerRequestor {

    private AMRMProtocol amRmProtocol;
    
    public RMContainerRequestorForTest(AppContext context) {
      super(null, context);
    }

    public RMContainerRequestorForTest(AppContext context, AMRMProtocol amrm) {
      super(null, context);
      this.amRmProtocol = amrm;
    }
    
    @Override
    public AMRMProtocol createSchedulerProxy() {
      if (amRmProtocol == null) {
        amRmProtocol = mock(AMRMProtocol.class);
        AMResponse amResponse = BuilderUtils.newAMResponse(
            new ArrayList<Container>(), BuilderUtils.newResource(1024),
            new ArrayList<ContainerStatus>(), false, 1,
            new ArrayList<NodeReport>());
        AllocateResponse allocateResponse = BuilderUtils.newAllocateResponse(
            amResponse, 2);
        try {
          when(amRmProtocol.allocate(any(AllocateRequest.class))).thenReturn(allocateResponse);
        } catch (YarnRemoteException e) {
        }
      }
      return amRmProtocol;
    }
    
    @Override public void register() {}
    @Override public void unregister() {}
      
    @Override public void startAllocatorThread() {}
  }

  private AppContext setupDefaultTestContext() {
    ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        appId, 1);
    JobID id = TypeConverter.fromYarn(appId);
    JobId jobId = TypeConverter.toYarn(id);

    Job mockJob = mock(Job.class);
    when(mockJob.getID()).thenReturn(jobId);
    when(mockJob.getProgress()).thenReturn(0.0f);

    @SuppressWarnings("rawtypes")
    EventHandler handler = mock(EventHandler.class);

    Clock clock = new ControlledClock(new SystemClock());
    
    AMNodeMap amNodeMap = mock(AMNodeMap.class);
    when(amNodeMap.isHostBlackListed(any(String.class))).thenReturn(false);
    
    AppContext appContext = mock(AppContext.class);
    when(appContext.getApplicationID()).thenReturn(appId);
    when(appContext.getApplicationAttemptId()).thenReturn(appAttemptId);
    when(appContext.getEventHandler()).thenReturn(handler);
    when(appContext.getJob(jobId)).thenReturn(mockJob);
    when(appContext.getClock()).thenReturn(clock);
    when(appContext.getAllNodes()).thenReturn(amNodeMap);

    return appContext;
  }
}