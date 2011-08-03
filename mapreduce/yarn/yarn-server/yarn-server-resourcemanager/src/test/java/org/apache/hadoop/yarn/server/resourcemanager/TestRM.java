package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Test;

public class TestRM {

  private static final Log LOG = LogFactory.getLog(TestRM.class);

  @Test
  public void testAppWithNoContainers() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM();
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    
    RMApp app = rm.submitApp(2000);

    //kick the scheduling
    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();
    am.unregisterAppAttempt();
    am.waitForState(RMAppAttemptState.FINISHED);
    rm.stop();
  }

  @Test
  public void testAppOnMultiNode() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM();
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:5678", 10240);
    
    RMApp app = rm.submitApp(2000);

    //kick the scheduling
    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();
    
    //request for containers
    int request = 13;
    am.allocate("h1" , 1000, request, new ArrayList<ContainerId>());
    
    //kick the scheduler
    nm1.nodeHeartbeat(true);
    List<Container> conts = am.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<Container>()).getNewContainerList();
    int contReceived = conts.size();
    while (contReceived < 3) {//only 3 containers are available on node1
      conts.addAll(am.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<Container>()).getNewContainerList());
      contReceived = conts.size();
      LOG.info("Got " + contReceived + " containers. Waiting to get " + 3);
      Thread.sleep(2000);
    }
    Assert.assertEquals(3, conts.size());

    //send node2 heartbeat
    nm2.nodeHeartbeat(true);
    conts = am.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<Container>()).getNewContainerList();
    contReceived = conts.size();
    while (contReceived < 10) {
      conts.addAll(am.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<Container>()).getNewContainerList());
      contReceived = conts.size();
      LOG.info("Got " + contReceived + " containers. Waiting to get " + 10);
      Thread.sleep(2000);
    }
    Assert.assertEquals(10, conts.size());

    am.unregisterAppAttempt();
    am.waitForState(RMAppAttemptState.FINISHED);

    rm.stop();
  }

  public static void main(String[] args) throws Exception {
    TestRM t = new TestRM();
    t.testAppWithNoContainers();
    t.testAppOnMultiNode();
  }
}
