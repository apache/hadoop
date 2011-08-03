package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Test;

public class TestRM {

  @Test
  public void testApp() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM();
    rm.start();
    rm.registerNode("h1:1234", 5000);
    rm.registerNode("h2:5678", 10000);
    
    RMApp app = rm.submitApp(2000);

    //kick the scheduling
    rm.nodeHeartbeat("h1:1234", true);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    rm.sendAMLaunched(attempt.getAppAttemptId());
    rm.registerAppAttempt(attempt.getAppAttemptId());
    rm.unregisterAppAttempt(attempt.getAppAttemptId());
    rm.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.FINISHED);

    rm.stop();
  }

  public static void main(String[] args) throws Exception {
    TestRM t = new TestRM();
    t.testApp();
  }
}
