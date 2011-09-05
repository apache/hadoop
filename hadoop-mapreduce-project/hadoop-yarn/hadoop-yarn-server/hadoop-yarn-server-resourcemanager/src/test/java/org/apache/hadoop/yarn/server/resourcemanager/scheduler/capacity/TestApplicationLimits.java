package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestApplicationLimits {
  
  private static final Log LOG = LogFactory.getLog(TestApplicationLimits.class);
  final static int GB = 1024;

  LeafQueue queue;
  
  @Before
  public void setUp() {
    CapacitySchedulerConfiguration csConf = 
        new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);
    
    
    CapacitySchedulerContext csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getMinimumResourceCapability()).thenReturn(Resources.createResource(GB));
    when(csContext.getMaximumResourceCapability()).thenReturn(Resources.createResource(16*GB));
    when(csContext.getClusterResources()).thenReturn(Resources.createResource(10 * 16 * GB));
    
    Map<String, Queue> queues = new HashMap<String, Queue>();
    Queue root = 
        CapacityScheduler.parseQueue(csContext, csConf, null, "root", 
            queues, queues, 
            CapacityScheduler.queueComparator, 
            CapacityScheduler.applicationComparator, 
            TestUtils.spyHook);

    
    queue = spy(
        new LeafQueue(csContext, A, root, 
                      CapacityScheduler.applicationComparator, null)
        );

    // Stub out ACL checks
    doReturn(true).
        when(queue).hasAccess(any(QueueACL.class), 
                              any(UserGroupInformation.class));
    
    // Some default values
    doReturn(100).when(queue).getMaxApplications();
    doReturn(25).when(queue).getMaxApplicationsPerUser();
    doReturn(10).when(queue).getMaximumActiveApplications();
    doReturn(2).when(queue).getMaximumActiveApplicationsPerUser();
  }
  
  private static final String A = "a";
  private static final String B = "b";
  private void setupQueueConfiguration(CapacitySchedulerConfiguration conf) {
    
    // Define top-level queues
    conf.setQueues(CapacityScheduler.ROOT, new String[] {A, B});
    conf.setCapacity(CapacityScheduler.ROOT, 100);
    
    final String Q_A = CapacityScheduler.ROOT + "." + A;
    conf.setCapacity(Q_A, 10);
    
    final String Q_B = CapacityScheduler.ROOT + "." + B;
    conf.setCapacity(Q_B, 90);
    
    LOG.info("Setup top-level queues a and b");
  }

  private SchedulerApp getMockApplication(int appId, String user) {
    SchedulerApp application = mock(SchedulerApp.class);
    ApplicationAttemptId applicationAttemptId =
        TestUtils.getMockApplicationAttemptId(appId, 0);
    doReturn(applicationAttemptId.getApplicationId()).
        when(application).getApplicationId();
    doReturn(applicationAttemptId). when(application).getApplicationAttemptId();
    doReturn(user).when(application).getUser();
    return application;
  }
  
  @Test
  public void testLimitsComputation() throws Exception {
    CapacitySchedulerConfiguration csConf = 
        new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);
    
    
    CapacitySchedulerContext csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getMinimumResourceCapability()).thenReturn(Resources.createResource(GB));
    when(csContext.getMaximumResourceCapability()).thenReturn(Resources.createResource(16*GB));
    
    // Say cluster has 100 nodes of 16G each
    Resource clusterResource = Resources.createResource(100 * 16 * GB);
    when(csContext.getClusterResources()).thenReturn(clusterResource);
    
    Map<String, Queue> queues = new HashMap<String, Queue>();
    Queue root = 
        CapacityScheduler.parseQueue(csContext, csConf, null, "root", 
            queues, queues, 
            CapacityScheduler.queueComparator, 
            CapacityScheduler.applicationComparator, 
            TestUtils.spyHook);

    LeafQueue queue = (LeafQueue)queues.get(A);
    
    LOG.info("Queue 'A' -" +
    		" maxActiveApplications=" + queue.getMaximumActiveApplications() + 
    		" maxActiveApplicationsPerUser=" + 
    		queue.getMaximumActiveApplicationsPerUser());
    int expectedMaxActiveApps = 
        Math.max(1, 
            (int)((clusterResource.getMemory() / LeafQueue.DEFAULT_AM_RESOURCE) * 
                   csConf.getMaximumApplicationMasterResourcePercent() *
                   queue.getAbsoluteCapacity()));
    assertEquals(expectedMaxActiveApps, 
                 queue.getMaximumActiveApplications());
    assertEquals((int)(expectedMaxActiveApps * (queue.getUserLimit() / 100.0f) * 
                       queue.getUserLimitFactor()), 
                 queue.getMaximumActiveApplicationsPerUser());
    
    // Add some nodes to the cluster & test new limits
    clusterResource = Resources.createResource(120 * 16 * GB);
    root.updateClusterResource(clusterResource);
    expectedMaxActiveApps = 
        Math.max(1, 
            (int)((clusterResource.getMemory() / LeafQueue.DEFAULT_AM_RESOURCE) * 
                   csConf.getMaximumApplicationMasterResourcePercent() *
                   queue.getAbsoluteCapacity()));
    assertEquals(expectedMaxActiveApps, 
                 queue.getMaximumActiveApplications());
    assertEquals((int)(expectedMaxActiveApps * (queue.getUserLimit() / 100.0f) * 
                       queue.getUserLimitFactor()), 
                 queue.getMaximumActiveApplicationsPerUser());
    
  }
  
  @Test
  public void testActiveApplicationLimits() throws Exception {
    final String user_0 = "user_0";
    final String user_1 = "user_1";
    
    int APPLICATION_ID = 0;
    // Submit first application
    SchedulerApp app_0 = getMockApplication(APPLICATION_ID++, user_0);
    queue.submitApplication(app_0, user_0, A);
    assertEquals(1, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(1, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));

    // Submit second application
    SchedulerApp app_1 = getMockApplication(APPLICATION_ID++, user_0);
    queue.submitApplication(app_1, user_0, A);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    
    // Submit third application, should remain pending
    SchedulerApp app_2 = getMockApplication(APPLICATION_ID++, user_0);
    queue.submitApplication(app_2, user_0, A);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    
    // Finish one application, app_2 should be activated
    queue.finishApplication(app_0, A);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(0, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(0, queue.getNumPendingApplications(user_0));
    
    // Submit another one for user_0
    SchedulerApp app_3 = getMockApplication(APPLICATION_ID++, user_0);
    queue.submitApplication(app_3, user_0, A);
    assertEquals(2, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    
    // Change queue limit to be smaller so 2 users can fill it up
    doReturn(3).when(queue).getMaximumActiveApplications();
    
    // Submit first app for user_1
    SchedulerApp app_4 = getMockApplication(APPLICATION_ID++, user_1);
    queue.submitApplication(app_4, user_1, A);
    assertEquals(3, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    assertEquals(1, queue.getNumActiveApplications(user_1));
    assertEquals(0, queue.getNumPendingApplications(user_1));

    // Submit second app for user_1, should block due to queue-limit
    SchedulerApp app_5 = getMockApplication(APPLICATION_ID++, user_1);
    queue.submitApplication(app_5, user_1, A);
    assertEquals(3, queue.getNumActiveApplications());
    assertEquals(2, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    assertEquals(1, queue.getNumActiveApplications(user_1));
    assertEquals(1, queue.getNumPendingApplications(user_1));

    // Now finish one app of user_1 so app_5 should be activated
    queue.finishApplication(app_4, A);
    assertEquals(3, queue.getNumActiveApplications());
    assertEquals(1, queue.getNumPendingApplications());
    assertEquals(2, queue.getNumActiveApplications(user_0));
    assertEquals(1, queue.getNumPendingApplications(user_0));
    assertEquals(1, queue.getNumActiveApplications(user_1));
    assertEquals(0, queue.getNumPendingApplications(user_1));
  }

  @After
  public void tearDown() {
  
  }
}
