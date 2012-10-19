package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFSQueueSchedulable {
  private FSQueueSchedulable schedulable = null;
  private Resource maxResource = Resources.createResource(10);

  @Before
  public void setup() {
    String queueName = "testFSQueue";
    FSQueue mockQueue = mock(FSQueue.class);
    when(mockQueue.getName()).thenReturn(queueName);

    QueueManager mockMgr = mock(QueueManager.class);
    when(mockMgr.getMaxResources(queueName)).thenReturn(maxResource);

    schedulable = new FSQueueSchedulable(null, mockQueue, mockMgr, null, 0, 0);
  }

  @Test
  public void testUpdateDemand() {
    AppSchedulable app = mock(AppSchedulable.class);
    Mockito.when(app.getDemand()).thenReturn(maxResource);

    schedulable.addApp(app);
    schedulable.addApp(app);

    schedulable.updateDemand();

    assertTrue("Demand is greater than max allowed ",
        Resources.equals(schedulable.getDemand(), maxResource));
  }
}
