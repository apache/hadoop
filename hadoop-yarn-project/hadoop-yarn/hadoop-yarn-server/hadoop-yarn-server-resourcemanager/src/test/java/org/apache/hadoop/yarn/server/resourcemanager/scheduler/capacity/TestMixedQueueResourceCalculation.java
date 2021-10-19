package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestMixedQueueResourceCalculation extends CapacitySchedulerQueueCalculationTestBase {

  private static final Resource UPDATE_RESOURCE = Resource.newInstance(16384, 16);

  @Test
  public void testMixedCapacitiesWithoutRemainingResource() throws IOException {
    setupQueueHierarchyWithoutRemainingResource();

    QueueAssertionBuilder assertionBuilder = createAssertionBuilder()
        .withQueue("root.a")
        .toExpect(Resource.newInstance(2486, 9))
        .assertEffectiveMinResource()
        .withQueue("root.a.a1")
        .toExpect(Resource.newInstance(621, 4))
        .assertEffectiveMinResource()
        .withQueue("root.a.a2")
        .toExpect(Resource.newInstance(1865, 5))
        .assertEffectiveMinResource()
        .withQueue("root.b")
        .toExpect(Resource.newInstance(8095, 3))
        .assertEffectiveMinResource()
        .withQueue("root.b.b1")
        .toExpect(Resource.newInstance(8095, 3))
        .assertEffectiveMinResource()
        .withQueue("root.c")
        .toExpect(Resource.newInstance(5803, 4))
        .assertEffectiveMinResource()
        .build();

    QueueHierarchyUpdateContext updateContext = update(assertionBuilder, UPDATE_RESOURCE);

    Assert.assertEquals("No warning should occur", 0,
        updateContext.getUpdateWarnings().size());
  }

  @Test
  public void testMixedCapacitiesWithWarnings() throws IOException {
    setupQueueHierarchyWithWarnings();
    QueueAssertionBuilder assertionBuilder = createAssertionBuilder();

    QueueHierarchyUpdateContext updateContext = update(assertionBuilder, UPDATE_RESOURCE);
    System.out.println("");

  }

  private void setupQueueHierarchyWithoutRemainingResource() throws IOException {
    csConf.setState("root.b", QueueState.STOPPED);
    cs.reinitialize(csConf, mockRM.getRMContext());

    csConf.setQueues("root", new String[]{"a", "b", "c"});
    csConf.setQueues("root.a", new String[]{"a1", "a2"});
    csConf.setQueues("root.b", new String[]{"b1"});

    csConf.setState("root.b", QueueState.RUNNING);
    csConf.setCapacityVector("root.a", "", "[memory=30%,vcores=6w]");
    csConf.setCapacityVector("root.a.a1", "", "[memory=1w,vcores=4]");
    csConf.setCapacityVector("root.a.a2", "", "[memory=3w,vcores=100%]");
    csConf.setCapacityVector("root.b", "", "[memory=8095,vcores=30%]");
    csConf.setCapacityVector("root.b.b1", "", "[memory=5w,vcores=3]");
    csConf.setCapacityVector("root.c", "", "[memory=3w,vcores=4]");

    cs.reinitialize(csConf, mockRM.getRMContext());
  }

  private void setupQueueHierarchyWithWarnings() throws IOException {
    csConf.setState("root.b", QueueState.STOPPED);
    cs.reinitialize(csConf, mockRM.getRMContext());

    csConf.setQueues("root", new String[]{"a", "b", "c"});
    csConf.setQueues("root.a", new String[]{"a1", "a2"});
    csConf.setQueues("root.b", new String[]{"b1"});

    csConf.setState("root.b", QueueState.RUNNING);
    csConf.setCapacityVector("root.a", "", "[memory=100%,vcores=6w]");
    csConf.setCapacityVector("root.a.a1", "", "[memory=2048,vcores=4]");
    csConf.setCapacityVector("root.a.a2", "", "[memory=2048,vcores=100%]");
    csConf.setCapacityVector("root.b", "", "[memory=8096,vcores=30%]");
    csConf.setCapacityVector("root.b.b1", "", "[memory=10256,vcores=3]");
    csConf.setCapacityVector("root.c", "", "[memory=3w,vcores=4]");

    cs.reinitialize(csConf, mockRM.getRMContext());
  }
}
