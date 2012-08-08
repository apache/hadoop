package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerResourceRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceLocalizedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceReleaseEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceRequestEvent;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Test;

public class TestLocalResourcesTrackerImpl {

  @Test
  @SuppressWarnings("unchecked")
  public void test() {
    String user = "testuser";
    DrainDispatcher dispatcher = null;
    try {
      dispatcher = createDispatcher(new Configuration());
      EventHandler<LocalizerEvent> localizerEventHandler =
          mock(EventHandler.class);
      EventHandler<LocalizerEvent> containerEventHandler =
          mock(EventHandler.class);
      dispatcher.register(LocalizerEventType.class, localizerEventHandler);
      dispatcher.register(ContainerEventType.class, containerEventHandler);

      DeletionService mockDelService = mock(DeletionService.class);

      ContainerId cId1 = BuilderUtils.newContainerId(1, 1, 1, 1);
      LocalizerContext lc1 = new LocalizerContext(user, cId1, null);
      ContainerId cId2 = BuilderUtils.newContainerId(1, 1, 1, 2);
      LocalizerContext lc2 = new LocalizerContext(user, cId2, null);

      LocalResourceRequest req1 =
          createLocalResourceRequest(user, 1, 1, LocalResourceVisibility.PUBLIC);
      LocalResourceRequest req2 =
          createLocalResourceRequest(user, 2, 1, LocalResourceVisibility.PUBLIC);
      LocalizedResource lr1 = createLocalizedResource(req1, dispatcher);
      LocalizedResource lr2 = createLocalizedResource(req2, dispatcher);
      ConcurrentMap<LocalResourceRequest, LocalizedResource> localrsrc =
          new ConcurrentHashMap<LocalResourceRequest, LocalizedResource>();
      localrsrc.put(req1, lr1);
      localrsrc.put(req2, lr2);
      LocalResourcesTracker tracker =
          new LocalResourcesTrackerImpl(user, dispatcher, localrsrc);

      ResourceEvent req11Event =
          new ResourceRequestEvent(req1, LocalResourceVisibility.PUBLIC, lc1);
      ResourceEvent req12Event =
          new ResourceRequestEvent(req1, LocalResourceVisibility.PUBLIC, lc2);
      ResourceEvent req21Event =
          new ResourceRequestEvent(req2, LocalResourceVisibility.PUBLIC, lc1);

      ResourceEvent rel11Event = new ResourceReleaseEvent(req1, cId1);
      ResourceEvent rel12Event = new ResourceReleaseEvent(req1, cId2);
      ResourceEvent rel21Event = new ResourceReleaseEvent(req2, cId1);

      // Localize R1 for C1
      tracker.handle(req11Event);

      // Localize R1 for C2
      tracker.handle(req12Event);

      // Localize R2 for C1
      tracker.handle(req21Event);

      dispatcher.await();
      verify(localizerEventHandler, times(3)).handle(
          any(LocalizerResourceRequestEvent.class));
      // Verify refCount for R1 is 2
      Assert.assertEquals(2, lr1.getRefCount());
      // Verify refCount for R2 is 1
      Assert.assertEquals(1, lr2.getRefCount());

      // Release R2 for C1
      tracker.handle(rel21Event);

      dispatcher.await();
      verifyTrackedResourceCount(tracker, 2);

      // Verify resources in state INIT with ref-count=0 is removed.
      Assert.assertTrue(tracker.remove(lr2, mockDelService));
      verifyTrackedResourceCount(tracker, 1);

      // Verify resource with non zero ref count is not removed.
      Assert.assertEquals(2, lr1.getRefCount());
      Assert.assertFalse(tracker.remove(lr1, mockDelService));
      verifyTrackedResourceCount(tracker, 1);

      // Localize resource1
      ResourceLocalizedEvent rle =
          new ResourceLocalizedEvent(req1, new Path("file:///tmp/r1"), 1);
      lr1.handle(rle);
      Assert.assertTrue(lr1.getState().equals(ResourceState.LOCALIZED));

      // Release resource1
      tracker.handle(rel11Event);
      tracker.handle(rel12Event);
      Assert.assertEquals(0, lr1.getRefCount());

      // Verify resources in state LOCALIZED with ref-count=0 is removed.
      Assert.assertTrue(tracker.remove(lr1, mockDelService));
      verifyTrackedResourceCount(tracker, 0);
    } finally {
      if (dispatcher != null) {
        dispatcher.stop();
      }
    }
  }

  private void verifyTrackedResourceCount(LocalResourcesTracker tracker,
      int expected) {
    int count = 0;
    Iterator<LocalizedResource> iter = tracker.iterator();
    while (iter.hasNext()) {
      iter.next();
      count++;
    }
    Assert.assertEquals("Tracker resource count does not match", expected,
        count);
  }

  private LocalResourceRequest createLocalResourceRequest(String user, int i,
      long ts, LocalResourceVisibility vis) {
    final LocalResourceRequest req =
        new LocalResourceRequest(new Path("file:///tmp/" + user + "/rsrc" + i),
            ts + i * 2000, LocalResourceType.FILE, vis);
    return req;
  }

  private LocalizedResource createLocalizedResource(LocalResourceRequest req,
      Dispatcher dispatcher) {
    LocalizedResource lr = new LocalizedResource(req, dispatcher);
    return lr;
  }

  private DrainDispatcher createDispatcher(Configuration conf) {
    DrainDispatcher dispatcher = new DrainDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    return dispatcher;
  }
}
