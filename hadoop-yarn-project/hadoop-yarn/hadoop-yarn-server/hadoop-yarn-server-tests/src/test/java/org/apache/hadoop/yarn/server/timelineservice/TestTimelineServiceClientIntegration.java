package org.apache.hadoop.yarn.server.timelineservice;


import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.aggregator.PerNodeTimelineAggregatorsAuxService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.fail;

public class TestTimelineServiceClientIntegration {
  private static PerNodeTimelineAggregatorsAuxService auxService;

  @BeforeClass
  public static void setupClass() throws Exception {
    try {
      auxService = PerNodeTimelineAggregatorsAuxService.launchServer(new String[0]);
      auxService.addApplication(ApplicationId.newInstance(0, 1));
    } catch (ExitUtil.ExitException e) {
      fail();
    }
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (auxService != null) {
      auxService.stop();
    }
  }

  @Test
  public void testPutEntities() throws Exception {
    TimelineClient client =
        TimelineClient.createTimelineClient(ApplicationId.newInstance(0, 1));
    try {
      client.init(new YarnConfiguration());
      client.start();
      TimelineEntity entity = new TimelineEntity();
      entity.setType("test entity type");
      entity.setId("test entity id");
      client.putEntities(entity);
      client.putEntitiesAsync(entity);
    } catch(Exception e) {
      fail();
    } finally {
      client.stop();
    }
  }
}
