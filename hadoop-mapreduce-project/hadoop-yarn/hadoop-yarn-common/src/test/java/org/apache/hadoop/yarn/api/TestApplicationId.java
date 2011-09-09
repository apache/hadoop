package org.apache.hadoop.yarn.api;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;

public class TestApplicationId {

  @Test
  public void testApplicationId() {
    ApplicationId a1 = createAppId(10l, 1);
    ApplicationId a2 = createAppId(10l, 2);
    ApplicationId a3 = createAppId(10l, 1);
    ApplicationId a4 = createAppId(8l, 3);

    Assert.assertFalse(a1.equals(a2));
    Assert.assertFalse(a1.equals(a4));
    Assert.assertTrue(a1.equals(a3));

    Assert.assertTrue(a1.compareTo(a2) < 0);
    Assert.assertTrue(a1.compareTo(a3) == 0);
    Assert.assertTrue(a1.compareTo(a4) > 0);

    Assert.assertTrue(a1.hashCode() == a3.hashCode());
    Assert.assertFalse(a1.hashCode() == a2.hashCode());
    Assert.assertFalse(a2.hashCode() == a4.hashCode());
  }

  private ApplicationId createAppId(long clusterTimeStamp, int id) {
    ApplicationId appId = Records.newRecord(ApplicationId.class);
    appId.setClusterTimestamp(clusterTimeStamp);
    appId.setId(id);
    return appId;
  }
}