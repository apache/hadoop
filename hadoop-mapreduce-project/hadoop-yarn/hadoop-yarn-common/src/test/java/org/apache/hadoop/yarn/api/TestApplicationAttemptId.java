package org.apache.hadoop.yarn.api;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;

public class TestApplicationAttemptId {

  @Test
  public void testApplicationAttemptId() {
    ApplicationAttemptId a1 = createAppAttemptId(10l, 1, 1);
    ApplicationAttemptId a2 = createAppAttemptId(10l, 1, 2);
    ApplicationAttemptId a3 = createAppAttemptId(10l, 2, 1);
    ApplicationAttemptId a4 = createAppAttemptId(8l, 1, 4);
    ApplicationAttemptId a5 = createAppAttemptId(10l, 1, 1);
    
    Assert.assertTrue(a1.equals(a5));
    Assert.assertFalse(a1.equals(a2));
    Assert.assertFalse(a1.equals(a3));
    Assert.assertFalse(a1.equals(a4));
    
    Assert.assertTrue(a1.compareTo(a5) == 0);
    Assert.assertTrue(a1.compareTo(a2) < 0);
    Assert.assertTrue(a1.compareTo(a3) < 0);
    Assert.assertTrue(a1.compareTo(a4) > 0);
    
    Assert.assertTrue(a1.hashCode() == a5.hashCode());
    Assert.assertFalse(a1.hashCode() == a2.hashCode());
    Assert.assertFalse(a1.hashCode() == a3.hashCode());
    Assert.assertFalse(a1.hashCode() == a4.hashCode());
    
  }

  private ApplicationAttemptId createAppAttemptId(long clusterTimeStamp,
      int id, int attemptId) {
    ApplicationAttemptId appAttemptId =
        Records.newRecord(ApplicationAttemptId.class);
    ApplicationId appId = Records.newRecord(ApplicationId.class);
    appId.setClusterTimestamp(clusterTimeStamp);
    appId.setId(id);
    appAttemptId.setApplicationId(appId);
    appAttemptId.setAttemptId(attemptId);
    return appAttemptId;
  }
}
