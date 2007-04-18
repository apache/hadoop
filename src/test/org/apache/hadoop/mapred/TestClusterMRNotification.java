package org.apache.hadoop.mapred;

import java.io.IOException;

/**
 * Tests Job end notification in cluster mode.
 *
 * @author Alejandro Abdelnur
 */
public class TestClusterMRNotification extends NotificationTestCase {

  public TestClusterMRNotification() throws IOException {
    super(HadoopTestCase.CLUSTER_MR);
  }

}
