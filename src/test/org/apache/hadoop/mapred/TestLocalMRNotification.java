package org.apache.hadoop.mapred;

import java.io.IOException;


/**
 * Tests Job end notification in local mode.
 */
public class TestLocalMRNotification extends NotificationTestCase {

  public TestLocalMRNotification() throws IOException {
    super(HadoopTestCase.LOCAL_MR);
  }

}
