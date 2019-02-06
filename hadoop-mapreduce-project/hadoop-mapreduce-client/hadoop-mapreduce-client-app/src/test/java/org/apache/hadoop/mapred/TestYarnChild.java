/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ClusterStorageCapacityExceededException;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

/**
 * Tests the behavior of YarnChild.
 */
public class TestYarnChild {
  private Task task;
  private TaskUmbilicalProtocol umbilical;
  private Configuration conf;
  final static private String KILL_LIMIT_EXCEED_CONF_NAME =
      "mapreduce.job.dfs.storage.capacity.kill-limit-exceed";

  @Before
  public void setUp() throws Exception {
    task = mock(Task.class);
    umbilical = mock(TaskUmbilicalProtocol.class);
    conf = new Configuration();
    when(task.getConf()).thenReturn(conf);
  }

  @Test
  public void testReportErrorWhenCapacityExceptionNotHappenByDefault()
      throws IOException {
    Exception exception = new RuntimeException(new IOException());

    verifyReportError(exception, false);
  }

  @Test
  public void testReportErrorWhenCapacityExceptionNotHappenAndFastFailDisabled()
      throws IOException {
    Exception exception = new RuntimeException(new IOException());
    conf.setBoolean(KILL_LIMIT_EXCEED_CONF_NAME, false);

    verifyReportError(exception, false);
  }

  @Test
  public void testReportErrorWhenCapacityExceptionNotHappenAndFastFailEnabled()
      throws IOException {
    Exception exception = new RuntimeException(new IOException());
    conf.setBoolean(KILL_LIMIT_EXCEED_CONF_NAME, true);

    verifyReportError(exception, false);
  }

  @Test
  public void testReportErrorWhenCapacityExceptionHappenByDefault()
      throws IOException {
    Exception exception =
        new RuntimeException(new ClusterStorageCapacityExceededException());

    verifyReportError(exception, false);
  }

  @Test
  public void testReportErrorWhenCapacityExceptionHappenAndFastFailDisabled()
      throws IOException {
    Exception exception =
        new RuntimeException(new ClusterStorageCapacityExceededException());
    conf.setBoolean(KILL_LIMIT_EXCEED_CONF_NAME, false);

    verifyReportError(exception, false);
  }

  @Test
  public void testReportErrorWhenCapacityExceptionHappenAndFastFailEnabled()
      throws IOException {
    Exception exception =
        new RuntimeException(new ClusterStorageCapacityExceededException());
    conf.setBoolean(KILL_LIMIT_EXCEED_CONF_NAME, true);

    verifyReportError(exception, true);
  }

  @Test
  public void testReportErrorWhenCapacityExceptionHappenInThirdOfExceptionChain()
      throws IOException {
    Exception exception = new RuntimeException(new IllegalStateException(
        new ClusterStorageCapacityExceededException()));
    conf.setBoolean(KILL_LIMIT_EXCEED_CONF_NAME, true);

    verifyReportError(exception, true);
  }

  private void verifyReportError(Exception exception, boolean fastFail)
      throws IOException {
    YarnChild.reportError(exception, task, umbilical);
    verify(umbilical).fatalError(any(), anyString(),
        eq(fastFail));
  }
}
