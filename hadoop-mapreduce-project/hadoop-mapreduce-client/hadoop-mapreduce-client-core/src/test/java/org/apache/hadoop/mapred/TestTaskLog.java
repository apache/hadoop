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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.AfterClass;
import org.junit.Test;

/**
 * TestCounters checks the sanity and recoverability of Queue
 */
public class TestTaskLog {

  private static final String testDirName = TestTaskLog.class.getSimpleName();
  private static final String testDir = System.getProperty("test.build.data",
      "target" + File.separatorChar + "test-dir")
      + File.separatorChar + testDirName;

  @AfterClass
  public static void cleanup() {
    FileUtil.fullyDelete(new File(testDir));
  }

  /**
   * test TaskAttemptID
   * 
   * @throws IOException
   */
  @Test (timeout=50000)
  public void testTaskLog() throws IOException {
    // test TaskLog
    System.setProperty(
        YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR, "testString");
    assertThat(TaskLog.getMRv2LogDir()).isEqualTo("testString");
    TaskAttemptID taid = mock(TaskAttemptID.class);
    JobID jid = new JobID("job", 1);

    when(taid.getJobID()).thenReturn(jid);
    when(taid.toString()).thenReturn("JobId");

    File f = TaskLog.getTaskLogFile(taid, true, LogName.STDOUT);
    assertTrue(f.getAbsolutePath().endsWith("testString"
        + File.separatorChar + "stdout"));

    // test getRealTaskLogFileLocation

    File indexFile = TaskLog.getIndexFile(taid, true);
    if (!indexFile.getParentFile().exists()) {
      indexFile.getParentFile().mkdirs();
    }
    indexFile.delete();
    indexFile.createNewFile();

    TaskLog.syncLogs(testDir, taid, true);

    assertTrue(indexFile.getAbsolutePath().endsWith(
        "userlogs" + File.separatorChar + "job_job_0001"
        + File.separatorChar + "JobId.cleanup"
        + File.separatorChar + "log.index"));

    f = TaskLog.getRealTaskLogFileLocation(taid, true, LogName.DEBUGOUT);
    if (f != null) {
      assertTrue(f.getAbsolutePath().endsWith(testDirName
          + File.separatorChar + "debugout"));
      FileUtils.copyFile(indexFile, f);
    }
    // test obtainLogDirOwner
    assertTrue(TaskLog.obtainLogDirOwner(taid).length() > 0);
    // test TaskLog.Reader
    assertTrue(readTaskLog(TaskLog.LogName.DEBUGOUT, taid, true).length() > 0);
  }

  private String readTaskLog(TaskLog.LogName filter,
      org.apache.hadoop.mapred.TaskAttemptID taskId, boolean isCleanup)
      throws IOException {
    // string buffer to store task log
    StringBuilder result = new StringBuilder();
    int res;

    // reads the whole tasklog into inputstream
    InputStream taskLogReader = new TaskLog.Reader(taskId, filter, 0, -1,
        isCleanup);
    // construct string log from inputstream.
    byte[] b = new byte[65536];
    while (true) {
      res = taskLogReader.read(b);
      if (res > 0) {
        result.append(new String(b));
      } else {
        break;
      }
    }
    taskLogReader.close();

    // trim the string and return it
    String str = result.toString();
    str = str.trim();
    return str;
  }

  /**
   * test without TASK_LOG_DIR
   * 
   * @throws IOException
   */
  @Test (timeout=50000)
  public void testTaskLogWithoutTaskLogDir() throws IOException {
    // TaskLog tasklog= new TaskLog();
    System.clearProperty(YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR);

    // test TaskLog

    assertThat(TaskLog.getMRv2LogDir()).isNull();
    TaskAttemptID taid = mock(TaskAttemptID.class);
    JobID jid = new JobID("job", 1);

    when(taid.getJobID()).thenReturn(jid);
    when(taid.toString()).thenReturn("JobId");

    File f = TaskLog.getTaskLogFile(taid, true, LogName.STDOUT);
    assertTrue(f.getAbsolutePath().endsWith("stdout"));

  }

}
