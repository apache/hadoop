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

package org.apache.hadoop.mapreduce.v2.hs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import org.junit.Assert;
import org.junit.Test;


/**
 * Test service initialization of HistoryFileManager when
 * HDFS is not running normally (either in start phase or
 * in safe mode).
 */
public class TestHistoryFileManagerInitWithNonRunningDFS {
  private static final String CLUSTER_BASE_DIR =
      MiniDFSCluster.getBaseDirectory();

  /**
   * Verify if JHS keeps retrying to connect to HDFS, if the name node is
   * in safe mode, when it creates history directories during service
   * initialization. The expected behavior of JHS is to keep retrying for
   * a time limit as specified by
   * JHAdminConfig.MR_HISTORY_MAX_START_WAIT_TIME, and give up by throwing
   * a YarnRuntimeException with a time out message.
   */
  @Test
  public void testKeepRetryingWhileNameNodeInSafeMode() throws Exception {
    Configuration conf = new Configuration();
    // set maximum wait time for JHS to wait for HDFS NameNode to start running
    final long maxJhsWaitTime = 500;
    conf.setLong(JHAdminConfig.MR_HISTORY_MAX_START_WAIT_TIME, maxJhsWaitTime);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, CLUSTER_BASE_DIR);

    MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(conf).build();
    try {
      // set up a cluster with its name node in safe mode
      dfsCluster.getFileSystem().setSafeMode(
          SafeModeAction.ENTER);
      Assert.assertTrue(dfsCluster.getFileSystem().isInSafeMode());

      HistoryFileManager hfm = new HistoryFileManager();
      hfm.serviceInit(conf);
      Assert.fail("History File Manager did not retry to connect to name node");
    } catch (YarnRuntimeException yex) {
      String expectedExceptionMsg = "Timed out '" + maxJhsWaitTime +
          "ms' waiting for FileSystem to become available";
      Assert.assertEquals("Unexpected reconnect timeout exception message",
          expectedExceptionMsg, yex.getMessage());
    } finally {
      dfsCluster.shutdown(true);
    }
  }
}
