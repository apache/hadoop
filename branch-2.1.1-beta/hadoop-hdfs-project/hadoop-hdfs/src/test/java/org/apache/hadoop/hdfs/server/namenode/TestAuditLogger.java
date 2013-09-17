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

package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

/**
 * Tests for the {@link AuditLogger} custom audit logging interface.
 */
public class TestAuditLogger {

  /**
   * Tests that AuditLogger works as expected.
   */
  @Test
  public void testAuditLogger() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFS_NAMENODE_AUDIT_LOGGERS_KEY,
        DummyAuditLogger.class.getName());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();

    try {
      cluster.waitClusterUp();
      assertTrue(DummyAuditLogger.initialized);

      FileSystem fs = cluster.getFileSystem();
      long time = System.currentTimeMillis();
      fs.setTimes(new Path("/"), time, time);
      assertEquals(1, DummyAuditLogger.logCount);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Tests that a broken audit logger causes requests to fail.
   */
  @Test
  public void testBrokenLogger() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFS_NAMENODE_AUDIT_LOGGERS_KEY,
        BrokenAuditLogger.class.getName());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();

    try {
      cluster.waitClusterUp();

      FileSystem fs = cluster.getFileSystem();
      long time = System.currentTimeMillis();
      fs.setTimes(new Path("/"), time, time);
      fail("Expected exception due to broken audit logger.");
    } catch (RemoteException re) {
      // Expected.
    } finally {
      cluster.shutdown();
    }
  }

  public static class DummyAuditLogger implements AuditLogger {

    static boolean initialized;
    static int logCount;

    public void initialize(Configuration conf) {
      initialized = true;
    }

    public void logAuditEvent(boolean succeeded, String userName,
        InetAddress addr, String cmd, String src, String dst,
        FileStatus stat) {
      logCount++;
    }

  }

  public static class BrokenAuditLogger implements AuditLogger {

    public void initialize(Configuration conf) {
      // No op.
    }

    public void logAuditEvent(boolean succeeded, String userName,
        InetAddress addr, String cmd, String src, String dst,
        FileStatus stat) {
      throw new RuntimeException("uh oh");
    }

  }

}
