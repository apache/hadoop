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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.AuthorizationContext;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestAuthorizationHeaderPropagation {

  public static class HeaderCapturingAuditLogger implements AuditLogger {
    public static final List<byte[]> capturedHeaders = new ArrayList<>();
    @Override
    public void initialize(Configuration conf) {}
    @Override
    public void logAuditEvent(boolean succeeded, String userName, InetAddress addr,
                              String cmd, String src, String dst, FileStatus stat) {
      byte[] header = AuthorizationContext.getCurrentAuthorizationHeader();
      capturedHeaders.add(header == null ? null : Arrays.copyOf(header, header.length));
    }
  }

  @Test
  public void testAuthorizationHeaderPerRpc() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFS_NAMENODE_AUDIT_LOGGERS_KEY, HeaderCapturingAuditLogger.class.getName());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitClusterUp();
      HeaderCapturingAuditLogger.capturedHeaders.clear();
      FileSystem fs = cluster.getFileSystem();
      // First RPC with header1
      byte[] header1 = "header-one".getBytes();
      AuthorizationContext.setCurrentAuthorizationHeader(header1);
      fs.mkdirs(new Path("/authz1"));
      AuthorizationContext.clear();
      // Second RPC with header2
      byte[] header2 = "header-two".getBytes();
      AuthorizationContext.setCurrentAuthorizationHeader(header2);
      fs.mkdirs(new Path("/authz2"));
      AuthorizationContext.clear();
      // Third RPC with no header
      fs.mkdirs(new Path("/authz3"));
      // Now assert
      assertArrayEquals(header1, HeaderCapturingAuditLogger.capturedHeaders.get(0));
      assertArrayEquals(header2, HeaderCapturingAuditLogger.capturedHeaders.get(1));
      assertNull(HeaderCapturingAuditLogger.capturedHeaders.get(2));
    } finally {
      cluster.shutdown();
    }
  }
}