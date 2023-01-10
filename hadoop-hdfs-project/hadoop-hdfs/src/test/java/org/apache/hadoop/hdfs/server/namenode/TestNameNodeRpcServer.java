/*
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

/*
 * Test the MiniDFSCluster functionality that allows "dfs.datanode.address",
 * "dfs.datanode.http.address", and "dfs.datanode.ipc.address" to be
 * configurable. The MiniDFSCluster.startDataNodes() API now has a parameter
 * that will check these properties if told to do so.
 */
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_IP_PROXY_USERS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_BIND_HOST_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.ObserverRetryOnActiveException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Test;
import org.junit.jupiter.api.Timeout;

public class TestNameNodeRpcServer {

  @Test
  public void testNamenodeRpcBindAny() throws IOException {
    Configuration conf = new HdfsConfiguration();

    // The name node in MiniDFSCluster only binds to 127.0.0.1.
    // We can set the bind address to 0.0.0.0 to make it listen
    // to all interfaces.
    conf.set(DFS_NAMENODE_RPC_BIND_HOST_KEY, "0.0.0.0");
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      assertEquals("0.0.0.0", ((NameNodeRpcServer)cluster.getNameNodeRpc())
          .getClientRpcServer().getListenerAddress().getHostName());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      // Reset the config
      conf.unset(DFS_NAMENODE_RPC_BIND_HOST_KEY);
    }
  }

  /**
   * Get the preferred DataNode location for the first block of the
   * given file.
   * @param fs The file system to use
   * @param p The path to use
   * @return the preferred host to get the data
   */
  private static String getPreferredLocation(DistributedFileSystem fs,
                                             Path p) throws IOException{
    // Use getLocatedBlocks because it is the basis for HDFS open,
    // but provides visibility into which host will be used.
    LocatedBlocks blocks = fs.getClient()
        .getLocatedBlocks(p.toUri().getPath(), 0);
    return blocks.get(0).getLocations()[0].getHostName();
  }

  // Because of the randomness of the NN assigning DN, we run multiple
  // trials. 1/3^20=3e-10, so that should be good enough.
  static final int ITERATIONS_TO_USE = 20;

  @Test
  @Timeout(30000)
  public void testNamenodeRpcClientIpProxyWithFailBack() throws Exception {
    // Make 3 nodes & racks so that we have a decent shot of detecting when
    // our change overrides the random choice of datanode.
    Configuration conf = new HdfsConfiguration();
    conf.set(DFS_NAMENODE_IP_PROXY_USERS, "fake_joe");
    final CallerContext original = CallerContext.getCurrent();

    MiniQJMHACluster qjmhaCluster = null;
    try {
      String baseDir = GenericTestUtils.getRandomizedTempPath();
      MiniQJMHACluster.Builder builder = new MiniQJMHACluster.Builder(conf);
      builder.getDfsBuilder().numDataNodes(3);
      qjmhaCluster = builder.baseDir(baseDir).build();
      MiniDFSCluster dfsCluster = qjmhaCluster.getDfsCluster();
      dfsCluster.waitActive();
      dfsCluster.transitionToActive(0);

      // Set the caller context to set the ip address
      CallerContext.setCurrent(
          new CallerContext.Builder("test", conf)
              .build());

      dfsCluster.getFileSystem(0).setPermission(
          new Path("/"), FsPermission.getDirDefault());

      // Run as fake joe to authorize the test
      UserGroupInformation joe =
          UserGroupInformation.createUserForTesting("fake_joe",
              new String[]{"fake_group"});

      FileSystem joeFs = joe.doAs((PrivilegedExceptionAction<FileSystem>) () ->
          FileSystem.get(dfsCluster.getURI(0), conf));

      Path testPath = new Path("/foo");
      // Write a sample file
      FSDataOutputStream stream = joeFs.create(testPath);
      stream.write("Hello world!\n".getBytes(StandardCharsets.UTF_8));
      stream.close();

      qjmhaCluster.getDfsCluster().transitionToStandby(0);
      qjmhaCluster.getDfsCluster().transitionToActive(1);

      DistributedFileSystem nn1 = dfsCluster.getFileSystem(1);
      assertNotNull(nn1.getFileStatus(testPath));
    } finally {
      CallerContext.setCurrent(original);
      if (qjmhaCluster != null) {
        try {
          qjmhaCluster.shutdown();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      // Reset the config
      conf.unset(DFS_NAMENODE_IP_PROXY_USERS);
    }
  }

  @Test
  @Timeout(30000)
  public void testObserverHandleAddBlock() throws Exception {
    String baseDir = GenericTestUtils.getRandomizedTempPath();
    Configuration conf = new HdfsConfiguration();
    MiniQJMHACluster.Builder builder = new MiniQJMHACluster.Builder(conf).setNumNameNodes(3);
    builder.getDfsBuilder().numDataNodes(3);
    try (MiniQJMHACluster qjmhaCluster = builder.baseDir(baseDir).build()) {
      MiniDFSCluster dfsCluster = qjmhaCluster.getDfsCluster();
      dfsCluster.waitActive();
      dfsCluster.transitionToActive(0);
      dfsCluster.transitionToObserver(2);

      NameNode activeNN = dfsCluster.getNameNode(0);
      NameNode observerNN = dfsCluster.getNameNode(2);

      // Stop the editLogTailer of Observer NameNode
      observerNN.getNamesystem().getEditLogTailer().stop();
      DistributedFileSystem dfs = dfsCluster.getFileSystem(0);

      Path testPath = new Path("/testObserverHandleAddBlock/file.txt");
      try (FSDataOutputStream ignore = dfs.create(testPath)) {
        HdfsFileStatus fileStatus = activeNN.getRpcServer().getFileInfo(testPath.toUri().getPath());
        assertNotNull(fileStatus);
        assertNull(observerNN.getRpcServer().getFileInfo(testPath.toUri().getPath()));

        LambdaTestUtils.intercept(ObserverRetryOnActiveException.class, () -> {
          observerNN.getRpcServer().addBlock(testPath.toUri().getPath(),
              dfs.getClient().getClientName(), null, null,
              fileStatus.getFileId(), null, EnumSet.noneOf(AddBlockFlag.class));
        });
      } finally {
        dfs.delete(testPath, true);
      }
    }
  }

  /**
   * A test to make sure that if an authorized user adds "clientIp:" to their
   * caller context, it will be used to make locality decisions on the NN.
   */
  @Test
  public void testNamenodeRpcClientIpProxy() throws IOException {
    Configuration conf = new HdfsConfiguration();

    conf.set(DFS_NAMENODE_IP_PROXY_USERS, "fake_joe");
    // Make 3 nodes & racks so that we have a decent shot of detecting when
    // our change overrides the random choice of datanode.
    final String[] racks = new String[]{"/rack1", "/rack2", "/rack3"};
    final String[] hosts = new String[]{"node1", "node2", "node3"};
    MiniDFSCluster cluster = null;
    final CallerContext original = CallerContext.getCurrent();

    try {
      cluster = new MiniDFSCluster.Builder(conf)
          .racks(racks).hosts(hosts).numDataNodes(hosts.length)
          .build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      // Write a sample file
      final Path fooName = fs.makeQualified(new Path("/foo"));
      FSDataOutputStream stream = fs.create(fooName);
      stream.write("Hello world!\n".getBytes(StandardCharsets.UTF_8));
      stream.close();
      // Set the caller context to set the ip address
      CallerContext.setCurrent(
          new CallerContext.Builder("test", conf)
              .append(CallerContext.CLIENT_IP_STR, hosts[0])
              .build());
      // Should get a random mix of DataNodes since we aren't joe.
      for (int trial = 0; trial < ITERATIONS_TO_USE; ++trial) {
        String host = getPreferredLocation(fs, fooName);
        if (!hosts[0].equals(host)) {
          // found some other host, so things are good
          break;
        } else if (trial == ITERATIONS_TO_USE - 1) {
          assertNotEquals("Failed to get non-node1", hosts[0], host);
        }
      }
      // Run as fake joe to authorize the test
      UserGroupInformation joe =
          UserGroupInformation.createUserForTesting("fake_joe",
              new String[]{"fake_group"});
      DistributedFileSystem joeFs =
          (DistributedFileSystem) DFSTestUtil.getFileSystemAs(joe, conf);
      // As joe, we should get all node1.
      for (int trial = 0; trial < ITERATIONS_TO_USE; ++trial) {
        String host = getPreferredLocation(joeFs, fooName);
        assertEquals("Trial " + trial + " failed", hosts[0], host);
      }
    } finally {
      CallerContext.setCurrent(original);
      if (cluster != null) {
        cluster.shutdown();
      }
      // Reset the config
      conf.unset(DFS_NAMENODE_IP_PROXY_USERS);
    }
  }
}

