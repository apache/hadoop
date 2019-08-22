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
package org.apache.hadoop.hdfs;

import java.net.URI;
import java.util.ArrayList;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferServer;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferTestCase;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_OVERWRITE_DOWNSTREAM_DERIVED_QOP_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SEND_QOP_ENABLED;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_AUXILIARY_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_ENCRYPT_DATA_OVERWRITE_DOWNSTREAM_NEW_QOP_KEY;
import static org.junit.Assert.*;


/**
 * This test tests access NameNode on different port with different
 * configured QOP.
 */
public class TestMultipleNNPortQOP extends SaslDataTransferTestCase {

  private static final Path PATH1  = new Path("/file1");
  private static final Path PATH2  = new Path("/file2");
  private static final Path PATH3  = new Path("/file3");
  private static final int BLOCK_SIZE = 4096;
  private static final int NUM_BLOCKS = 3;

  private static HdfsConfiguration clusterConf;

  @Before
  public void setup() throws Exception {
    clusterConf = createSecureConfig(
        "authentication,integrity,privacy");
    clusterConf.set(DFS_NAMENODE_RPC_ADDRESS_AUXILIARY_KEY,
        "12000,12100,12200");
    // explicitly setting service rpc for datanode. This because
    // DFSUtil.getNNServiceRpcAddressesForCluster looks up client facing port
    // and service port at the same time, and if no setting for service
    // rpc, it would return client port, in this case, it will be the
    // auxiliary port for data node. Which is not what auxiliary is for.
    // setting service rpc port to avoid this.
    clusterConf.set(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "localhost:9020");
    clusterConf.set(
        CommonConfigurationKeys.HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS,
        "org.apache.hadoop.security.IngressPortBasedResolver");
    clusterConf.set("ingress.port.sasl.configured.ports", "12000,12100,12200");
    clusterConf.set("ingress.port.sasl.prop.12000", "authentication");
    clusterConf.set("ingress.port.sasl.prop.12100", "integrity");
    clusterConf.set("ingress.port.sasl.prop.12200", "privacy");
    clusterConf.setBoolean(DFS_NAMENODE_SEND_QOP_ENABLED, true);
  }

  /**
   * Test accessing NameNode from three different ports.
   *
   * @throws Exception
   */
  @Test
  public void testMultipleNNPort() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(clusterConf)
          .numDataNodes(3).build();

      cluster.waitActive();
      HdfsConfiguration clientConf = new HdfsConfiguration(clusterConf);
      clientConf.unset(
          CommonConfigurationKeys.HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS);
      ArrayList<DataNode> dataNodes = cluster.getDataNodes();

      URI currentURI = cluster.getURI();
      URI uriAuthPort = new URI(currentURI.getScheme() +
          "://" + currentURI.getHost() + ":12000");
      URI uriIntegrityPort = new URI(currentURI.getScheme() +
          "://" + currentURI.getHost() + ":12100");
      URI uriPrivacyPort = new URI(currentURI.getScheme() +
          "://" + currentURI.getHost() + ":12200");

      clientConf.set(HADOOP_RPC_PROTECTION, "privacy");
      FileSystem fsPrivacy = FileSystem.get(uriPrivacyPort, clientConf);
      doTest(fsPrivacy, PATH1);
      for (DataNode dn : dataNodes) {
        SaslDataTransferServer saslServer = dn.getSaslServer();
        assertEquals("auth-conf", saslServer.getNegotiatedQOP());
      }

      clientConf.set(HADOOP_RPC_PROTECTION, "integrity");
      FileSystem fsIntegrity = FileSystem.get(uriIntegrityPort, clientConf);
      doTest(fsIntegrity, PATH2);
      for (DataNode dn : dataNodes) {
        SaslDataTransferServer saslServer = dn.getSaslServer();
        assertEquals("auth-int", saslServer.getNegotiatedQOP());
      }

      clientConf.set(HADOOP_RPC_PROTECTION, "authentication");
      FileSystem fsAuth = FileSystem.get(uriAuthPort, clientConf);
      doTest(fsAuth, PATH3);
      for (DataNode dn : dataNodes) {
        SaslDataTransferServer saslServer = dn.getSaslServer();
        assertEquals("auth", saslServer.getNegotiatedQOP());
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test accessing NameNode from three different ports, tests
   * overwriting downstream DN in the pipeline.
   *
   * @throws Exception
   */
  @Test
  public void testMultipleNNPortOverwriteDownStream() throws Exception {
    clusterConf.set(DFS_ENCRYPT_DATA_OVERWRITE_DOWNSTREAM_NEW_QOP_KEY, "auth");
    clusterConf.setBoolean(
        DFS_ENCRYPT_DATA_OVERWRITE_DOWNSTREAM_DERIVED_QOP_KEY, true);
    MiniDFSCluster cluster = null;
    try {
      cluster =
          new MiniDFSCluster.Builder(clusterConf).numDataNodes(3).build();
      cluster.waitActive();
      HdfsConfiguration clientConf = new HdfsConfiguration(clusterConf);
      clientConf.unset(
          CommonConfigurationKeys.HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS);
      ArrayList<DataNode> dataNodes = cluster.getDataNodes();

      URI currentURI = cluster.getURI();
      URI uriAuthPort =
          new URI(currentURI.getScheme() + "://" +
              currentURI.getHost() + ":12000");
      URI uriIntegrityPort =
          new URI(currentURI.getScheme() + "://" +
              currentURI.getHost() + ":12100");
      URI uriPrivacyPort =
          new URI(currentURI.getScheme() + "://" +
              currentURI.getHost() + ":12200");

      clientConf.set(HADOOP_RPC_PROTECTION, "privacy");
      FileSystem fsPrivacy = FileSystem.get(uriPrivacyPort, clientConf);
      doTest(fsPrivacy, PATH1);
      // add a wait so that data has reached not only first DN,
      // but also the rest
      Thread.sleep(100);
      for (int i = 0; i < 2; i++) {
        DataNode dn = dataNodes.get(i);
        SaslDataTransferClient saslClient = dn.getSaslClient();
        assertEquals("auth", saslClient.getTargetQOP());
      }

      clientConf.set(HADOOP_RPC_PROTECTION, "integrity");
      FileSystem fsIntegrity = FileSystem.get(uriIntegrityPort, clientConf);
      doTest(fsIntegrity, PATH2);
      Thread.sleep(100);
      for (int i = 0; i < 2; i++) {
        DataNode dn = dataNodes.get(i);
        SaslDataTransferClient saslClient = dn.getSaslClient();
        assertEquals("auth", saslClient.getTargetQOP());
      }

      clientConf.set(HADOOP_RPC_PROTECTION, "authentication");
      FileSystem fsAuth = FileSystem.get(uriAuthPort, clientConf);
      doTest(fsAuth, PATH3);
      Thread.sleep(100);
      for (int i = 0; i < 3; i++) {
        DataNode dn = dataNodes.get(i);
        SaslDataTransferServer saslServer = dn.getSaslServer();
        assertEquals("auth", saslServer.getNegotiatedQOP());
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void doTest(FileSystem fs, Path path) throws Exception {
    FileSystemTestHelper.createFile(fs, path, NUM_BLOCKS, BLOCK_SIZE);
    assertArrayEquals(FileSystemTestHelper.getFileData(NUM_BLOCKS, BLOCK_SIZE),
        DFSTestUtil.readFile(fs, path).getBytes("UTF-8"));
    BlockLocation[] blockLocations = fs.getFileBlockLocations(path, 0,
        Long.MAX_VALUE);
    assertNotNull(blockLocations);
    assertEquals(NUM_BLOCKS, blockLocations.length);
    for (BlockLocation blockLocation: blockLocations) {
      assertNotNull(blockLocation.getHosts());
      assertEquals(3, blockLocation.getHosts().length);
    }
  }
}
