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
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferTestCase;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.security.TestPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.junit.Assert.*;


/**
 * This tests enabling NN sending the established QOP back to client,
 * in encrypted message, using block access token key.
 */
@RunWith(Parameterized.class)
public class TestBlockTokenWrappingQOP extends SaslDataTransferTestCase {
  public static final Log LOG = LogFactory.getLog(TestPermission.class);

  private HdfsConfiguration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;

  private String configKey;
  private String qopValue;

  @Parameterized.Parameters
  public static Collection<Object[]> qopSettings() {
    // if configured with privacy, the negotiated QOP should auth-conf
    // similarly for the other two
    return Arrays.asList(new Object[][] {
        {"privacy", "auth-conf"},
        {"integrity", "auth-int"},
        {"authentication", "auth"}
    });
  }

  public TestBlockTokenWrappingQOP(String configKey, String qopValue) {
    this.configKey = configKey;
    this.qopValue = qopValue;
  }

  @Before
  public void setup() throws Exception {
    conf = createSecureConfig(this.configKey);
    conf.set(DFS_NAMENODE_RPC_ADDRESS_AUXILIARY_KEY, "12000");
    // explicitly setting service rpc for datanode. This because
    // DFSUtil.getNNServiceRpcAddressesForCluster looks up client facing port
    // and service port at the same time, and if no setting for service
    // rpc, it would return client port, in this case, it will be the
    // auxiliary port for data node. Which is not what auxiliary is for.
    // setting service rpc port to avoid this.
    conf.set(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "localhost:9020");
    conf.set(
        CommonConfigurationKeys.HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS,
        "org.apache.hadoop.security.IngressPortBasedResolver");
    conf.set("ingress.port.sasl.configured.ports", "12000");
    conf.set("ingress.port.sasl.prop.12000", this.configKey);
    conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setBoolean(DFS_NAMENODE_SEND_QOP_ENABLED, true);
    conf.set(HADOOP_RPC_PROTECTION, this.configKey);
    cluster = null;
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();

    HdfsConfiguration clientConf = new HdfsConfiguration(conf);
    clientConf.unset(
        CommonConfigurationKeys.HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS);
    URI currentURI = cluster.getURI();
    URI uriAuxiliary = new URI(currentURI.getScheme() +
        "://" + currentURI.getHost() + ":12000");
    dfs = (DistributedFileSystem) FileSystem.get(uriAuxiliary, conf);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testAddBlockWrappingQOP() throws Exception {
    final String src = "/testAddBlockWrappingQOP";
    final Path path = new Path(src);

    dfs.create(path);

    DFSClient client = dfs.getClient();
    String clientName = client.getClientName();

    LocatedBlock lb = client.namenode.addBlock(src, clientName, null, null,
        HdfsConstants.GRANDFATHER_INODE_ID, null, null);
    byte[] secret = lb.getBlockToken().decodeIdentifier().getHandshakeMsg();
    assertEquals(this.qopValue, new String(secret));
  }

  @Test
  public void testAppendWrappingQOP() throws Exception {
    final String src = "/testAppendWrappingQOP";
    final Path path = new Path(src);

    FSDataOutputStream out = dfs.create(path);
    // NameNode append call returns a last block instance. If there is nothing
    // it returns as a null. So write something, so that lastBlock has
    // something
    out.write(0);
    out.close();

    DFSClient client = dfs.getClient();
    String clientName = client.getClientName();

    LastBlockWithStatus lastBlock = client.namenode.append(src, clientName,
        new EnumSetWritable<>(EnumSet.of(CreateFlag.APPEND)));

    byte[] secret = lastBlock.getLastBlock().getBlockToken()
        .decodeIdentifier().getHandshakeMsg();
    assertEquals(this.qopValue, new String(secret));
  }

  @Test
  public void testGetBlockLocationWrappingQOP() throws Exception {
    final String src = "/testGetBlockLocationWrappingQOP";
    final Path path = new Path(src);

    FSDataOutputStream out = dfs.create(path);
    // if the file is empty, there will be no blocks returned. Write something
    // so that getBlockLocations actually returns some block.
    out.write(0);
    out.close();

    FileStatus status = dfs.getFileStatus(path);
    DFSClient client = dfs.getClient();
    LocatedBlocks lbs = client.namenode.getBlockLocations(
        src, 0, status.getLen());

    assertTrue(lbs.getLocatedBlocks().size() > 0);

    for (LocatedBlock lb : lbs.getLocatedBlocks()) {
      byte[] secret = lb.getBlockToken()
          .decodeIdentifier().getHandshakeMsg();
      assertEquals(this.qopValue, new String(secret));
    }
  }
}
