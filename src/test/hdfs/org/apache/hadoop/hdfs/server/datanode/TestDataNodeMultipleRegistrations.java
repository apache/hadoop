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

package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.hdfs.server.common.Util.fileAsURI;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode.BPOfferService;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.VolumeInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDataNodeMultipleRegistrations {
  private static final Log LOG = 
    LogFactory.getLog(TestDataNodeMultipleRegistrations.class);
  File common_base_dir;
  String localHost;
  Configuration conf;

  @Before
  public void setUp() throws Exception {
    common_base_dir = new File(MiniDFSCluster.getBaseDirectory());
    if (common_base_dir != null) {
      if (common_base_dir.exists() && !FileUtil.fullyDelete(common_base_dir)) {
        throw new IOException("cannot get directory ready:"
            + common_base_dir.getAbsolutePath());
      }
    }

    conf = new HdfsConfiguration();
    localHost = DNS.getDefaultHost(conf.get("dfs.datanode.dns.interface",
        "default"), conf.get("dfs.datanode.dns.nameserver", "default"));

    localHost = "127.0.0.1";
    conf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, localHost);
  }

  NameNode startNameNode(Configuration conf, int nnPort) throws IOException {
    // per nn base_dir
    File base_dir = new File(common_base_dir, Integer.toString(nnPort));

    boolean manageNameDfsDirs = true; // for now
    boolean format = true; // for now
    // disable service authorization
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        false);

    // Setup the NameNode configuration
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, localHost + ":0");
    if (manageNameDfsDirs) {
      String name = fileAsURI(new File(base_dir, "name1")) + ","
          + fileAsURI(new File(base_dir, "name2"));
      conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, name);
      String sname = fileAsURI(new File(base_dir, "namesecondary1")) + ","
          + fileAsURI(new File(base_dir, "namesecondary2"));
      conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY, sname);
    }

    // Format and clean out DataNode directories
    if (format) {
      GenericTestUtils.formatNamenode(conf);
    }

    // Start the NameNode
    String[] args = new String[] {};
    return NameNode.createNameNode(args, conf);
  }

  public DataNode startDataNode(Configuration conf) throws IOException {
    Configuration dnConf = new HdfsConfiguration(conf);
    boolean manageDfsDirs = true; // for now
    File data_dir = new File(common_base_dir, "data");
    if (data_dir.exists() && !FileUtil.fullyDelete(data_dir)) {
      throw new IOException("Cannot remove data directory: " + data_dir);
    }

    if (manageDfsDirs) {
      File dir1 = new File(data_dir, "data1");
      File dir2 = new File(data_dir, "data2");
      dir1.mkdirs();
      dir2.mkdirs();
      if (!dir1.isDirectory() || !dir2.isDirectory()) {
        throw new IOException(
            "Mkdirs failed to create directory for DataNode: " + dir1 + " or "
                + dir2);
      }
      String dirs = fileAsURI(dir1) + "," + fileAsURI(dir2);
      dnConf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dirs);
      conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dirs);
    }
    LOG.debug("Starting DataNode " + " with "
        + DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY + ": "
        + dnConf.get(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY));

    String[] dnArgs = null; // for now
    DataNode dn = DataNode.instantiateDataNode(dnArgs, dnConf);
    if (dn == null)
      throw new IOException("Cannot start DataNode in "
          + dnConf.get(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY));

    dn.runDatanodeDaemon();
    return dn;
  }

  /**
   * start multiple NNs and single DN and verifies per BP registrations and
   * handshakes.
   * 
   * @throws IOException
   */
  @Test
  public void test2NNRegistration() throws IOException {
    NameNode nn1, nn2;
    // figure out host name for DataNode
    int nnPort = 9928;
    String nnURL1 = "hdfs://" + localHost + ":" + Integer.toString(nnPort);
    FileSystem.setDefaultUri(conf, nnURL1);
    nn1 = startNameNode(conf, nnPort);
    
    nnPort = 9929;
    String nnURL2 = "hdfs://" + localHost + ":" + Integer.toString(nnPort);
    FileSystem.setDefaultUri(conf, nnURL2);
    nn2 = startNameNode(conf, nnPort);
    
    Assert.assertNotNull("cannot create nn1", nn1);
    Assert.assertNotNull("cannot create nn2", nn2);
    
    String bpid1 = nn1.getFSImage().getBlockPoolID();
    String bpid2 = nn2.getFSImage().getBlockPoolID();
    String cid1 = nn1.getFSImage().getClusterID();
    String cid2 = nn2.getFSImage().getClusterID();
    int lv1 = nn1.getFSImage().getLayoutVersion();
    int lv2 = nn2.getFSImage().getLayoutVersion();
    int ns1 = nn1.getFSImage().namespaceID;
    int ns2 = nn2.getFSImage().namespaceID;
    Assert.assertNotSame("namespace ids should be different", ns1, ns2);
    LOG.info("nn1: lv=" + lv1 + ";cid=" + cid1 + ";bpid=" + bpid1
        + ";uri=" + nn1.getNameNodeAddress());
    LOG.info("nn2: lv=" + lv2 + ";cid=" + cid2 + ";bpid=" + bpid2
        + ";uri=" + nn2.getNameNodeAddress());

    // now start the datanode...
    String nns = nnURL1 + "," + nnURL2;
    conf.set(DFSConfigKeys.DFS_FEDERATION_NAMENODES, nns);
    DataNode dn = startDataNode(conf);
    Assert.assertNotNull("failed to create DataNode", dn);
    waitDataNodeUp(dn);

    
 // check number of vlumes in fsdataset
    Collection<VolumeInfo> volInfos = ((FSDataset) dn.data).getVolumeInfo();
    Assert.assertNotNull("No volumes in the fsdataset", volInfos);
    int i=0;
    for(VolumeInfo vi : volInfos) {
      LOG.info("vol " + i++ + ";dir=" + vi.directory + ";fs= " + vi.freeSpace);
    }
    // number of volumes should be 2 - [data1, data2]
    Assert.assertEquals("number of volumes is wrong",2, volInfos.size());
    
    
    for (BPOfferService bpos : dn.nameNodeThreads) {
      LOG.info("reg: bpid=" + "; name=" + bpos.bpRegistration.name
          + "; sid=" + bpos.bpRegistration.storageID + "; nna=" + bpos.nn_addr);
    }
    
    BPOfferService bpos1 = dn.nameNodeThreads[0];
    BPOfferService bpos2 = dn.nameNodeThreads[1];

    Assert.assertEquals("wrong nn address", bpos1.nn_addr, nn1
        .getNameNodeAddress());
    Assert.assertEquals("wrong nn address", bpos2.nn_addr, nn2
        .getNameNodeAddress());
    Assert.assertEquals("wrong bpid", bpos1.getBlockPoolId(), bpid1);
    Assert.assertEquals("wrong bpid", bpos2.getBlockPoolId(), bpid2);
    Assert.assertEquals("wrong cid", dn.getClusterId(), cid1);
    Assert.assertEquals("cid should be same", cid2, cid1);
    Assert.assertEquals("namespace should be same", bpos1.bpNSInfo.namespaceID,
        ns1);
    Assert.assertEquals("namespace should be same", bpos2.bpNSInfo.namespaceID,
        ns2);

    dn.shutdown();
    shutdownNN(nn1);
    nn1 = null;
    shutdownNN(nn2);
    nn2 = null;
  }

  /**
   * starts single nn and single dn and verifies registration and handshake
   * 
   * @throws IOException
   */
  @Test
  public void testFedSingleNN() throws IOException {
    NameNode nn1;
    int nnPort = 9927;
    // figure out host name for DataNode
    String nnURL = "hdfs://" + localHost + ":" + Integer.toString(nnPort);

    FileSystem.setDefaultUri(conf, nnURL);
    nn1 = startNameNode(conf, nnPort);
    Assert.assertNotNull("cannot create nn1", nn1);

    String bpid1 = nn1.getFSImage().getBlockPoolID();
    String cid1 = nn1.getFSImage().getClusterID();
    int lv1 = nn1.getFSImage().getLayoutVersion();
    LOG.info("nn1: lv=" + lv1 + ";cid=" + cid1 + ";bpid=" + bpid1
        + ";uri=" + nn1.getNameNodeAddress());

    // now start the datanode...
    String nns = nnURL;
    conf.set(DFSConfigKeys.DFS_FEDERATION_NAMENODES, nns);

    DataNode dn = startDataNode(conf);
    Assert.assertNotNull("failed to create DataNode", dn);

    waitDataNodeUp(dn);
    // check number of vlumes in fsdataset
    Collection<VolumeInfo> volInfos = ((FSDataset) dn.data).getVolumeInfo();
    Assert.assertNotNull("No volumes in the fsdataset", volInfos);
    int i=0;
    for(VolumeInfo vi : volInfos) {
      LOG.info("vol " + i++ + ";dir=" + vi.directory + ";fs= " + vi.freeSpace);
    }
    // number of volumes should be 2 - [data1, data2]
    Assert.assertEquals("number of volumes is wrong",2, volInfos.size());
    

    for (BPOfferService bpos : dn.nameNodeThreads) {
      LOG.debug("reg: bpid=" + "; name=" + bpos.bpRegistration.name
          + "; sid=" + bpos.bpRegistration.storageID + "; nna=" + bpos.nn_addr);
    }
    
    // try block report
    BPOfferService bpos1 = dn.nameNodeThreads[0];
    bpos1.lastBlockReport = 0;
    DatanodeCommand cmd = bpos1.blockReport();

    Assert.assertNotNull("cmd is null", cmd);

    Assert.assertEquals("wrong nn address", bpos1.nn_addr, nn1
        .getNameNodeAddress());
    Assert.assertEquals("wrong bpid", bpos1.getBlockPoolId(), bpid1);
    Assert.assertEquals("wrong cid", dn.getClusterId(), cid1);

    dn.shutdown();
    dn = null;
    shutdownNN(nn1);
    nn1 = null;
  }

  private void shutdownNN(NameNode nn) {
    if (nn == null) {
      return;
    }
    nn.stop();
    nn.join();
  }

  public boolean isDnUp(DataNode dn) {
    boolean up = dn.nameNodeThreads.length > 0;
    for (BPOfferService bpos : dn.nameNodeThreads) {
      up = up && bpos.initialized();
    }
    return up;
  }

  public void waitDataNodeUp(DataNode dn) {
    // should be something smart
    while (!isDnUp(dn)) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
      }
    }
  }
}
