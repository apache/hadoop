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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDataNodeMultipleRegistrations {
  private static final Log LOG = 
    LogFactory.getLog(TestDataNodeMultipleRegistrations.class);
  Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = new HdfsConfiguration();
  }

  /**
   * start multiple NNs and single DN and verifies per BP registrations and
   * handshakes.
   * 
   * @throws IOException
   */
  @Test
  public void test2NNRegistration() throws IOException {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
        .build();
    try {
      cluster.waitActive();
      NameNode nn1 = cluster.getNameNode(0);
      NameNode nn2 = cluster.getNameNode(1);
      assertNotNull("cannot create nn1", nn1);
      assertNotNull("cannot create nn2", nn2);

      String bpid1 = FSImageTestUtil.getFSImage(nn1).getBlockPoolID();
      String bpid2 = FSImageTestUtil.getFSImage(nn2).getBlockPoolID();
      String cid1 = FSImageTestUtil.getFSImage(nn1).getClusterID();
      String cid2 = FSImageTestUtil.getFSImage(nn2).getClusterID();
      int lv1 =FSImageTestUtil.getFSImage(nn1).getLayoutVersion();
      int lv2 = FSImageTestUtil.getFSImage(nn2).getLayoutVersion();
      int ns1 = FSImageTestUtil.getFSImage(nn1).getNamespaceID();
      int ns2 = FSImageTestUtil.getFSImage(nn2).getNamespaceID();
      assertNotSame("namespace ids should be different", ns1, ns2);
      LOG.info("nn1: lv=" + lv1 + ";cid=" + cid1 + ";bpid=" + bpid1 + ";uri="
          + nn1.getNameNodeAddress());
      LOG.info("nn2: lv=" + lv2 + ";cid=" + cid2 + ";bpid=" + bpid2 + ";uri="
          + nn2.getNameNodeAddress());

      // check number of volumes in fsdataset
      DataNode dn = cluster.getDataNodes().get(0);
      final Map<String, Object> volInfos = dn.data.getVolumeInfoMap();
      Assert.assertTrue("No volumes in the fsdataset", volInfos.size() > 0);
      int i = 0;
      for (Map.Entry<String, Object> e : volInfos.entrySet()) {
        LOG.info("vol " + i++ + ") " + e.getKey() + ": " + e.getValue());
      }
      // number of volumes should be 2 - [data1, data2]
      assertEquals("number of volumes is wrong",
          cluster.getFsDatasetTestUtils(0).getDefaultNumOfDataDirs(),
          volInfos.size());

      for (BPOfferService bpos : dn.getAllBpOs()) {
        LOG.info("BP: " + bpos);
      }

      BPOfferService bpos1 = dn.getAllBpOs().get(0);
      BPOfferService bpos2 = dn.getAllBpOs().get(1);

      // The order of bpos is not guaranteed, so fix the order
      if (getNNSocketAddress(bpos1).equals(nn2.getServiceRpcAddress())) {
        BPOfferService tmp = bpos1;
        bpos1 = bpos2;
        bpos2 = tmp;
      }

      assertEquals("wrong nn address", getNNSocketAddress(bpos1),
          nn1.getServiceRpcAddress());
      assertEquals("wrong nn address", getNNSocketAddress(bpos2),
          nn2.getServiceRpcAddress());
      assertEquals("wrong bpid", bpos1.getBlockPoolId(), bpid1);
      assertEquals("wrong bpid", bpos2.getBlockPoolId(), bpid2);
      assertEquals("wrong cid", dn.getClusterId(), cid1);
      assertEquals("cid should be same", cid2, cid1);
      assertEquals("namespace should be same",
          bpos1.bpNSInfo.namespaceID, ns1);
      assertEquals("namespace should be same",
          bpos2.bpNSInfo.namespaceID, ns2);
    } finally {
      cluster.shutdown();
    }
  }
  
  private static InetSocketAddress getNNSocketAddress(BPOfferService bpos) {
    List<BPServiceActor> actors = bpos.getBPServiceActors();
    assertEquals(1, actors.size());
    return actors.get(0).getNNSocketAddress();
  }

  /**
   * starts single nn and single dn and verifies registration and handshake
   * 
   * @throws IOException
   */
  @Test
  public void testFedSingleNN() throws IOException {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nameNodePort(9927).build();
    try {
      NameNode nn1 = cluster.getNameNode();
      assertNotNull("cannot create nn1", nn1);

      String bpid1 = FSImageTestUtil.getFSImage(nn1).getBlockPoolID();
      String cid1 = FSImageTestUtil.getFSImage(nn1).getClusterID();
      int lv1 = FSImageTestUtil.getFSImage(nn1).getLayoutVersion();
      LOG.info("nn1: lv=" + lv1 + ";cid=" + cid1 + ";bpid=" + bpid1 + ";uri="
          + nn1.getNameNodeAddress());

      // check number of vlumes in fsdataset
      DataNode dn = cluster.getDataNodes().get(0);
      final Map<String, Object> volInfos = dn.data.getVolumeInfoMap();
      Assert.assertTrue("No volumes in the fsdataset", volInfos.size() > 0);
      int i = 0;
      for (Map.Entry<String, Object> e : volInfos.entrySet()) {
        LOG.info("vol " + i++ + ") " + e.getKey() + ": " + e.getValue());
      }
      // number of volumes should be 2 - [data1, data2]
      assertEquals("number of volumes is wrong",
          cluster.getFsDatasetTestUtils(0).getDefaultNumOfDataDirs(),
          volInfos.size());

      for (BPOfferService bpos : dn.getAllBpOs()) {
        LOG.info("reg: bpid=" + "; name=" + bpos.bpRegistration + "; sid="
            + bpos.bpRegistration.getDatanodeUuid() + "; nna=" +
            getNNSocketAddress(bpos));
      }

      // try block report
      BPOfferService bpos1 = dn.getAllBpOs().get(0);
      bpos1.triggerBlockReportForTests();

      assertEquals("wrong nn address",
          getNNSocketAddress(bpos1),
          nn1.getServiceRpcAddress());
      assertEquals("wrong bpid", bpos1.getBlockPoolId(), bpid1);
      assertEquals("wrong cid", dn.getClusterId(), cid1);
      cluster.shutdown();
      
      // Ensure all the BPOfferService threads are shutdown
      assertEquals(0, dn.getAllBpOs().size());
      cluster = null;
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testClusterIdMismatch() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
        .build();
    try {
      cluster.waitActive();

      DataNode dn = cluster.getDataNodes().get(0);
      List<BPOfferService> bposs = dn.getAllBpOs();
      LOG.info("dn bpos len (should be 2):" + bposs.size());
      Assert.assertEquals("should've registered with two namenodes", bposs.size(),2);
      
      // add another namenode
      cluster.addNameNode(conf, 9938);
      Thread.sleep(500);// lets wait for the registration to happen
      bposs = dn.getAllBpOs(); 
      LOG.info("dn bpos len (should be 3):" + bposs.size());
      Assert.assertEquals("should've registered with three namenodes", bposs.size(),3);
      
      // change cluster id and another Namenode
      StartupOption.FORMAT.setClusterId("DifferentCID");
      cluster.addNameNode(conf, 9948);
      NameNode nn4 = cluster.getNameNode(3);
      assertNotNull("cannot create nn4", nn4);

      Thread.sleep(500);// lets wait for the registration to happen
      bposs = dn.getAllBpOs(); 
      LOG.info("dn bpos len (still should be 3):" + bposs.size());
      Assert.assertEquals("should've registered with three namenodes", 3, bposs.size());
    } finally {
        cluster.shutdown();
    }
  }

  @Test(timeout = 20000)
  public void testClusterIdMismatchAtStartupWithHA() throws Exception {
    MiniDFSNNTopology top = new MiniDFSNNTopology()
      .addNameservice(new MiniDFSNNTopology.NSConf("ns1")
        .addNN(new MiniDFSNNTopology.NNConf("nn0"))
        .addNN(new MiniDFSNNTopology.NNConf("nn1")))
      .addNameservice(new MiniDFSNNTopology.NSConf("ns2")
        .addNN(new MiniDFSNNTopology.NNConf("nn2").setClusterId("bad-cid"))
        .addNN(new MiniDFSNNTopology.NNConf("nn3").setClusterId("bad-cid")));

    top.setFederation(true);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).nnTopology(top)
        .numDataNodes(0).build();
    
    try {
      cluster.startDataNodes(conf, 1, true, null, null);
      // let the initialization be complete
      Thread.sleep(10000);
      DataNode dn = cluster.getDataNodes().get(0);
      assertTrue("Datanode should be running", dn.isDatanodeUp());
      assertEquals("Only one BPOfferService should be running", 1,
          dn.getAllBpOs().size());
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testDNWithInvalidStorageWithHA() throws Exception {
    MiniDFSNNTopology top = new MiniDFSNNTopology()
      .addNameservice(new MiniDFSNNTopology.NSConf("ns1")
        .addNN(new MiniDFSNNTopology.NNConf("nn0").setClusterId("cluster-1"))
        .addNN(new MiniDFSNNTopology.NNConf("nn1").setClusterId("cluster-1")));

    top.setFederation(true);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).nnTopology(top)
        .numDataNodes(0).build();
    try {
      cluster.startDataNodes(conf, 1, true, null, null);
      // let the initialization be complete
      Thread.sleep(10000);
      DataNode dn = cluster.getDataNodes().get(0);
      assertTrue("Datanode should be running", dn.isDatanodeUp());
      assertEquals("BPOfferService should be running", 1,
          dn.getAllBpOs().size());
      DataNodeProperties dnProp = cluster.stopDataNode(0);

      cluster.getNameNode(0).stop();
      cluster.getNameNode(1).stop();
      Configuration nn1 = cluster.getConfiguration(0);
      Configuration nn2 = cluster.getConfiguration(1);
      // setting up invalid cluster
      StartupOption.FORMAT.setClusterId("cluster-2");
      DFSTestUtil.formatNameNode(nn1);
      MiniDFSCluster.copyNameDirs(FSNamesystem.getNamespaceDirs(nn1),
          FSNamesystem.getNamespaceDirs(nn2), nn2);
      cluster.restartNameNode(0, false);
      cluster.restartNameNode(1, false);
      cluster.restartDataNode(dnProp);
      
      // let the initialization be complete
      Thread.sleep(10000);
      dn = cluster.getDataNodes().get(0);
      assertFalse("Datanode should have shutdown as only service failed",
          dn.isDatanodeUp());
    } finally {
      cluster.shutdown();
    }
  }

  
  @Test
  public void testMiniDFSClusterWithMultipleNN() throws IOException {
    Configuration conf = new HdfsConfiguration();
    // start Federated cluster and add a node.
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
      .build();
    
    // add a node
    try {
      cluster.waitActive();
      Assert.assertEquals("(1)Should be 2 namenodes", 2, cluster.getNumNameNodes());

      cluster.addNameNode(conf, 0);
      Assert.assertEquals("(1)Should be 3 namenodes", 3, cluster.getNumNameNodes());
    } catch (IOException ioe) {
      Assert.fail("Failed to add NN to cluster:" + StringUtils.stringifyException(ioe));
    } finally {
      cluster.shutdown();
    }
        
    // 2. start with Federation flag set
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(1))
      .build();
    
    try {
      Assert.assertNotNull(cluster);
      cluster.waitActive();
      Assert.assertEquals("(2)Should be 1 namenodes", 1, cluster.getNumNameNodes());
    
      // add a node
      cluster.addNameNode(conf, 0);
      Assert.assertEquals("(2)Should be 2 namenodes", 2, cluster.getNumNameNodes());
    } catch (IOException ioe) {
      Assert.fail("Failed to add NN to cluster:" + StringUtils.stringifyException(ioe));
    } finally {
      cluster.shutdown();
    }

    // 3. start non-federated
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).build();
    
    // add a node
    try {
      cluster.waitActive();
      Assert.assertNotNull(cluster);
      Assert.assertEquals("(2)Should be 1 namenodes", 1, cluster.getNumNameNodes());

      cluster.addNameNode(conf, 9929);
      Assert.fail("shouldn't be able to add another NN to non federated cluster");
    } catch (IOException e) {
      // correct 
      Assert.assertTrue(e.getMessage().startsWith("cannot add namenode"));
      Assert.assertEquals("(3)Should be 1 namenodes", 1, cluster.getNumNameNodes());
    } finally {
      cluster.shutdown();
    }
  }
}
