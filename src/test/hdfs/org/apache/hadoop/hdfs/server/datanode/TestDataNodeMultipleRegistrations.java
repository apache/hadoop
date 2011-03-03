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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode.BPOfferService;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.VolumeInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
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
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numNameNodes(2)
        .nameNodePort(9928).build();
    try {
      cluster.waitActive();
      NameNode nn1 = cluster.getNameNode(0);
      NameNode nn2 = cluster.getNameNode(1);
      assertNotNull("cannot create nn1", nn1);
      assertNotNull("cannot create nn2", nn2);

      String bpid1 = nn1.getFSImage().getBlockPoolID();
      String bpid2 = nn2.getFSImage().getBlockPoolID();
      String cid1 = nn1.getFSImage().getClusterID();
      String cid2 = nn2.getFSImage().getClusterID();
      int lv1 = nn1.getFSImage().getLayoutVersion();
      int lv2 = nn2.getFSImage().getLayoutVersion();
      int ns1 = nn1.getFSImage().namespaceID;
      int ns2 = nn2.getFSImage().namespaceID;
      assertNotSame("namespace ids should be different", ns1, ns2);
      LOG.info("nn1: lv=" + lv1 + ";cid=" + cid1 + ";bpid=" + bpid1 + ";uri="
          + nn1.getNameNodeAddress());
      LOG.info("nn2: lv=" + lv2 + ";cid=" + cid2 + ";bpid=" + bpid2 + ";uri="
          + nn2.getNameNodeAddress());

      // check number of volumes in fsdataset
      DataNode dn = cluster.getDataNodes().get(0);
      Collection<VolumeInfo> volInfos = ((FSDataset) dn.data).getVolumeInfo();
      assertNotNull("No volumes in the fsdataset", volInfos);
      int i = 0;
      for (VolumeInfo vi : volInfos) {
        LOG.info("vol " + i++ + ";dir=" + vi.directory + ";fs= " + vi.freeSpace);
      }
      // number of volumes should be 2 - [data1, data2]
      assertEquals("number of volumes is wrong", 2, volInfos.size());

      for (BPOfferService bpos : dn.getAllBpOs()) {
        LOG.info("reg: bpid=" + "; name=" + bpos.bpRegistration.name + "; sid="
            + bpos.bpRegistration.storageID + "; nna=" + bpos.nnAddr);
      }

      BPOfferService bpos1 = dn.getAllBpOs()[0];
      BPOfferService bpos2 = dn.getAllBpOs()[1];

      // The order of bpos is not guaranteed, so fix the order
      if (bpos1.nnAddr.equals(nn2.getNameNodeAddress())) {
        BPOfferService tmp = bpos1;
        bpos1 = bpos2;
        bpos2 = tmp;
      }

      assertEquals("wrong nn address", bpos1.nnAddr,
          nn1.getNameNodeAddress());
      assertEquals("wrong nn address", bpos2.nnAddr,
          nn2.getNameNodeAddress());
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

      String bpid1 = nn1.getFSImage().getBlockPoolID();
      String cid1 = nn1.getFSImage().getClusterID();
      int lv1 = nn1.getFSImage().getLayoutVersion();
      LOG.info("nn1: lv=" + lv1 + ";cid=" + cid1 + ";bpid=" + bpid1 + ";uri="
          + nn1.getNameNodeAddress());

      // check number of vlumes in fsdataset
      DataNode dn = cluster.getDataNodes().get(0);
      Collection<VolumeInfo> volInfos = ((FSDataset) dn.data).getVolumeInfo();
      assertNotNull("No volumes in the fsdataset", volInfos);
      int i = 0;
      for (VolumeInfo vi : volInfos) {
        LOG.info("vol " + i++ + ";dir=" + vi.directory + ";fs= " + vi.freeSpace);
      }
      // number of volumes should be 2 - [data1, data2]
      assertEquals("number of volumes is wrong", 2, volInfos.size());

      for (BPOfferService bpos : dn.getAllBpOs()) {
        LOG.info("reg: bpid=" + "; name=" + bpos.bpRegistration.name + "; sid="
            + bpos.bpRegistration.storageID + "; nna=" + bpos.nnAddr);
      }

      // try block report
      BPOfferService bpos1 = dn.getAllBpOs()[0];
      bpos1.lastBlockReport = 0;
      DatanodeCommand cmd = bpos1.blockReport();

      assertNotNull("cmd is null", cmd);

      assertEquals("wrong nn address", bpos1.nnAddr,
          nn1.getNameNodeAddress());
      assertEquals("wrong bpid", bpos1.getBlockPoolId(), bpid1);
      assertEquals("wrong cid", dn.getClusterId(), cid1);
      cluster.shutdown();
      
      // Ensure all the BPOfferService threads are shutdown
      assertEquals(0, dn.getAllBpOs().length);
      cluster = null;
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testClusterIdMismatch() throws IOException {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numNameNodes(2).
    nameNodePort(9928).build();
    try {
      cluster.waitActive();

      DataNode dn = cluster.getDataNodes().get(0);
      BPOfferService [] bposs = dn.getAllBpOs(); 
      LOG.info("dn bpos len (should be 2):" + bposs.length);
      Assert.assertEquals("should've registered with two namenodes", bposs.length,2);
      
      // add another namenode
      cluster.addNameNode(conf, 9938);
      bposs = dn.getAllBpOs(); 
      LOG.info("dn bpos len (should be 3):" + bposs.length);
      Assert.assertEquals("should've registered with three namenodes", bposs.length,3);
      
      // change cluster id and another Namenode
      StartupOption.FORMAT.setClusterId("DifferentCID");
      cluster.addNameNode(conf, 9948);
      NameNode nn4 = cluster.getNameNode(3);
      assertNotNull("cannot create nn4", nn4);
      
      bposs = dn.getAllBpOs(); 
      LOG.info("dn bpos len (still should be 3):" + bposs.length);
      Assert.assertEquals("should've registered with three namenodes", 3, bposs.length);
    } finally {
      if(cluster != null) 
        cluster.shutdown();
    }
  }

}
