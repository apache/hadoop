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
package org.apache.hadoop.hdfs.server.journalservice;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.FenceResponse;
import org.apache.hadoop.hdfs.server.protocol.FencedException;
import org.apache.hadoop.hdfs.server.protocol.JournalInfo;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link JournalService}
 */
public class TestJournalService {
  static final Log LOG = LogFactory.getLog(TestJournalService.class);
  static final InetSocketAddress RPC_ADDR = new InetSocketAddress(0);

  /**
   * Test calls backs {@link JournalListener#startLogSegment(JournalService, long)} and
   * {@link JournalListener#journal(JournalService, long, int, byte[])} are
   * called.
   */
  @Test
  public void testCallBacks() throws Exception {
    Configuration conf = TestJournal.newConf("testCallBacks");
    JournalListener listener = Mockito.mock(JournalListener.class);
    JournalService service = null;
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive(0);
      InetSocketAddress nnAddr = cluster.getNameNode(0).getNameNodeAddress();
      service = newJournalService(nnAddr, listener, conf);
      service.start();
      verifyRollLogsCallback(service, listener);
      verifyJournalCallback(cluster.getFileSystem(), service, listener);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (service != null) {
        service.stop();
      }
    }
  }

  @Test
  public void testFence() throws Exception {
    final Configuration conf = TestJournal.newConf("testFence");
    final JournalListener listener = Mockito.mock(JournalListener.class);
    final InetSocketAddress nnAddress;

    JournalService service = null;
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive(0);
      NameNode nn = cluster.getNameNode(0);
      nnAddress = nn.getNameNodeAddress();
      service = newJournalService(nnAddress, listener, conf);
      service.start();
      String cid = nn.getNamesystem().getClusterId();
      int nsId = nn.getNamesystem().getFSImage().getNamespaceID();
      int lv = nn.getNamesystem().getFSImage().getLayoutVersion();

      verifyFence(service, listener, cid, nsId, lv);
    } finally {
      cluster.shutdown();
    }

    //test restart journal service
    StorageInfo before = service.getJournal().getStorageInfo();
    LOG.info("before: " + before);
    service.stop();
    service = newJournalService(nnAddress, listener, conf);
    StorageInfo after = service.getJournal().getStorageInfo();
    LOG.info("after : " + after);
    Assert.assertEquals(before.toString(), after.toString());
  }

  private JournalService newJournalService(InetSocketAddress nnAddr,
      JournalListener listener, Configuration conf) throws IOException {
    return new JournalService(conf, nnAddr, RPC_ADDR, listener);
  }
  
  /**
   * Starting {@link JournalService} should result in Namenode calling
   * {@link JournalService#startLogSegment}, resulting in callback 
   * {@link JournalListener#rollLogs}
   */
  private void verifyRollLogsCallback(JournalService s, JournalListener l)
      throws IOException {
    Mockito.verify(l, Mockito.times(1)).startLogSegment(Mockito.eq(s), Mockito.anyLong());
  }

  /**
   * File system write operations should result in JournalListener call
   * backs.
   */
  private void verifyJournalCallback(FileSystem fs, JournalService s,
      JournalListener l) throws IOException {
    Path fileName = new Path("/tmp/verifyJournalCallback");
    FileSystemTestHelper.createFile(fs, fileName);
    fs.delete(fileName, true);
    Mockito.verify(l, Mockito.atLeastOnce()).journal(Mockito.eq(s),
        Mockito.anyLong(), Mockito.anyInt(), (byte[]) Mockito.any());
  }
  
  void verifyFence(JournalService s, JournalListener listener,
      String cid, int nsId, int lv) throws Exception {
    
    // Fence the journal service
    JournalInfo info = new JournalInfo(lv, cid, nsId);
    long currentEpoch = s.getEpoch();
   
    // New epoch lower than the current epoch is rejected
    try {
      s.fence(info, (currentEpoch - 1), "fencer");
      Assert.fail();
    } catch (FencedException ignore) { /* Ignored */ }
    
    // New epoch equal to the current epoch is rejected
    try {
      s.fence(info, currentEpoch, "fencer");
      Assert.fail();
    } catch (FencedException ignore) { /* Ignored */ }
    
    // New epoch higher than the current epoch is successful
    FenceResponse resp = s.fence(info, currentEpoch+1, "fencer");
    Assert.assertNotNull(resp);
    
    JournalInfo badInfo = new JournalInfo(lv, "fake", nsId);
    currentEpoch = s.getEpoch();
   
    // Send in the wrong cluster id. fence should fail
    try {
      s.fence(badInfo, currentEpoch+1, "fencer");
      Assert.fail();
      
    } catch (UnregisteredNodeException ignore) {
      LOG.info(ignore.getMessage());
    }
  
    badInfo = new JournalInfo(lv, cid, nsId+1);
    currentEpoch = s.getEpoch();
    
    // Send in the wrong nsid. fence should fail
    try {
      s.fence(badInfo, currentEpoch+1, "fencer");
      Assert.fail();
    } catch (UnregisteredNodeException ignore) {
      LOG.info(ignore.getMessage());
    } 
    
    // New epoch higher than the current epoch is successful
    resp = s.fence(info, currentEpoch+1, "fencer");
    Assert.assertNotNull(resp);
   
  }
}