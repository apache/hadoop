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

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.FenceResponse;
import org.apache.hadoop.hdfs.server.protocol.FencedException;
import org.apache.hadoop.hdfs.server.protocol.JournalInfo;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link JournalService}
 */
public class TestJournalService {
  private MiniDFSCluster cluster;
  private Configuration conf = new HdfsConfiguration();
  static final Log LOG = LogFactory.getLog(TestJournalService.class);
  
  /**
   * Test calls backs {@link JournalListener#rollLogs(JournalService, long)} and
   * {@link JournalListener#journal(JournalService, long, int, byte[])} are
   * called.
   */
  @Test
  public void testCallBacks() throws Exception {
    JournalListener listener = Mockito.mock(JournalListener.class);
    JournalService service = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive(0);
      service = startJournalService(listener);
      verifyRollLogsCallback(service, listener);
      verifyJournalCallback(service, listener);
      verifyFence(service, cluster.getNameNode(0));
    } finally {
      if (service != null) {
        stopJournalService(service);
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private JournalService startJournalService(JournalListener listener)
      throws IOException {
    InetSocketAddress nnAddr = cluster.getNameNode(0).getNameNodeAddress();
    InetSocketAddress serverAddr = new InetSocketAddress(0);
    JournalService service = new JournalService(conf, nnAddr, serverAddr,
        listener);
    service.start();
    return service;
  }
  
  private void stopJournalService(JournalService service) throws IOException {
    service.stop();
  }
  
  /**
   * Starting {@link JournalService} should result in Namenode calling
   * {@link JournalService#startLogSegment}, resulting in callback 
   * {@link JournalListener#rollLogs}
   */
  private void verifyRollLogsCallback(JournalService s, JournalListener l)
      throws IOException {
    Mockito.verify(l, Mockito.times(1)).rollLogs(Mockito.eq(s), Mockito.anyLong());
  }

  /**
   * File system write operations should result in JournalListener call
   * backs.
   */
  private void verifyJournalCallback(JournalService s, JournalListener l) throws IOException {
    Path fileName = new Path("/tmp/verifyJournalCallback");
    FileSystem fs = cluster.getFileSystem();
    FileSystemTestHelper.createFile(fs, fileName);
    fs.delete(fileName, true);
    Mockito.verify(l, Mockito.atLeastOnce()).journal(Mockito.eq(s),
        Mockito.anyLong(), Mockito.anyInt(), (byte[]) Mockito.any());
  }
  
  public void verifyFence(JournalService s, NameNode nn) throws Exception {
    String cid = nn.getNamesystem().getClusterId();
    int nsId = nn.getNamesystem().getFSImage().getNamespaceID();
    int lv = nn.getNamesystem().getFSImage().getLayoutVersion();
    
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
   
  }
}