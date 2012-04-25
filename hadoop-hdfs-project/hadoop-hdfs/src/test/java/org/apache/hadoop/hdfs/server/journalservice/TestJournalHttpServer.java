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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.journalservice.JournalHttpServer;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.JournalInfo;
import org.apache.hadoop.hdfs.server.protocol.JournalSyncProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestJournalHttpServer {
  public static final Log LOG = LogFactory.getLog(TestJournalHttpServer.class);

  static {
    ((Log4JLogger) JournalHttpServer.LOG).getLogger().setLevel(Level.ALL);
  }

  private Configuration conf;
  private File hdfsDir = null;
  private File path1, path2;

  @Before
  public void setUp() throws Exception {
    HdfsConfiguration.init();
    conf = new HdfsConfiguration();

    hdfsDir = new File(MiniDFSCluster.getBaseDirectory()).getCanonicalFile();
    if (hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir)) {
      throw new IOException("Could not delete hdfs directory '" + hdfsDir + "'");
    }

    hdfsDir.mkdirs();

    path1 = new File(hdfsDir, "j1dir");
    path2 = new File(hdfsDir, "j2dir");
    path1.mkdir();
    path2.mkdir();
    if (!path1.exists() || !path2.exists()) {
      throw new IOException("Couldn't create path in "
          + hdfsDir.getAbsolutePath());
    }

    System.out.println("configuring hdfsdir is " + hdfsDir.getAbsolutePath()
        + "; j1Dir = " + path1.getPath() + "; j2Dir = " + path2.getPath());
  }

  /**
   * Test Journal service Http Server
   * 
   * @throws Exception
   */
  @Test
  public void testHttpServer() throws Exception {
    MiniDFSCluster cluster = null;
    JournalHttpServer jh1 = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();

      conf.set(DFSConfigKeys.DFS_JOURNAL_EDITS_DIR_KEY, path1.getPath());
      jh1 = new JournalHttpServer(conf, new Journal(conf),
          NetUtils.createSocketAddr("localhost:50200"));
      jh1.start();

      String pageContents = DFSTestUtil.urlGet(new URL(
          "http://localhost:50200/journalstatus.jsp"));
      assertTrue(pageContents.contains("JournalNode"));

    } catch (IOException e) {
      LOG.error("Error in TestHttpServer:", e);
      assertTrue(e.getLocalizedMessage(), false);
    } finally {
      if (jh1 != null)
        jh1.stop();
      if (cluster != null)
        cluster.shutdown();
    }
  }

  /**
   * Test lagging Journal service copies edit segments from another Journal
   * service
   * 
   * @throws Exception
   */
  @Test
  public void testCopyEdits() throws Exception {
    MiniDFSCluster cluster = null;
    JournalService service = null;
    JournalHttpServer jhs1 = null, jhs2 = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();

      // restart namenode, so it will have finalized edit segments
      cluster.restartNameNode();

      conf.set(DFSConfigKeys.DFS_JOURNAL_EDITS_DIR_KEY, path1.getPath());
      InetSocketAddress nnAddr = cluster.getNameNode(0).getNameNodeAddress();
      InetSocketAddress serverAddr = new InetSocketAddress(50900);
      JournalListener listener = Mockito.mock(JournalListener.class);
      service = new JournalService(conf, nnAddr, serverAddr, listener);
      service.start();
      
      // get namenode clusterID/layoutVersion/namespaceID
      StorageInfo si = service.getJournal().getStorage();
      JournalInfo journalInfo = new JournalInfo(si.layoutVersion, si.clusterID,
          si.namespaceID);

      // start jns1 with path1
      jhs1 = new JournalHttpServer(conf, service.getJournal(),
          NetUtils.createSocketAddr("localhost:50200"));
      jhs1.start();

      // get all edit segments
      InetSocketAddress srcRpcAddr = NameNode.getServiceAddress(conf, true);
      NamenodeProtocol namenode = NameNodeProxies.createNonHAProxy(conf,
          srcRpcAddr, NamenodeProtocol.class,
          UserGroupInformation.getCurrentUser(), true).getProxy();

      RemoteEditLogManifest manifest = namenode.getEditLogManifest(1);      
      jhs1.downloadEditFiles(
          conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY), manifest);

      // start jns2 with path2
      conf.set(DFSConfigKeys.DFS_JOURNAL_EDITS_DIR_KEY, path2.getPath());
      Journal journal2 = new Journal(conf);
      journal2.format(si.namespaceID, si.clusterID);   
      jhs2 = new JournalHttpServer(conf, journal2,
          NetUtils.createSocketAddr("localhost:50300"));
      jhs2.start();

      // transfer edit logs from j1 to j2
      JournalSyncProtocol journalp = JournalService.createProxyWithJournalSyncProtocol(
          NetUtils.createSocketAddr("localhost:50900"), conf,
          UserGroupInformation.getCurrentUser());
      RemoteEditLogManifest manifest1 = journalp.getEditLogManifest(journalInfo, 1);  
      jhs2.downloadEditFiles("localhost:50200", manifest1);

    } catch (IOException e) {
      LOG.error("Error in TestCopyEdits:", e);
      assertTrue(e.getLocalizedMessage(), false);
    } finally {
      if (jhs1 != null)
        jhs1.stop();
      if (jhs2 != null)
        jhs2.stop();
      if (cluster != null)
        cluster.shutdown();
      if (service != null)
        service.stop();
    }
  }
}
