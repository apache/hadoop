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
    // TODO: remove the manual setting storage when JN is fully implemented
    path1 = new File(hdfsDir, "jn1dir");
    path2 = new File(hdfsDir, "jn2dir");
    path1.mkdir();
    path2.mkdir();
    if (!path1.exists() || !path2.exists()) {
      throw new IOException("Couldn't create path in "
          + hdfsDir.getAbsolutePath());
    }

    System.out.println("configuring hdfsdir is " + hdfsDir.getAbsolutePath()
        + "; jn1Dir = " + path1.getPath() + "; jn2Dir = " + path2.getPath());
  }

  /**
   * Test Journal service Http Server
   * 
   * @throws Exception
   */
  @Test
  public void testHttpServer() throws Exception {
    MiniDFSCluster cluster = null;
    JournalHttpServer jns1 = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();

      conf.set(DFSConfigKeys.DFS_JOURNAL_EDITS_DIR_KEY, path1.getPath());
      jns1 = new JournalHttpServer(conf, new Journal(conf),
          NetUtils.createSocketAddr("localhost:50200"));
      jns1.start();

      String pageContents = DFSTestUtil.urlGet(new URL(
          "http://localhost:50200/journalstatus.jsp"));
      assertTrue(pageContents.contains("JournalNode"));

    } catch (IOException e) {
      LOG.error("Error in TestHttpServer:", e);
      assertTrue(e.getLocalizedMessage(), false);
    } finally {
      if (jns1 != null)
        jns1.stop();
      if (cluster != null)
        cluster.shutdown();
    }
  }
  
  //TODO: remove this method when the same rpc is supported by journal service
  private RemoteEditLogManifest editsToDownload(InetSocketAddress srcRpcAddr,
      long txid) throws IOException {

    NamenodeProtocol namenode = NameNodeProxies.createNonHAProxy(conf,
        srcRpcAddr, NamenodeProtocol.class,
        UserGroupInformation.getCurrentUser(), true).getProxy();

    // get all edit segments
    RemoteEditLogManifest manifest = namenode.getEditLogManifest(txid);

    return manifest;
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
    JournalHttpServer jns1 = null, jns2 = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();

      // restart namenode, so it will have one finalized edit segment
      cluster.restartNameNode();

      // get namenode clusterID/layoutVersion/namespaceID
      InetSocketAddress nnAddr = cluster.getNameNode(0).getNameNodeAddress();
      InetSocketAddress serverAddr = new InetSocketAddress(0);
      JournalListener listener = Mockito.mock(JournalListener.class);
      JournalService service = new JournalService(conf, nnAddr, serverAddr,
          listener);
      service.start();
      StorageInfo si = service.getJournal().getStorage();
      service.stop();
        
      // start jns1 with path1
      conf.set(DFSConfigKeys.DFS_JOURNAL_EDITS_DIR_KEY, path1.getPath());
      Journal journal1 = new Journal(conf);
      journal1.format(si.namespaceID, si.clusterID);   
      jns1 = new JournalHttpServer(conf, journal1,
          NetUtils.createSocketAddr("localhost:50200"));
      jns1.start();

      InetSocketAddress srcRpcAddr = NameNode.getServiceAddress(conf, true);
      RemoteEditLogManifest manifest = editsToDownload(srcRpcAddr, 1);

      String addr = conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
      jns1.downloadEditFiles(addr, manifest);

      // start jns2 with path2
      conf.set(DFSConfigKeys.DFS_JOURNAL_EDITS_DIR_KEY, path2.getPath());
      Journal journal2 = new Journal(conf);
      journal2.format(si.namespaceID, si.clusterID);   
      jns2 = new JournalHttpServer(conf, journal2,
          NetUtils.createSocketAddr("localhost:50300"));
      jns2.start();

      jns2.downloadEditFiles("localhost:50200", manifest);

    } catch (IOException e) {
      LOG.error("Error in TestCopyEdits:", e);
      assertTrue(e.getLocalizedMessage(), false);
    } finally {
      if (jns1 != null)
        jns1.stop();
      if (jns2 != null)
        jns2.stop();
      if (cluster != null)
        cluster.shutdown();
    }
  }
}
