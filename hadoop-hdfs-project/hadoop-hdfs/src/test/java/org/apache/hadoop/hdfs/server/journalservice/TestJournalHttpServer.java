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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.journalservice.JournalHttpServer;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.protocol.JournalInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;

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
   * Test Journal service Http Server by verifying the html page is accessible
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
   * Test hasCompleteJournalSegments with different log list combinations
   * 
   * @throws Exception
   */
  @Test
  public void testHasCompleteJournalSegments() throws Exception {
    List<RemoteEditLog> logs = new ArrayList<RemoteEditLog>();
    logs.add(new RemoteEditLog(3,6));
    logs.add(new RemoteEditLog(7,10));
    logs.add(new RemoteEditLog(11,12));

    assertTrue(FSEditLog.hasCompleteJournalSegments(logs, 3, 13));
    assertFalse(FSEditLog.hasCompleteJournalSegments(logs, 1, 13));
    assertFalse(FSEditLog.hasCompleteJournalSegments(logs, 13, 19));
    assertFalse(FSEditLog.hasCompleteJournalSegments(logs, 11, 19));

    logs.remove(1);
    assertFalse(FSEditLog.hasCompleteJournalSegments(logs, 3, 13));
    assertFalse(FSEditLog.hasCompleteJournalSegments(logs, 1, 13));
    assertFalse(FSEditLog.hasCompleteJournalSegments(logs, 3, 19));
  }
  
  private void copyNNFiles(MiniDFSCluster cluster, File dstDir)
      throws IOException {
    Collection<URI> editURIs = cluster.getNameEditsDirs(0);
    String firstURI = editURIs.iterator().next().getPath().toString();
    File nnDir = new File(new String(firstURI + "/current"));
   
    File allFiles[] = FileUtil.listFiles(nnDir);
    for (File f : allFiles) {
      IOUtils.copyBytes(new FileInputStream(f),
          new FileOutputStream(dstDir + "/" + f.getName()), 4096, true);
    }
  }

  /**
   * Test lagging Journal service copies edit segments from another Journal
   * service: 
   * 1. start one journal service 
   * 2. reboot namenode so more segments are created
   * 3. add another journal service and this new journal service should sync
   *    with the first journal service
   * 
   * @throws Exception
   */
  @Test
  public void testCopyEdits() throws Exception {
    MiniDFSCluster cluster = null;
    JournalService service1 = null, service2 = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();

      // start journal service
      conf.set(DFSConfigKeys.DFS_JOURNAL_EDITS_DIR_KEY, path1.getPath());
      InetSocketAddress nnAddr = cluster.getNameNode(0).getNameNodeAddress();
      InetSocketAddress serverAddr = NetUtils
          .createSocketAddr("localhost:50900");
      Journal j1 = new Journal(conf);
      JournalListener listener1 = new JournalDiskWriter(j1);
      service1 = new JournalService(conf, nnAddr, serverAddr,
          new InetSocketAddress(50901), listener1, j1);
      service1.start();

      // get namenode clusterID/layoutVersion/namespaceID
      StorageInfo si = service1.getJournal().getStorage();
      JournalInfo journalInfo = new JournalInfo(si.layoutVersion, si.clusterID,
          si.namespaceID);

      // restart namenode, so there will be one more journal segments
      cluster.restartNameNode();

      // TODO: remove file copy when NN can work with journal auto-machine
      copyNNFiles(cluster, new File(new String(path1.toString() + "/current")));

      // start another journal service that will do the sync
      conf.set(DFSConfigKeys.DFS_JOURNAL_ADDRESS_KEY, "localhost:50900");
      conf.set(DFSConfigKeys.DFS_JOURNAL_EDITS_DIR_KEY, path2.getPath());
      conf.set(DFSConfigKeys.DFS_JOURNAL_HTTP_ADDRESS_KEY,
          "localhost:50902, localhost:50901");
      Journal j2 = new Journal(conf);
      JournalListener listener2 = new JournalDiskWriter(j2);
      service2 = new JournalService(conf, nnAddr, new InetSocketAddress(50800),
          NetUtils.createSocketAddr("localhost:50902"), listener2, j2);
      service2.start();

      // give service2 sometime to sync
      Thread.sleep(5000);
      
      // TODO: change to sinceTxid to 1 after NN is modified to use journal
      // service to start
      RemoteEditLogManifest manifest2 = service2.getEditLogManifest(
          journalInfo, 3);
      assertTrue(manifest2.getLogs().size() > 0);

    } catch (IOException e) {
      LOG.error("Error in TestCopyEdits:", e);
      assertTrue(e.getLocalizedMessage(), false);
    } finally {
      if (cluster != null)
        cluster.shutdown();
      if (service1 != null)
        service1.stop();
      if (service2 != null)
        service2.stop();
    }
  }
}
