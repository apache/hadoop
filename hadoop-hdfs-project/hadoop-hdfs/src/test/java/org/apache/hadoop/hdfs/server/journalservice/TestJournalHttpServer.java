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
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
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
import org.apache.hadoop.hdfs.server.journalservice.JournalHttpServer;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.net.NetUtils;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;

public class TestJournalHttpServer {
  public static final Log LOG = LogFactory
      .getLog(TestJournalHttpServer.class);

  static {
    ((Log4JLogger) JournalHttpServer.LOG).getLogger().setLevel(Level.ALL);
  }

  private Configuration conf;
  private File hdfsDir = null;
  private File path1;

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
    path1.mkdir();
    if (!path1.exists()) {
      throw new IOException("Couldn't create path in "
          + hdfsDir.getAbsolutePath());
    }

    System.out.println("configuring hdfsdir is " + hdfsDir.getAbsolutePath()
        + "; jn1Dir = " + path1.getPath());

    File path1current = new File(path1, "current");
    path1current.mkdir();
    if (!path1current.exists()) {
      throw new IOException("Couldn't create path " + path1current);
    }
  }

  /**
   * Test JN Http Server
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
      // TODO: remove the manual setting storage when JN is fully implemented
      URI uri = new URI(new String("file:" + path1.getPath()));
      List<URI> editsDirs = new ArrayList<URI>();
      editsDirs.add(uri);
      NNStorage storage = new NNStorage(conf, new ArrayList<URI>(), editsDirs);
      jns1 = new JournalHttpServer(conf, storage,
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
}
