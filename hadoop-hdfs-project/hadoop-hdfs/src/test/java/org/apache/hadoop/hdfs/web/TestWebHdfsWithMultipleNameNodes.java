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
package org.apache.hadoop.hdfs.web;

import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test WebHDFS with multiple NameNodes
 */
public class TestWebHdfsWithMultipleNameNodes {
  static final Log LOG = WebHdfsTestUtil.LOG;

  static private void setLogLevel() {
    GenericTestUtils.setLogLevel(LOG, Level.ALL);
    GenericTestUtils.setLogLevel(NamenodeWebHdfsMethods.LOG, Level.ALL);

    DFSTestUtil.setNameNodeLogLevel(Level.ALL);
  }

  private static final Configuration conf = new HdfsConfiguration();
  private static MiniDFSCluster cluster;
  private static WebHdfsFileSystem[] webhdfs;

  @BeforeClass
  public static void setupTest() {
    setLogLevel();
    try {
      setupCluster(4, 3);
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void setupCluster(final int nNameNodes, final int nDataNodes)
      throws Exception {
    LOG.info("nNameNodes=" + nNameNodes + ", nDataNodes=" + nDataNodes);

    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(nNameNodes))
        .numDataNodes(nDataNodes)
        .build();
    cluster.waitActive();
    
    webhdfs = new WebHdfsFileSystem[nNameNodes];
    for(int i = 0; i < webhdfs.length; i++) {
      final InetSocketAddress addr = cluster.getNameNode(i).getHttpAddress();
      final String uri = WebHdfsConstants.WEBHDFS_SCHEME + "://"
          + addr.getHostName() + ":" + addr.getPort() + "/";
      webhdfs[i] = (WebHdfsFileSystem)FileSystem.get(new URI(uri), conf);
    }
  }

  @AfterClass
  public static void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private static String createString(String prefix, int i) {
    //The suffix is to make sure the strings have different lengths.
    final String suffix = "*********************".substring(0, i+1);
    return prefix + i + suffix + "\n";
  }

  private static String[] createStrings(String prefix, String name) {
    final String[] strings = new String[webhdfs.length]; 
    for(int i = 0; i < webhdfs.length; i++) {
      strings[i] = createString(prefix, i);
      LOG.info(name + "[" + i + "] = " + strings[i]);
    }
    return strings;
  }

  @Test
  public void testRedirect() throws Exception {
    final String dir = "/testRedirect/";
    final String filename = "file";
    final Path p = new Path(dir, filename);

    final String[] writeStrings = createStrings("write to webhdfs ", "write"); 
    final String[] appendStrings = createStrings("append to webhdfs ", "append"); 
    
    //test create: create a file for each namenode
    for(int i = 0; i < webhdfs.length; i++) {
      final FSDataOutputStream out = webhdfs[i].create(p);
      out.write(writeStrings[i].getBytes());
      out.close();
    }
    
    for(int i = 0; i < webhdfs.length; i++) {
      //check file length
      final long expected = writeStrings[i].length();
      Assert.assertEquals(expected, webhdfs[i].getFileStatus(p).getLen());
    }

    //test read: check file content for each namenode
    for(int i = 0; i < webhdfs.length; i++) {
      final FSDataInputStream in = webhdfs[i].open(p);
      for(int c, j = 0; (c = in.read()) != -1; j++) {
        Assert.assertEquals(writeStrings[i].charAt(j), c);
      }
      in.close();
    }

    //test append: append to the file for each namenode
    for(int i = 0; i < webhdfs.length; i++) {
      final FSDataOutputStream out = webhdfs[i].append(p);
      out.write(appendStrings[i].getBytes());
      out.close();
    }

    for(int i = 0; i < webhdfs.length; i++) {
      //check file length
      final long expected = writeStrings[i].length() + appendStrings[i].length();
      Assert.assertEquals(expected, webhdfs[i].getFileStatus(p).getLen());
    }

    //test read: check file content for each namenode
    for(int i = 0; i < webhdfs.length; i++) {
      final StringBuilder b = new StringBuilder(); 
      final FSDataInputStream in = webhdfs[i].open(p);
      for(int c; (c = in.read()) != -1; ) {
        b.append((char)c);
      }
      final int wlen = writeStrings[i].length();
      Assert.assertEquals(writeStrings[i], b.substring(0, wlen));
      Assert.assertEquals(appendStrings[i], b.substring(wlen));
      in.close();
    }
  }
}
