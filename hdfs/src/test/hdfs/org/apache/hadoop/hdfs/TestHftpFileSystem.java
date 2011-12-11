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

package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.Random;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.log4j.Level;

/**
 * Unittest for HftpFileSystem.
 *
 */
public class TestHftpFileSystem extends TestCase {
  private static final Random RAN = new Random();
  private static final Path TEST_FILE = new Path("/testfile+1");
  
  private static Configuration config = null;
  private static MiniDFSCluster cluster = null;
  private static FileSystem hdfs = null;
  private static HftpFileSystem hftpFs = null;
  
  /**
   * Setup hadoop mini-cluster for test.
   */
  private static void oneTimeSetUp() throws IOException {
    ((Log4JLogger)HftpFileSystem.LOG).getLogger().setLevel(Level.ALL);

    final long seed = RAN.nextLong();
    System.out.println("seed=" + seed);
    RAN.setSeed(seed);

    config = new Configuration();
    config.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, "localhost");

    cluster = new MiniDFSCluster.Builder(config).numDataNodes(2).build();
    hdfs = cluster.getFileSystem();
    final String hftpuri = 
      "hftp://" + config.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    hftpFs = (HftpFileSystem) new Path(hftpuri).getFileSystem(config);
  }
  
  /**
   * Shutdown the hadoop mini-cluster.
   */
  private static void oneTimeTearDown() throws IOException {
    hdfs.close();
    hftpFs.close();
    cluster.shutdown();
  }
  
  public TestHftpFileSystem(String name) {
    super(name);
  }

  /**
   * For one time setup / teardown.
   */
  public static Test suite() {
    TestSuite suite = new TestSuite();
    
    suite.addTestSuite(TestHftpFileSystem.class);
    
    return new TestSetup(suite) {
      @Override
      protected void setUp() throws IOException {
        oneTimeSetUp();
      }
      
      @Override
      protected void tearDown() throws IOException {
        oneTimeTearDown();
      }
    };
  }
  
  public void testDataNodeRedirect() throws Exception {
    if (hdfs.exists(TEST_FILE)) {
      hdfs.delete(TEST_FILE, true);
    }
    FSDataOutputStream out = hdfs.create(TEST_FILE, (short) 1);
    out.writeBytes("0123456789");
    out.close();
    
    BlockLocation[] locations = 
        hdfs.getFileBlockLocations(TEST_FILE, 0, 10);
    String locationName = locations[0].getNames()[0];
    URL u = hftpFs.getNamenodeFileURL(TEST_FILE);
    HttpURLConnection conn = (HttpURLConnection)u.openConnection();
    conn.setFollowRedirects(true);
    conn.connect();
    conn.getInputStream();
    boolean checked = false;
    // Find the datanode that has the block according to locations
    // and check that the URL was redirected to this DN's info port
    for (DataNode node : cluster.getDataNodes()) {
      if (node.getDatanodeRegistration().getName().equals(locationName)) {
        checked = true;
        assertEquals(node.getDatanodeRegistration().getInfoPort(),
                    conn.getURL().getPort());
      }
    }
    assertTrue("The test never checked that location of " + 
              "the block and hftp desitnation are the same", checked);
  }
  /**
   * Tests getPos() functionality.
   */
  public void testGetPos() throws Exception {
    // Write a test file.
    FSDataOutputStream out = hdfs.create(TEST_FILE, true);
    out.writeBytes("0123456789");
    out.close();
    
    FSDataInputStream in = hftpFs.open(TEST_FILE);
    
    // Test read().
    for (int i = 0; i < 5; ++i) {
      assertEquals(i, in.getPos());
      in.read();
    }
    
    // Test read(b, off, len).
    assertEquals(5, in.getPos());
    byte[] buffer = new byte[10];
    assertEquals(2, in.read(buffer, 0, 2));
    assertEquals(7, in.getPos());
    
    // Test read(b).
    int bytesRead = in.read(buffer);
    assertEquals(7 + bytesRead, in.getPos());
    
    // Test EOF.
    for (int i = 0; i < 100; ++i) {
      in.read();
    }
    assertEquals(10, in.getPos());
    in.close();
  }
  
  /**
   * Tests seek().
   */
  public void testSeek() throws Exception {
    // Write a test file.
    FSDataOutputStream out = hdfs.create(TEST_FILE, true);
    out.writeBytes("0123456789");
    out.close();
    
    FSDataInputStream in = hftpFs.open(TEST_FILE);
    in.seek(7);
    assertEquals('7', in.read());
  }
}
