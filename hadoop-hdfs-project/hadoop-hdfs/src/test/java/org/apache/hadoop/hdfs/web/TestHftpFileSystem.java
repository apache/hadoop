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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.util.ServletUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHftpFileSystem {
  private static final String BASEDIR = System.getProperty("test.build.dir",
      "target/test-dir") + "/" + TestHftpFileSystem.class.getSimpleName();
  private static String keystoresDir;
  private static String sslConfDir;
  private static Configuration config = null;
  private static MiniDFSCluster cluster = null;
  private static String blockPoolId = null;
  private static String hftpUri = null;
  private FileSystem hdfs = null;
  private HftpFileSystem hftpFs = null;

  private static final Path[] TEST_PATHS = new Path[] {
      // URI does not encode, Request#getPathInfo returns /foo
      new Path("/foo;bar"),

      // URI does not encode, Request#getPathInfo returns verbatim
      new Path("/foo+"), new Path("/foo+bar/foo+bar"),
      new Path("/foo=bar/foo=bar"), new Path("/foo,bar/foo,bar"),
      new Path("/foo@bar/foo@bar"), new Path("/foo&bar/foo&bar"),
      new Path("/foo$bar/foo$bar"), new Path("/foo_bar/foo_bar"),
      new Path("/foo~bar/foo~bar"), new Path("/foo.bar/foo.bar"),
      new Path("/foo../bar/foo../bar"), new Path("/foo.../bar/foo.../bar"),
      new Path("/foo'bar/foo'bar"),
      new Path("/foo#bar/foo#bar"),
      new Path("/foo!bar/foo!bar"),
      // HDFS file names may not contain ":"

      // URI percent encodes, Request#getPathInfo decodes
      new Path("/foo bar/foo bar"), new Path("/foo?bar/foo?bar"),
      new Path("/foo\">bar/foo\">bar"), };

  @BeforeClass
  public static void setUp() throws Exception {
    config = new Configuration();
    cluster = new MiniDFSCluster.Builder(config).numDataNodes(2).build();
    blockPoolId = cluster.getNamesystem().getBlockPoolId();
    hftpUri = "hftp://"
        + config.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    keystoresDir = new File(BASEDIR).getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestHftpFileSystem.class);

    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, config, false);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
    FileUtil.fullyDelete(new File(BASEDIR));
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
  }

  @Before
  public void initFileSystems() throws IOException {
    hdfs = cluster.getFileSystem();
    hftpFs = (HftpFileSystem) new Path(hftpUri).getFileSystem(config);
    // clear out the namespace
    for (FileStatus stat : hdfs.listStatus(new Path("/"))) {
      hdfs.delete(stat.getPath(), true);
    }
  }

  @After
  public void resetFileSystems() throws IOException {
    FileSystem.closeAll();
  }

  /**
   * Test file creation and access with file names that need encoding.
   */
  @Test
  public void testFileNameEncoding() throws IOException, URISyntaxException {
    for (Path p : TEST_PATHS) {
      // Create and access the path (data and streamFile servlets)
      FSDataOutputStream out = hdfs.create(p, true);
      out.writeBytes("0123456789");
      out.close();
      FSDataInputStream in = hftpFs.open(p);
      assertEquals('0', in.read());
      in.close();

      // Check the file status matches the path. Hftp returns a FileStatus
      // with the entire URI, extract the path part.
      assertEquals(p, new Path(hftpFs.getFileStatus(p).getPath().toUri()
          .getPath()));

      // Test list status (listPath servlet)
      assertEquals(1, hftpFs.listStatus(p).length);

      // Test content summary (contentSummary servlet)
      assertNotNull("No content summary", hftpFs.getContentSummary(p));

      // Test checksums (fileChecksum and getFileChecksum servlets)
      assertNotNull("No file checksum", hftpFs.getFileChecksum(p));
    }
  }

  private void testDataNodeRedirect(Path path) throws IOException {
    // Create the file
    if (hdfs.exists(path)) {
      hdfs.delete(path, true);
    }
    FSDataOutputStream out = hdfs.create(path, (short) 1);
    out.writeBytes("0123456789");
    out.close();

    // Get the path's block location so we can determine
    // if we were redirected to the right DN.
    BlockLocation[] locations = hdfs.getFileBlockLocations(path, 0, 10);
    String xferAddr = locations[0].getNames()[0];

    // Connect to the NN to get redirected
    URL u = hftpFs.getNamenodeURL(
        "/data" + ServletUtil.encodePath(path.toUri().getPath()),
        "ugi=userx,groupy");
    HttpURLConnection conn = (HttpURLConnection) u.openConnection();
    HttpURLConnection.setFollowRedirects(true);
    conn.connect();
    conn.getInputStream();

    boolean checked = false;
    // Find the datanode that has the block according to locations
    // and check that the URL was redirected to this DN's info port
    for (DataNode node : cluster.getDataNodes()) {
      DatanodeRegistration dnR = DataNodeTestUtils.getDNRegistrationForBP(node,
          blockPoolId);
      if (dnR.getXferAddr().equals(xferAddr)) {
        checked = true;
        assertEquals(dnR.getInfoPort(), conn.getURL().getPort());
      }
    }
    assertTrue("The test never checked that location of "
        + "the block and hftp desitnation are the same", checked);
  }

  /**
   * Test that clients are redirected to the appropriate DN.
   */
  @Test
  public void testDataNodeRedirect() throws IOException {
    for (Path p : TEST_PATHS) {
      testDataNodeRedirect(p);
    }
  }

  /**
   * Tests getPos() functionality.
   */
  @Test
  public void testGetPos() throws IOException {
    final Path testFile = new Path("/testfile+1");
    // Write a test file.
    FSDataOutputStream out = hdfs.create(testFile, true);
    out.writeBytes("0123456789");
    out.close();

    FSDataInputStream in = hftpFs.open(testFile);

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
  @Test
  public void testSeek() throws IOException {
    final Path testFile = new Path("/testfile+1");
    FSDataOutputStream out = hdfs.create(testFile, true);
    out.writeBytes("0123456789");
    out.close();
    FSDataInputStream in = hftpFs.open(testFile);
    in.seek(7);
    assertEquals('7', in.read());
    in.close();
  }

  @Test
  public void testReadClosedStream() throws IOException {
    final Path testFile = new Path("/testfile+2");
    FSDataOutputStream os = hdfs.create(testFile, true);
    os.writeBytes("0123456789");
    os.close();

    // ByteRangeInputStream delays opens until reads. Make sure it doesn't
    // open a closed stream that has never been opened
    FSDataInputStream in = hftpFs.open(testFile);
    in.close();
    checkClosedStream(in);
    checkClosedStream(in.getWrappedStream());

    // force the stream to connect and then close it
    in = hftpFs.open(testFile);
    int ch = in.read();
    assertEquals('0', ch);
    in.close();
    checkClosedStream(in);
    checkClosedStream(in.getWrappedStream());

    // make sure seeking doesn't automagically reopen the stream
    in.seek(4);
    checkClosedStream(in);
    checkClosedStream(in.getWrappedStream());
  }

  private void checkClosedStream(InputStream is) {
    IOException ioe = null;
    try {
      is.read();
    } catch (IOException e) {
      ioe = e;
    }
    assertNotNull("No exception on closed read", ioe);
    assertEquals("Stream closed", ioe.getMessage());
  }

  @Test
  public void testHftpDefaultPorts() throws IOException {
    Configuration conf = new Configuration();
    URI uri = URI.create("hftp://localhost");
    HftpFileSystem fs = (HftpFileSystem) FileSystem.get(uri, conf);

    assertEquals(DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT,
        fs.getDefaultPort());

    assertEquals(uri, fs.getUri());

    // HFTP uses http to get the token so canonical service name should
    // return the http port.
    assertEquals("127.0.0.1:" + DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT,
        fs.getCanonicalServiceName());
  }

  @Test
  public void testHftpCustomDefaultPorts() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY, 123);

    URI uri = URI.create("hftp://localhost");
    HftpFileSystem fs = (HftpFileSystem) FileSystem.get(uri, conf);

    assertEquals(123, fs.getDefaultPort());

    assertEquals(uri, fs.getUri());

    // HFTP uses http to get the token so canonical service name should
    // return the http port.
    assertEquals("127.0.0.1:123", fs.getCanonicalServiceName());
  }

  @Test
  public void testHftpCustomUriPortWithDefaultPorts() throws IOException {
    Configuration conf = new Configuration();
    URI uri = URI.create("hftp://localhost:123");
    HftpFileSystem fs = (HftpFileSystem) FileSystem.get(uri, conf);

    assertEquals(DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT,
        fs.getDefaultPort());

    assertEquals(uri, fs.getUri());
    assertEquals("127.0.0.1:123", fs.getCanonicalServiceName());
  }

  @Test
  public void testHftpCustomUriPortWithCustomDefaultPorts() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY, 123);

    URI uri = URI.create("hftp://localhost:789");
    HftpFileSystem fs = (HftpFileSystem) FileSystem.get(uri, conf);

    assertEquals(123, fs.getDefaultPort());

    assertEquals(uri, fs.getUri());
    assertEquals("127.0.0.1:789", fs.getCanonicalServiceName());
  }

  @Test
  public void testTimeout() throws IOException {
    Configuration conf = new Configuration();
    URI uri = URI.create("hftp://localhost");
    HftpFileSystem fs = (HftpFileSystem) FileSystem.get(uri, conf);
    URLConnection conn = fs.connectionFactory.openConnection(new URL(
        "http://localhost"));
    assertEquals(URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
        conn.getConnectTimeout());
    assertEquals(URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
        conn.getReadTimeout());
  }

  // /

  @Test
  public void testHsftpDefaultPorts() throws IOException {
    Configuration conf = new Configuration();
    URI uri = URI.create("hsftp://localhost");
    HsftpFileSystem fs = (HsftpFileSystem) FileSystem.get(uri, conf);

    assertEquals(DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_DEFAULT,
        fs.getDefaultPort());

    assertEquals(uri, fs.getUri());
    assertEquals("127.0.0.1:" + DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_DEFAULT,
        fs.getCanonicalServiceName());
  }

  @Test
  public void testHsftpCustomDefaultPorts() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY, 123);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_KEY, 456);

    URI uri = URI.create("hsftp://localhost");
    HsftpFileSystem fs = (HsftpFileSystem) FileSystem.get(uri, conf);

    assertEquals(456, fs.getDefaultPort());

    assertEquals(uri, fs.getUri());
    assertEquals("127.0.0.1:456", fs.getCanonicalServiceName());
  }

  @Test
  public void testHsftpCustomUriPortWithDefaultPorts() throws IOException {
    Configuration conf = new Configuration();
    URI uri = URI.create("hsftp://localhost:123");
    HsftpFileSystem fs = (HsftpFileSystem) FileSystem.get(uri, conf);

    assertEquals(DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_DEFAULT,
        fs.getDefaultPort());

    assertEquals(uri, fs.getUri());
    assertEquals("127.0.0.1:123", fs.getCanonicalServiceName());
  }

  @Test
  public void testHsftpCustomUriPortWithCustomDefaultPorts() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY, 123);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_KEY, 456);

    URI uri = URI.create("hsftp://localhost:789");
    HsftpFileSystem fs = (HsftpFileSystem) FileSystem.get(uri, conf);

    assertEquals(456, fs.getDefaultPort());

    assertEquals(uri, fs.getUri());
    assertEquals("127.0.0.1:789", fs.getCanonicalServiceName());
  }
}
