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
package org.apache.hadoop.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.PathUtils;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test of the URL stream handler.
 */
public class TestUrlStreamHandler {

  private static final File TEST_ROOT_DIR =
      PathUtils.getTestDir(TestUrlStreamHandler.class);

  private static final FsUrlStreamHandlerFactory HANDLER_FACTORY
      = new FsUrlStreamHandlerFactory();

  @BeforeClass
  public static void setupHandler() {

    // Setup our own factory
    // setURLStreamHandlerFactor is can be set at most once in the JVM
    // the new URLStreamHandler is valid for all tests cases
    // in TestStreamHandler
    URL.setURLStreamHandlerFactory(HANDLER_FACTORY);
  }

  /**
   * Test opening and reading from an InputStream through a hdfs:// URL.
   * <p>
   * First generate a file with some content through the FileSystem API, then
   * try to open and read the file through the URL stream API.
   * 
   * @throws IOException
   */
  @Test
  public void testDfsUrls() throws IOException {

    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/thefile");

    try {
      byte[] fileContent = new byte[1024];
      for (int i = 0; i < fileContent.length; ++i)
        fileContent[i] = (byte) i;

      // First create the file through the FileSystem API
      OutputStream os = fs.create(filePath);
      os.write(fileContent);
      os.close();

      // Second, open and read the file content through the URL API
      URI uri = fs.getUri();
      URL fileURL =
          new URL(uri.getScheme(), uri.getHost(), uri.getPort(), filePath
              .toString());

      InputStream is = fileURL.openStream();
      assertNotNull(is);

      byte[] bytes = new byte[4096];
      assertEquals(1024, is.read(bytes));
      is.close();

      for (int i = 0; i < fileContent.length; ++i)
        assertEquals(fileContent[i], bytes[i]);

      // Cleanup: delete the file
      fs.delete(filePath, false);

    } finally {
      fs.close();
      cluster.shutdown();
    }

  }

  /**
   * Test opening and reading from an InputStream through a file:// URL.
   * 
   * @throws IOException
   * @throws URISyntaxException
   */
  @Test
  public void testFileUrls() throws IOException, URISyntaxException {
    // URLStreamHandler is already set in JVM by testDfsUrls() 
    Configuration conf = new HdfsConfiguration();

    // Locate the test temporary directory.
    if (!TEST_ROOT_DIR.exists()) {
      if (!TEST_ROOT_DIR.mkdirs())
        throw new IOException("Cannot create temporary directory: " + TEST_ROOT_DIR);
    }

    File tmpFile = new File(TEST_ROOT_DIR, "thefile");
    URI uri = tmpFile.toURI();

    FileSystem fs = FileSystem.get(uri, conf);

    try {
      byte[] fileContent = new byte[1024];
      for (int i = 0; i < fileContent.length; ++i)
        fileContent[i] = (byte) i;

      // First create the file through the FileSystem API
      OutputStream os = fs.create(new Path(uri.getPath()));
      os.write(fileContent);
      os.close();

      // Second, open and read the file content through the URL API.
      URL fileURL = uri.toURL();

      InputStream is = fileURL.openStream();
      assertNotNull(is);

      byte[] bytes = new byte[4096];
      assertEquals(1024, is.read(bytes));
      is.close();

      for (int i = 0; i < fileContent.length; ++i)
        assertEquals(fileContent[i], bytes[i]);

      // Cleanup: delete the file
      fs.delete(new Path(uri.getPath()), false);

    } finally {
      fs.close();
    }

  }

  @Test
  public void testHttpDefaultHandler() throws Throwable {
    assertNull("Handler for HTTP is the Hadoop one",
        HANDLER_FACTORY.createURLStreamHandler("http"));
  }

  @Test
  public void testHttpsDefaultHandler() throws Throwable {
    assertNull("Handler for HTTPS is the Hadoop one",
        HANDLER_FACTORY.createURLStreamHandler("https"));
  }

  @Test
  public void testUnknownProtocol() throws Throwable {
    assertNull("Unknown protocols are not handled",
        HANDLER_FACTORY.createURLStreamHandler("gopher"));
  }

}
