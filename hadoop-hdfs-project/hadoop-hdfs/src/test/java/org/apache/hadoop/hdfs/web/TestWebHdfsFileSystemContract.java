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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.web.resources.*;
import org.apache.hadoop.hdfs.web.resources.NamenodeAddressParam;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

public class TestWebHdfsFileSystemContract extends FileSystemContractBaseTest {
  private static final Configuration conf = new Configuration();
  private static final MiniDFSCluster cluster;
  private String defaultWorkingDirectory;
  
  private UserGroupInformation ugi;

  static {
    conf.setBoolean(DFSConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      cluster.waitActive();

      //change root permission to 777
      cluster.getFileSystem().setPermission(
          new Path("/"), new FsPermission((short)0777));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void setUp() throws Exception {
    //get file system as a non-superuser
    final UserGroupInformation current = UserGroupInformation.getCurrentUser();
    ugi = UserGroupInformation.createUserForTesting(
        current.getShortUserName() + "x", new String[]{"user"});
    fs = WebHdfsTestUtil.getWebHdfsFileSystemAs(ugi, conf, WebHdfsFileSystem.SCHEME);
    defaultWorkingDirectory = fs.getWorkingDirectory().toUri().getPath();
  }

  @Override
  protected String getDefaultWorkingDirectory() {
    return defaultWorkingDirectory;
  }

  /** HDFS throws AccessControlException
   * when calling exist(..) on a path /foo/bar/file
   * but /foo/bar is indeed a file in HDFS.
   */
  @Override
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    Path testDir = path("/test/hadoop");
    assertFalse(fs.exists(testDir));
    assertTrue(fs.mkdirs(testDir));
    assertTrue(fs.exists(testDir));
    
    createFile(path("/test/hadoop/file"));
    
    Path testSubDir = path("/test/hadoop/file/subdir");
    try {
      fs.mkdirs(testSubDir);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    try {
      assertFalse(fs.exists(testSubDir));
    } catch(AccessControlException e) {
      // also okay for HDFS.
    }    
    
    Path testDeepSubDir = path("/test/hadoop/file/deep/sub/dir");
    try {
      fs.mkdirs(testDeepSubDir);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    try {
      assertFalse(fs.exists(testDeepSubDir));
    } catch(AccessControlException e) {
      // also okay for HDFS.
    }    
  }
  
  //the following are new tests (i.e. not over-riding the super class methods)

  public void testGetFileBlockLocations() throws IOException {
    final String f = "/test/testGetFileBlockLocations";
    createFile(path(f));
    final BlockLocation[] computed = fs.getFileBlockLocations(new Path(f), 0L, 1L);
    final BlockLocation[] expected = cluster.getFileSystem().getFileBlockLocations(
        new Path(f), 0L, 1L);
    assertEquals(expected.length, computed.length);
    for (int i = 0; i < computed.length; i++) {
      assertEquals(expected[i].toString(), computed[i].toString());
      // Check names
      String names1[] = expected[i].getNames();
      String names2[] = computed[i].getNames();
      Arrays.sort(names1);
      Arrays.sort(names2);
      Assert.assertArrayEquals("Names differ", names1, names2);
      // Check topology
      String topos1[] = expected[i].getTopologyPaths();
      String topos2[] = computed[i].getTopologyPaths();
      Arrays.sort(topos1);
      Arrays.sort(topos2);
      Assert.assertArrayEquals("Topology differs", topos1, topos2);
    }
  }

  public void testCaseInsensitive() throws IOException {
    final Path p = new Path("/test/testCaseInsensitive");
    final WebHdfsFileSystem webhdfs = (WebHdfsFileSystem)fs;
    final PutOpParam.Op op = PutOpParam.Op.MKDIRS;

    //replace query with mix case letters
    final URL url = webhdfs.toUrl(op, p);
    WebHdfsFileSystem.LOG.info("url      = " + url);
    final URL replaced = new URL(url.toString().replace(op.toQueryString(),
        "Op=mkDIrs"));
    WebHdfsFileSystem.LOG.info("replaced = " + replaced);

    //connect with the replaced URL.
    final HttpURLConnection conn = (HttpURLConnection)replaced.openConnection();
    conn.setRequestMethod(op.getType().toString());
    conn.connect();
    final BufferedReader in = new BufferedReader(new InputStreamReader(
        conn.getInputStream()));
    for(String line; (line = in.readLine()) != null; ) {
      WebHdfsFileSystem.LOG.info("> " + line);
    }

    //check if the command successes.
    assertTrue(fs.getFileStatus(p).isDirectory());
  }

  public void testOpenNonExistFile() throws IOException {
    final Path p = new Path("/test/testOpenNonExistFile");
    //open it as a file, should get FileNotFoundException 
    try {
      fs.open(p).read();
      fail("Expected FileNotFoundException was not thrown");
    } catch(FileNotFoundException fnfe) {
      WebHdfsFileSystem.LOG.info("This is expected.", fnfe);
    }
  }

  public void testSeek() throws IOException {
    final Path dir = new Path("/test/testSeek");
    assertTrue(fs.mkdirs(dir));

    { //test zero file size
      final Path zero = new Path(dir, "zero");
      fs.create(zero).close();
      
      int count = 0;
      final FSDataInputStream in = fs.open(zero);
      for(; in.read() != -1; count++);
      in.close();
      assertEquals(0, count);
    }

    final byte[] mydata = new byte[1 << 20];
    new Random().nextBytes(mydata);

    final Path p = new Path(dir, "file");
    FSDataOutputStream out = fs.create(p, false, 4096, (short)3, 1L << 17);
    out.write(mydata, 0, mydata.length);
    out.close();

    final int one_third = mydata.length/3;
    final int two_third = one_third*2;

    { //test seek
      final int offset = one_third; 
      final int len = mydata.length - offset;
      final byte[] buf = new byte[len];

      final FSDataInputStream in = fs.open(p);
      in.seek(offset);
      
      //read all remaining data
      in.readFully(buf);
      in.close();
  
      for (int i = 0; i < buf.length; i++) {
        assertEquals("Position " + i + ", offset=" + offset + ", length=" + len,
            mydata[i + offset], buf[i]);
      }
    }

    { //test position read (read the data after the two_third location)
      final int offset = two_third; 
      final int len = mydata.length - offset;
      final byte[] buf = new byte[len];

      final FSDataInputStream in = fs.open(p);
      in.readFully(offset, buf);
      in.close();
  
      for (int i = 0; i < buf.length; i++) {
        assertEquals("Position " + i + ", offset=" + offset + ", length=" + len,
            mydata[i + offset], buf[i]);
      }
    }
  }


  public void testRootDir() throws IOException {
    final Path root = new Path("/");

    final WebHdfsFileSystem webhdfs = (WebHdfsFileSystem)fs;
    final URL url = webhdfs.toUrl(GetOpParam.Op.NULL, root);
    WebHdfsFileSystem.LOG.info("null url=" + url);
    Assert.assertTrue(url.toString().contains("v1"));

    //test root permission
    final FileStatus status = fs.getFileStatus(root);
    assertTrue(status != null);
    assertEquals(0777, status.getPermission().toShort());

    //delete root
    assertFalse(fs.delete(root, true));

    //create file using root path 
    try {
      final FSDataOutputStream out = fs.create(root);
      out.write(1);
      out.close();
      fail();
    } catch(IOException e) {
      WebHdfsFileSystem.LOG.info("This is expected.", e);
    }

    //open file using root path 
    try {
      final FSDataInputStream in = fs.open(root);
      in.read();
      fail();
    } catch(IOException e) {
      WebHdfsFileSystem.LOG.info("This is expected.", e);
    }
  }

  /**
   * Test get with length parameter greater than actual file length.
   */
  public void testLengthParamLongerThanFile() throws IOException {
    WebHdfsFileSystem webhdfs = (WebHdfsFileSystem)fs;
    Path dir = new Path("/test");
    assertTrue(webhdfs.mkdirs(dir));

    // Create a file with some content.
    Path testFile = new Path("/test/testLengthParamLongerThanFile");
    String content = "testLengthParamLongerThanFile";
    FSDataOutputStream testFileOut = webhdfs.create(testFile);
    try {
      testFileOut.write(content.getBytes("US-ASCII"));
    } finally {
      IOUtils.closeStream(testFileOut);
    }

    // Open the file, but request length longer than actual file length by 1.
    HttpOpParam.Op op = GetOpParam.Op.OPEN;
    URL url = webhdfs.toUrl(op, testFile, new LengthParam((long) (content
      .length() + 1)));
    HttpURLConnection conn = null;
    InputStream is = null;
    try {
      conn = (HttpURLConnection)url.openConnection();
      conn.setRequestMethod(op.getType().toString());
      conn.setDoOutput(op.getDoOutput());
      conn.setInstanceFollowRedirects(true);

      // Expect OK response and Content-Length header equal to actual length.
      assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
      assertEquals(String.valueOf(content.length()), conn.getHeaderField(
        "Content-Length"));

      // Check content matches.
      byte[] respBody = new byte[content.length()];
      is = conn.getInputStream();
      IOUtils.readFully(is, respBody, 0, content.length());
      assertEquals(content, new String(respBody, "US-ASCII"));
    } finally {
      IOUtils.closeStream(is);
      if (conn != null) {
        conn.disconnect();
      }
    }
  }

  /**
   * Test get with offset and length parameters that combine to request a length
   * greater than actual file length.
   */
  public void testOffsetPlusLengthParamsLongerThanFile() throws IOException {
    WebHdfsFileSystem webhdfs = (WebHdfsFileSystem)fs;
    Path dir = new Path("/test");
    assertTrue(webhdfs.mkdirs(dir));

    // Create a file with some content.
    Path testFile = new Path("/test/testOffsetPlusLengthParamsLongerThanFile");
    String content = "testOffsetPlusLengthParamsLongerThanFile";
    FSDataOutputStream testFileOut = webhdfs.create(testFile);
    try {
      testFileOut.write(content.getBytes("US-ASCII"));
    } finally {
      IOUtils.closeStream(testFileOut);
    }

    // Open the file, but request offset starting at 1 and length equal to file
    // length.  Considering the offset, this is longer than the actual content.
    HttpOpParam.Op op = GetOpParam.Op.OPEN;
    URL url = webhdfs.toUrl(op, testFile, new LengthParam(Long.valueOf(
      content.length())), new OffsetParam(1L));
    HttpURLConnection conn = null;
    InputStream is = null;
    try {
      conn = (HttpURLConnection)url.openConnection();
      conn.setRequestMethod(op.getType().toString());
      conn.setDoOutput(op.getDoOutput());
      conn.setInstanceFollowRedirects(true);

      // Expect OK response and Content-Length header equal to actual length.
      assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
      assertEquals(String.valueOf(content.length() - 1), conn.getHeaderField(
        "Content-Length"));

      // Check content matches.
      byte[] respBody = new byte[content.length() - 1];
      is = conn.getInputStream();
      IOUtils.readFully(is, respBody, 0, content.length() - 1);
      assertEquals(content.substring(1), new String(respBody, "US-ASCII"));
    } finally {
      IOUtils.closeStream(is);
      if (conn != null) {
        conn.disconnect();
      }
    }
  }

  public void testResponseCode() throws IOException {
    final WebHdfsFileSystem webhdfs = (WebHdfsFileSystem)fs;
    final Path root = new Path("/");
    final Path dir = new Path("/test/testUrl");
    assertTrue(webhdfs.mkdirs(dir));
    final Path file = new Path("/test/file");
    final FSDataOutputStream out = webhdfs.create(file);
    out.write(1);
    out.close();

    {//test GETHOMEDIRECTORY
      final URL url = webhdfs.toUrl(GetOpParam.Op.GETHOMEDIRECTORY, root);
      final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      final Map<?, ?> m = WebHdfsTestUtil.connectAndGetJson(
          conn, HttpServletResponse.SC_OK);
      assertEquals(WebHdfsFileSystem.getHomeDirectoryString(ugi),
          m.get(Path.class.getSimpleName()));
      conn.disconnect();
    }

    {//test GETHOMEDIRECTORY with unauthorized doAs
      final URL url = webhdfs.toUrl(GetOpParam.Op.GETHOMEDIRECTORY, root,
          new DoAsParam(ugi.getShortUserName() + "proxy"));
      final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.connect();
      assertEquals(HttpServletResponse.SC_FORBIDDEN, conn.getResponseCode());
      conn.disconnect();
    }

    {//test set owner with empty parameters
      final URL url = webhdfs.toUrl(PutOpParam.Op.SETOWNER, dir);
      final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.connect();
      assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
      conn.disconnect();
    }

    {//test set replication on a directory
      final HttpOpParam.Op op = PutOpParam.Op.SETREPLICATION;
      final URL url = webhdfs.toUrl(op, dir);
      final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod(op.getType().toString());
      conn.connect();
      assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
      
      assertFalse(webhdfs.setReplication(dir, (short)1));
      conn.disconnect();
    }

    {//test get file status for a non-exist file.
      final Path p = new Path(dir, "non-exist");
      final URL url = webhdfs.toUrl(GetOpParam.Op.GETFILESTATUS, p);
      final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.connect();
      assertEquals(HttpServletResponse.SC_NOT_FOUND, conn.getResponseCode());
      conn.disconnect();
    }

    {//test set permission with empty parameters
      final HttpOpParam.Op op = PutOpParam.Op.SETPERMISSION;
      final URL url = webhdfs.toUrl(op, dir);
      final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod(op.getType().toString());
      conn.connect();
      assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
      assertEquals(0, conn.getContentLength());
      assertEquals(MediaType.APPLICATION_OCTET_STREAM, conn.getContentType());
      assertEquals((short)0755, webhdfs.getFileStatus(dir).getPermission().toShort());
      conn.disconnect();
    }

    {//test append.
      AppendTestUtil.testAppend(fs, new Path(dir, "append"));
    }

    {//test NamenodeAddressParam not set.
      final HttpOpParam.Op op = PutOpParam.Op.CREATE;
      final URL url = webhdfs.toUrl(op, dir);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod(op.getType().toString());
      conn.setDoOutput(false);
      conn.setInstanceFollowRedirects(false);
      conn.connect();
      final String redirect = conn.getHeaderField("Location");
      conn.disconnect();

      //remove NamenodeAddressParam
      WebHdfsFileSystem.LOG.info("redirect = " + redirect);
      final int i = redirect.indexOf(NamenodeAddressParam.NAME);
      final int j = redirect.indexOf("&", i);
      String modified = redirect.substring(0, i - 1) + redirect.substring(j);
      WebHdfsFileSystem.LOG.info("modified = " + modified);

      //connect to datanode
      conn = (HttpURLConnection)new URL(modified).openConnection();
      conn.setRequestMethod(op.getType().toString());
      conn.setDoOutput(op.getDoOutput());
      conn.connect();
      assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
    }

    {//test jsonParse with non-json type.
      final HttpOpParam.Op op = GetOpParam.Op.OPEN;
      final URL url = webhdfs.toUrl(op, file);
      final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod(op.getType().toString());
      conn.connect();

      try {
        WebHdfsFileSystem.jsonParse(conn, false);
        fail();
      } catch(IOException ioe) {
        WebHdfsFileSystem.LOG.info("GOOD", ioe);
      }
      conn.disconnect();
    }

    {//test create with path containing spaces
      HttpOpParam.Op op = PutOpParam.Op.CREATE;
      Path path = new Path("/test/path with spaces");
      URL url = webhdfs.toUrl(op, path);
      HttpURLConnection conn = (HttpURLConnection)url.openConnection();
      conn.setRequestMethod(op.getType().toString());
      conn.setDoOutput(false);
      conn.setInstanceFollowRedirects(false);
      final String redirect;
      try {
        conn.connect();
        assertEquals(HttpServletResponse.SC_TEMPORARY_REDIRECT,
          conn.getResponseCode());
        redirect = conn.getHeaderField("Location");
      } finally {
        conn.disconnect();
      }

      conn = (HttpURLConnection)new URL(redirect).openConnection();
      conn.setRequestMethod(op.getType().toString());
      conn.setDoOutput(op.getDoOutput());
      try {
        conn.connect();
        assertEquals(HttpServletResponse.SC_CREATED, conn.getResponseCode());
      } finally {
        conn.disconnect();
      }
    }
  }

  @Test
  public void testAccess() throws IOException, InterruptedException {
    Path p1 = new Path("/pathX");
    try {
      UserGroupInformation ugi = UserGroupInformation.createUserForTesting("alpha",
          new String[]{"beta"});
      WebHdfsFileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystemAs(ugi, conf,
          WebHdfsFileSystem.SCHEME);

      fs.mkdirs(p1);
      fs.setPermission(p1, new FsPermission((short) 0444));
      fs.access(p1, FsAction.READ);
      try {
        fs.access(p1, FsAction.WRITE);
        fail("The access call should have failed.");
      } catch (AccessControlException e) {
        // expected
      }

      Path badPath = new Path("/bad");
      try {
        fs.access(badPath, FsAction.READ);
        fail("The access call should have failed");
      } catch (FileNotFoundException e) {
        // expected
      }
    } finally {
      fs.delete(p1, true);
    }
  }
}
