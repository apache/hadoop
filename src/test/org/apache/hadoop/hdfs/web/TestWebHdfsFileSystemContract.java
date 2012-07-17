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
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.web.resources.DoAsParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;

public class TestWebHdfsFileSystemContract extends FileSystemContractBaseTest {
  private static final Configuration conf = new Configuration();
  private static final MiniDFSCluster cluster;
  private String defaultWorkingDirectory;
  
  private UserGroupInformation ugi;

  static {
    conf.setBoolean(DFSConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    try {
      cluster = new MiniDFSCluster(conf, 2, true, null);
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
    fs = WebHdfsTestUtil.getWebHdfsFileSystemAs(ugi, conf);
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

  //In trunk, testListStatusReturnsNullForNonExistentFile was replaced by
  //testListStatusThrowsExceptionForNonExistentFile.
  //
  //For WebHdfsFileSystem,
  //disable testListStatusReturnsNullForNonExistentFile
  //and add testListStatusThrowsExceptionForNonExistentFile below.
  @Override
  public void testListStatusReturnsNullForNonExistentFile() {}

  public void testListStatusThrowsExceptionForNonExistentFile() throws Exception {
    try {
      fs.listStatus(path("/test/hadoop/file"));
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException fnfe) {
      // expected
    }
  }
  
  //the following are new tests (i.e. not over-riding the super class methods)

  public void testGetFileBlockLocations() throws IOException {
    final String f = "/test/testGetFileBlockLocations";
    final Path p = path(f);
    createFile(p);
    final BlockLocation[] computed = fs.getFileBlockLocations(
        fs.getFileStatus(p), 0L, 1L);
    final FileSystem hdfs = cluster.getFileSystem();
    final BlockLocation[] expected = hdfs.getFileBlockLocations(
        hdfs.getFileStatus(new Path(f)), 0L, 1L);
    assertEquals(expected.length, computed.length);
    for(int i = 0; i < computed.length; i++) {
      assertEquals(expected[i].toString(), computed[i].toString());
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
    assertTrue(fs.getFileStatus(p).isDir());
  }

  public void testOpenNonExistFile() throws IOException {
    final Path p = new Path("/test/testOpenNonExistFile");
    //open it as a file, should get FileNotFoundException 
    try {
      final FSDataInputStream in = fs.open(p);
      in.read();
      fail();
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

    final Path p = new Path(dir, "file");
    createFile(p);

    final int one_third = data.length/3;
    final int two_third = one_third*2;

    { //test seek
      final int offset = one_third; 
      final int len = data.length - offset;
      final byte[] buf = new byte[len];

      final FSDataInputStream in = fs.open(p);
      in.seek(offset);
      
      //read all remaining data
      in.readFully(buf);
      in.close();
  
      for (int i = 0; i < buf.length; i++) {
        assertEquals("Position " + i + ", offset=" + offset + ", length=" + len,
            data[i + offset], buf[i]);
      }
    }

    { //test position read (read the data after the two_third location)
      final int offset = two_third; 
      final int len = data.length - offset;
      final byte[] buf = new byte[len];

      final FSDataInputStream in = fs.open(p);
      in.readFully(offset, buf);
      in.close();
  
      for (int i = 0; i < buf.length; i++) {
        assertEquals("Position " + i + ", offset=" + offset + ", length=" + len,
            data[i + offset], buf[i]);
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

  public void testResponseCode() throws IOException {
    final WebHdfsFileSystem webhdfs = (WebHdfsFileSystem)fs;
    final Path root = new Path("/");
    final Path dir = new Path("/test/testUrl");
    assertTrue(webhdfs.mkdirs(dir));

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
      assertEquals(HttpServletResponse.SC_UNAUTHORIZED, conn.getResponseCode());
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
  }
}
