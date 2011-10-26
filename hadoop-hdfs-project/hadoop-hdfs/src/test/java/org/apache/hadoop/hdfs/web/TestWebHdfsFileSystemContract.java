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
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

public class TestWebHdfsFileSystemContract extends FileSystemContractBaseTest {
  private static final Configuration conf = new Configuration();
  private static final MiniDFSCluster cluster;
  private String defaultWorkingDirectory;

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
    final String uri = WebHdfsFileSystem.SCHEME  + "://"
        + conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);

    //get file system as a non-superuser
    final UserGroupInformation current = UserGroupInformation.getCurrentUser();
    final UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        current.getShortUserName() + "x", new String[]{"user"});
    fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
      @Override
      public FileSystem run() throws Exception {
        return FileSystem.get(new URI(uri), conf);
      }
    });

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
  
  public void testGetFileBlockLocations() throws IOException {
    final String f = "/test/testGetFileBlockLocations";
    createFile(path(f));
    final BlockLocation[] computed = fs.getFileBlockLocations(new Path(f), 0L, 1L);
    final BlockLocation[] expected = cluster.getFileSystem().getFileBlockLocations(
        new Path(f), 0L, 1L);
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
    assertTrue(fs.getFileStatus(p).isDirectory());
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
}
