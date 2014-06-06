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

package org.apache.hadoop.tools;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ToolRunner;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

/**
 * Tests distcp in combination with HDFS XAttrs.
 */
public class TestDistCpWithXAttrs {

  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static FileSystem fs;
  
  //XAttrs
  private static final String name1 = "user.a1";
  private static final byte[] value1 = {0x31, 0x32, 0x33};
  private static final String name2 = "trusted.a2";
  private static final byte[] value2 = {0x37, 0x38, 0x39};
  private static final String name3 = "user.a3";
  private static final byte[] value3 = null;
  private static final String name4 = "user.a4";
  private static final byte[] value4 = null;
  
  private static final Path dir1 = new Path("/src/dir1");
  private static final Path subDir1 = new Path(dir1, "subdir1");
  private static final Path file1 = new Path("/src/file1");
  private static final Path dir2 = new Path("/src/dir2");
  private static final Path file2 = new Path(dir2, "file2");
  private static final Path file3 = new Path(dir2, "file3");
  private static final Path file4 = new Path(dir2, "file4");
  private static final Path dstDir1 = new Path("/dstPreserveXAttrs/dir1");
  private static final Path dstSubDir1 = new Path(dstDir1, "subdir1");
  private static final Path dstFile1 = new Path("/dstPreserveXAttrs/file1");
  private static final Path dstDir2 = new Path("/dstPreserveXAttrs/dir2");
  private static final Path dstFile2 = new Path(dstDir2, "file2");
  private static final Path dstFile3 = new Path(dstDir2, "file3");
  private static final Path dstFile4 = new Path(dstDir2, "file4");

  @BeforeClass
  public static void init() throws Exception {
    initCluster(true, true);
    fs.mkdirs(subDir1);
    fs.create(file1).close();
    fs.mkdirs(dir2);
    fs.create(file2).close();
    fs.create(file3).close();
    fs.create(file4).close();

    // dir1
    fs.setXAttr(dir1, name1, value1);
    fs.setXAttr(dir1, name2, value2);
    
    // subDir1
    fs.setXAttr(subDir1, name1, value1);
    fs.setXAttr(subDir1, name3, value3);
    
    // file1
    fs.setXAttr(file1, name1, value1);
    fs.setXAttr(file1, name2, value2);
    fs.setXAttr(file1, name3, value3);
    
    // dir2
    fs.setXAttr(dir2, name2, value2);
    
    // file2
    fs.setXAttr(file2, name1, value1);
    fs.setXAttr(file2, name4, value4);
    
    // file3
    fs.setXAttr(file3, name3, value3);
    fs.setXAttr(file3, name4, value4);
  }

  @AfterClass
  public static void shutdown() {
    IOUtils.cleanup(null, fs);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testPreserveXAttrs() throws Exception {
    assertRunDistCp(DistCpConstants.SUCCESS, "/dstPreserveXAttrs");

    // dstDir1
    Map<String, byte[]> xAttrs = Maps.newHashMap();
    xAttrs.put(name1, value1);
    xAttrs.put(name2, value2);
    assertXAttrs(dstDir1, xAttrs);
    
    // dstSubDir1
    xAttrs.clear();
    xAttrs.put(name1, value1);
    xAttrs.put(name3, new byte[0]);
    assertXAttrs(dstSubDir1, xAttrs);
    
    // dstFile1
    xAttrs.clear();
    xAttrs.put(name1, value1);
    xAttrs.put(name2, value2);
    xAttrs.put(name3, new byte[0]);
    assertXAttrs(dstFile1, xAttrs);
    
    // dstDir2
    xAttrs.clear();
    xAttrs.put(name2, value2);
    assertXAttrs(dstDir2, xAttrs);
    
    // dstFile2
    xAttrs.clear();
    xAttrs.put(name1, value1);
    xAttrs.put(name4, new byte[0]);
    assertXAttrs(dstFile2, xAttrs);
    
    // dstFile3
    xAttrs.clear();
    xAttrs.put(name3, new byte[0]);
    xAttrs.put(name4, new byte[0]);
    assertXAttrs(dstFile3, xAttrs);
    
    // dstFile4
    xAttrs.clear();
    assertXAttrs(dstFile4, xAttrs);
  }

  @Test
  public void testXAttrsNotEnabled() throws Exception {
    try {
      restart(false);
      assertRunDistCp(DistCpConstants.XATTRS_NOT_SUPPORTED, 
          "/dstXAttrsNotEnabled");
    } finally {
      restart(true);
    }
  }

  @Test
  public void testXAttrsNotImplemented() throws Exception {
    assertRunDistCp(DistCpConstants.XATTRS_NOT_SUPPORTED,
        "stubfs://dstXAttrsNotImplemented");
  }

  /**
   * Stub FileSystem implementation used for testing the case of attempting
   * distcp with XAttrs preserved on a file system that does not support XAttrs. 
   * The base class implementation throws UnsupportedOperationException for 
   * the XAttr methods, so we don't need to override them.
   */
  public static class StubFileSystem extends FileSystem {

    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
        Progressable progress) throws IOException {
      return null;
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
        boolean overwrite, int bufferSize, short replication, long blockSize,
        Progressable progress) throws IOException {
      return null;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
      return false;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      return null;
    }

    @Override
    public URI getUri() {
      return URI.create("stubfs:///");
    }

    @Override
    public Path getWorkingDirectory() {
      return new Path(Path.SEPARATOR);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
      return null;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
      return false;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      return null;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
      return false;
    }

    @Override
    public void setWorkingDirectory(Path dir) {
    }
  }

  /**
   * Asserts the XAttrs returned by getXAttrs for a specific path.
   * 
   * @param path String path to check
   * @param xAttrs XAttr[] expected xAttrs
   * @throws Exception if there is any error
   */
  private static void assertXAttrs(Path path, Map<String, byte[]> expectedXAttrs)
      throws Exception {
    Map<String, byte[]> xAttrs = fs.getXAttrs(path);
    assertEquals(expectedXAttrs.size(), xAttrs.size());
    Iterator<Entry<String, byte[]>> i = expectedXAttrs.entrySet().iterator();
    while (i.hasNext()) {
      Entry<String, byte[]> e = i.next();
      String name = e.getKey();
      byte[] value = e.getValue();
      if (value == null) {
        assertTrue(xAttrs.containsKey(name) && xAttrs.get(name) == null);
      } else {
        assertArrayEquals(value, xAttrs.get(name));
      }
    }
  }

  /**
   * Runs distcp from /src to specified destination, preserving XAttrs. Asserts
   * expected exit code.
   * 
   * @param int exitCode expected exit code
   * @param dst String distcp destination
   * @throws Exception if there is any error
   */
  private static void assertRunDistCp(int exitCode, String dst)
      throws Exception {
    DistCp distCp = new DistCp(conf, null);
    assertEquals(exitCode,
        ToolRunner.run(conf, distCp, new String[] { "-px", "/src", dst }));
  }

  /**
   * Initialize the cluster, wait for it to become active, and get FileSystem.
   * 
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param xAttrsEnabled if true, XAttr support is enabled
   * @throws Exception if any step fails
   */
  private static void initCluster(boolean format, boolean xAttrsEnabled)
      throws Exception {
    conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, xAttrsEnabled);
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "stubfs:///");
    conf.setClass("fs.stubfs.impl", StubFileSystem.class, FileSystem.class);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(format)
        .build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  /**
   * Restarts the cluster with XAttrs enabled or disabled.
   * 
   * @param xAttrsEnabled if true, XAttr support is enabled
   * @throws Exception if any step fails
   */
  private static void restart(boolean xAttrsEnabled) throws Exception {
    shutdown();
    initCluster(false, xAttrsEnabled);
  }
}
