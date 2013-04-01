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

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.NetUtilsTestResolver;
import org.apache.hadoop.util.Progressable;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFileSystemCanonicalization {
  static String[] authorities = {
    "myfs://host",
    "myfs://host.a",
    "myfs://host.a.b",
  };

  static String[] ips = {
    "myfs://127.0.0.1"
  };


  @BeforeClass
  public static void initialize() throws Exception {
    NetUtilsTestResolver.install();
  }

  // no ports

  @Test
  public void testShortAuthority() throws Exception {
    FileSystem fs = getVerifiedFS("myfs://host", "myfs://host.a.b:123");
    verifyPaths(fs, authorities, -1, true);
    verifyPaths(fs, authorities, 123, true);
    verifyPaths(fs, authorities, 456, false);
    verifyPaths(fs, ips, -1, false);
    verifyPaths(fs, ips, 123, false);
    verifyPaths(fs, ips, 456, false);
  }

  @Test
  public void testPartialAuthority() throws Exception {
    FileSystem fs = getVerifiedFS("myfs://host.a", "myfs://host.a.b:123");
    verifyPaths(fs, authorities, -1, true);
    verifyPaths(fs, authorities, 123, true);
    verifyPaths(fs, authorities, 456, false);
    verifyPaths(fs, ips, -1, false);
    verifyPaths(fs, ips, 123, false);
    verifyPaths(fs, ips, 456, false);
  }

  @Test
  public void testFullAuthority() throws Exception {
    FileSystem fs = getVerifiedFS("myfs://host.a.b", "myfs://host.a.b:123");
    verifyPaths(fs, authorities, -1, true);
    verifyPaths(fs, authorities, 123, true);
    verifyPaths(fs, authorities, 456, false);
    verifyPaths(fs, ips, -1, false);
    verifyPaths(fs, ips, 123, false);
    verifyPaths(fs, ips, 456, false);
  }

  // with default ports
  
  @Test
  public void testShortAuthorityWithDefaultPort() throws Exception {
    FileSystem fs = getVerifiedFS("myfs://host:123", "myfs://host.a.b:123");
    verifyPaths(fs, authorities, -1, true);
    verifyPaths(fs, authorities, 123, true);
    verifyPaths(fs, authorities, 456, false);
    verifyPaths(fs, ips, -1, false);
    verifyPaths(fs, ips, 123, false);
    verifyPaths(fs, ips, 456, false);
  }

  @Test
  public void testPartialAuthorityWithDefaultPort() throws Exception {
    FileSystem fs = getVerifiedFS("myfs://host.a:123", "myfs://host.a.b:123");
    verifyPaths(fs, authorities, -1, true);
    verifyPaths(fs, authorities, 123, true);
    verifyPaths(fs, authorities, 456, false);
    verifyPaths(fs, ips, -1, false);
    verifyPaths(fs, ips, 123, false);
    verifyPaths(fs, ips, 456, false);
  }

  @Test
  public void testFullAuthorityWithDefaultPort() throws Exception {
    FileSystem fs = getVerifiedFS("myfs://host.a.b:123", "myfs://host.a.b:123");
    verifyPaths(fs, authorities, -1, true);
    verifyPaths(fs, authorities, 123, true);
    verifyPaths(fs, authorities, 456, false);
    verifyPaths(fs, ips, -1, false);
    verifyPaths(fs, ips, 123, false);
    verifyPaths(fs, ips, 456, false);
  }

  // with non-standard ports
  
  @Test
  public void testShortAuthorityWithOtherPort() throws Exception {
    FileSystem fs = getVerifiedFS("myfs://host:456", "myfs://host.a.b:456");
    verifyPaths(fs, authorities, -1, false);
    verifyPaths(fs, authorities, 123, false);
    verifyPaths(fs, authorities, 456, true);
    verifyPaths(fs, ips, -1, false);
    verifyPaths(fs, ips, 123, false);
    verifyPaths(fs, ips, 456, false);
  }

  @Test
  public void testPartialAuthorityWithOtherPort() throws Exception {
    FileSystem fs = getVerifiedFS("myfs://host.a:456", "myfs://host.a.b:456");
    verifyPaths(fs, authorities, -1, false);
    verifyPaths(fs, authorities, 123, false);
    verifyPaths(fs, authorities, 456, true);
    verifyPaths(fs, ips, -1, false);
    verifyPaths(fs, ips, 123, false);
    verifyPaths(fs, ips, 456, false);
  }

  @Test
  public void testFullAuthorityWithOtherPort() throws Exception {
    FileSystem fs = getVerifiedFS("myfs://host.a.b:456", "myfs://host.a.b:456");
    verifyPaths(fs, authorities, -1, false);
    verifyPaths(fs, authorities, 123, false);
    verifyPaths(fs, authorities, 456, true);
    verifyPaths(fs, ips, -1, false);
    verifyPaths(fs, ips, 123, false);
    verifyPaths(fs, ips, 456, false);
  }

  // ips
  
  @Test
  public void testIpAuthority() throws Exception {
    FileSystem fs = getVerifiedFS("myfs://127.0.0.1", "myfs://127.0.0.1:123");
    verifyPaths(fs, authorities, -1, false);
    verifyPaths(fs, authorities, 123, false);
    verifyPaths(fs, authorities, 456, false);
    verifyPaths(fs, ips, -1, true);
    verifyPaths(fs, ips, 123, true);
    verifyPaths(fs, ips, 456, false);
  }

  @Test
  public void testIpAuthorityWithDefaultPort() throws Exception {
    FileSystem fs = getVerifiedFS("myfs://127.0.0.1:123", "myfs://127.0.0.1:123");
    verifyPaths(fs, authorities, -1, false);
    verifyPaths(fs, authorities, 123, false);
    verifyPaths(fs, authorities, 456, false);
    verifyPaths(fs, ips, -1, true);
    verifyPaths(fs, ips, 123, true);
    verifyPaths(fs, ips, 456, false);
  }

  @Test
  public void testIpAuthorityWithOtherPort() throws Exception {
    FileSystem fs = getVerifiedFS("myfs://127.0.0.1:456", "myfs://127.0.0.1:456");
    verifyPaths(fs, authorities, -1, false);
    verifyPaths(fs, authorities, 123, false);
    verifyPaths(fs, authorities, 456, false);
    verifyPaths(fs, ips, -1, false);
    verifyPaths(fs, ips, 123, false);
    verifyPaths(fs, ips, 456, true);
  }

  // bad stuff

  @Test
  public void testMismatchedSchemes() throws Exception {
    FileSystem fs = getVerifiedFS("myfs2://simple", "myfs2://simple:123");
    verifyPaths(fs, authorities, -1, false);
    verifyPaths(fs, authorities, 123, false);
    verifyPaths(fs, authorities, 456, false);
    verifyPaths(fs, ips, -1, false);
    verifyPaths(fs, ips, 123, false);
    verifyPaths(fs, ips, 456, false);
  }

  @Test
  public void testMismatchedHosts() throws Exception {
    FileSystem fs = getVerifiedFS("myfs://simple", "myfs://simple:123");
    verifyPaths(fs, authorities, -1, false);
    verifyPaths(fs, authorities, 123, false);
    verifyPaths(fs, authorities, 456, false);
    verifyPaths(fs, ips, -1, false);
    verifyPaths(fs, ips, 123, false);
    verifyPaths(fs, ips, 456, false);
  }

  @Test
  public void testNullAuthority() throws Exception {
    FileSystem fs = getVerifiedFS("myfs:///", "myfs:///");
    verifyPaths(fs, new String[]{ "myfs://" }, -1, true);
    verifyPaths(fs, authorities, -1, false);
    verifyPaths(fs, authorities, 123, false);
    verifyPaths(fs, authorities, 456, false);
    verifyPaths(fs, ips, -1, false);
    verifyPaths(fs, ips, 123, false);
    verifyPaths(fs, ips, 456, false);
  }

  @Test
  public void testAuthorityFromDefaultFS() throws Exception {
    Configuration config = new Configuration();
    String defaultFsKey = CommonConfigurationKeys.FS_DEFAULT_NAME_KEY;
    
    FileSystem fs = getVerifiedFS("myfs://host", "myfs://host.a.b:123", config);
    verifyPaths(fs, new String[]{ "myfs://" }, -1, false);

    config.set(defaultFsKey, "myfs://host");
    verifyPaths(fs, new String[]{ "myfs://" }, -1, true);

    config.set(defaultFsKey, "myfs2://host");
    verifyPaths(fs, new String[]{ "myfs://" }, -1, false);

    config.set(defaultFsKey, "myfs://host:123");
    verifyPaths(fs, new String[]{ "myfs://" }, -1, true);

    config.set(defaultFsKey, "myfs://host:456");
    verifyPaths(fs, new String[]{ "myfs://" }, -1, false);
  }

  FileSystem getVerifiedFS(String authority, String canonical) throws Exception {
    return getVerifiedFS(authority, canonical, new Configuration());
  }

  // create a fs from the authority, then check its uri against the given uri
  // and the canonical.  then try to fetch paths using the canonical
  FileSystem getVerifiedFS(String authority, String canonical, Configuration conf)
  throws Exception {
    URI uri = URI.create(authority);
    URI canonicalUri = URI.create(canonical);

    FileSystem fs = new DummyFileSystem(uri, conf);
    assertEquals(uri, fs.getUri());
    assertEquals(canonicalUri, fs.getCanonicalUri());
    verifyCheckPath(fs, "/file", true);
    return fs;
  }  
  
  void verifyPaths(FileSystem fs, String[] uris, int port, boolean shouldPass) {
    for (String uri : uris) {
      if (port != -1) uri += ":"+port;
      verifyCheckPath(fs, uri+"/file", shouldPass);
    }
  }

  void verifyCheckPath(FileSystem fs, String path, boolean shouldPass) {
    Path rawPath = new Path(path);
    Path fqPath = null;
    Exception e = null;
    try {
      fqPath = fs.makeQualified(rawPath);
    } catch (IllegalArgumentException iae) {
      e = iae;
    }
    if (shouldPass) {
      assertEquals(null, e);
      String pathAuthority = rawPath.toUri().getAuthority();
      if (pathAuthority == null) {
        pathAuthority = fs.getUri().getAuthority();
      }
      assertEquals(pathAuthority, fqPath.toUri().getAuthority());
    } else {
      assertNotNull("did not fail", e);
      assertEquals("Wrong FS: "+rawPath+", expected: "+fs.getUri(),
          e.getMessage());
    }
  }
    
  static class DummyFileSystem extends FileSystem {
    URI uri;
    static int defaultPort = 123;

    DummyFileSystem(URI uri, Configuration conf) throws IOException {
      this.uri = uri;
      setConf(conf);
    }
    
    @Override
    public URI getUri() {
      return uri;
    }

    @Override
    protected int getDefaultPort() {
      return defaultPort;
    }
    
    @Override
    protected URI canonicalizeUri(URI uri) {
      return NetUtils.getCanonicalUri(uri, getDefaultPort());
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      throw new IOException("not supposed to be here");
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
        boolean overwrite, int bufferSize, short replication, long blockSize,
        Progressable progress) throws IOException {
      throw new IOException("not supposed to be here");
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
        Progressable progress) throws IOException {
      throw new IOException("not supposed to be here");
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
      throw new IOException("not supposed to be here");
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
      throw new IOException("not supposed to be here");
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
      throw new IOException("not supposed to be here");
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
    }

    @Override
    public Path getWorkingDirectory() {
      return new Path("/");
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
      throw new IOException("not supposed to be here");
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      throw new IOException("not supposed to be here");
    }
  }
}
