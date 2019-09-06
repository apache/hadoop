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

package org.apache.hadoop.fs.viewfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.viewfs.TestChRootedFileSystem.MockFileSystem;
import org.junit.*;

import static org.apache.hadoop.fs.viewfs.TestChRootedFileSystem.getChildFileSystem;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Verify that viewfs propagates certain methods to the underlying fs 
 */
public class TestViewFileSystemDelegation { //extends ViewFileSystemTestSetup {
  static Configuration conf;
  static FileSystem viewFs;
  static FakeFileSystem fs1;
  static FakeFileSystem fs2;

  @BeforeClass
  public static void setup() throws Exception {
    conf = ViewFileSystemTestSetup.createConfig();
    setupFileSystem(new URI("fs1:/"), FakeFileSystem.class);
    setupFileSystem(new URI("fs2:/"), FakeFileSystem.class);
    viewFs = FileSystem.get(FsConstants.VIEWFS_URI, conf);
    fs1 = (FakeFileSystem) getChildFileSystem((ViewFileSystem) viewFs,
        new URI("fs1:/"));
    fs2 = (FakeFileSystem) getChildFileSystem((ViewFileSystem) viewFs,
        new URI("fs2:/"));
  }

  static void setupFileSystem(URI uri, Class clazz)
      throws Exception {
    String scheme = uri.getScheme();
    conf.set("fs."+scheme+".impl", clazz.getName());
    FakeFileSystem fs = (FakeFileSystem)FileSystem.get(uri, conf);
    assertEquals(uri, fs.getUri());
    Path targetPath = new FileSystemTestHelper().getAbsoluteTestRootPath(fs);
    ConfigUtil.addLink(conf, "/mounts/"+scheme, targetPath.toUri());
  }

  private static void setupMockFileSystem(Configuration config, URI uri)
      throws Exception {
    String scheme = uri.getScheme();
    config.set("fs." + scheme + ".impl", MockFileSystem.class.getName());
    ConfigUtil.addLink(config, "/mounts/" + scheme, uri);
  }

  @Test
  public void testSanity() throws URISyntaxException {
    assertEquals(new URI("fs1:/").getScheme(), fs1.getUri().getScheme());
    assertEquals(new URI("fs1:/").getAuthority(), fs1.getUri().getAuthority());
    assertEquals(new URI("fs2:/").getScheme(), fs2.getUri().getScheme());
    assertEquals(new URI("fs2:/").getAuthority(), fs2.getUri().getAuthority());
  }
  
  @Test
  public void testVerifyChecksum() throws Exception {
    checkVerifyChecksum(false);
    checkVerifyChecksum(true);
  }

  /**
   * Tests that ViewFileSystem dispatches calls for every ACL method through the
   * mount table to the correct underlying FileSystem with all Path arguments
   * translated as required.
   */
  @Test
  public void testAclMethods() throws Exception {
    Configuration conf = ViewFileSystemTestSetup.createConfig();
    setupMockFileSystem(conf, new URI("mockfs1:/"));
    setupMockFileSystem(conf, new URI("mockfs2:/"));
    FileSystem viewFs = FileSystem.get(FsConstants.VIEWFS_URI, conf);
    FileSystem mockFs1 =
        ((MockFileSystem) getChildFileSystem((ViewFileSystem) viewFs,
            new URI("mockfs1:/"))).getRawFileSystem();
    FileSystem mockFs2 =
        ((MockFileSystem) getChildFileSystem((ViewFileSystem) viewFs,
            new URI("mockfs2:/"))).getRawFileSystem();

    Path viewFsPath1 = new Path("/mounts/mockfs1/a/b/c");
    Path mockFsPath1 = new Path("/a/b/c");
    Path viewFsPath2 = new Path("/mounts/mockfs2/d/e/f");
    Path mockFsPath2 = new Path("/d/e/f");
    List<AclEntry> entries = Collections.emptyList();

    viewFs.modifyAclEntries(viewFsPath1, entries);
    verify(mockFs1).modifyAclEntries(mockFsPath1, entries);
    viewFs.modifyAclEntries(viewFsPath2, entries);
    verify(mockFs2).modifyAclEntries(mockFsPath2, entries);

    viewFs.removeAclEntries(viewFsPath1, entries);
    verify(mockFs1).removeAclEntries(mockFsPath1, entries);
    viewFs.removeAclEntries(viewFsPath2, entries);
    verify(mockFs2).removeAclEntries(mockFsPath2, entries);

    viewFs.removeDefaultAcl(viewFsPath1);
    verify(mockFs1).removeDefaultAcl(mockFsPath1);
    viewFs.removeDefaultAcl(viewFsPath2);
    verify(mockFs2).removeDefaultAcl(mockFsPath2);

    viewFs.removeAcl(viewFsPath1);
    verify(mockFs1).removeAcl(mockFsPath1);
    viewFs.removeAcl(viewFsPath2);
    verify(mockFs2).removeAcl(mockFsPath2);

    viewFs.setAcl(viewFsPath1, entries);
    verify(mockFs1).setAcl(mockFsPath1, entries);
    viewFs.setAcl(viewFsPath2, entries);
    verify(mockFs2).setAcl(mockFsPath2, entries);

    viewFs.getAclStatus(viewFsPath1);
    verify(mockFs1).getAclStatus(mockFsPath1);
    viewFs.getAclStatus(viewFsPath2);
    verify(mockFs2).getAclStatus(mockFsPath2);
  }

  void checkVerifyChecksum(boolean flag) {
    viewFs.setVerifyChecksum(flag);
    assertEquals(flag, fs1.getVerifyChecksum());
    assertEquals(flag, fs2.getVerifyChecksum());
  }

  static class FakeFileSystem extends LocalFileSystem {
    boolean verifyChecksum = true;
    URI uri;
    
    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
      super.initialize(uri, conf);
      this.uri = uri;
    }
    
    @Override
    public URI getUri() {
      return uri;
    }
    
    @Override
    public void setVerifyChecksum(boolean verifyChecksum) {
      this.verifyChecksum = verifyChecksum;
    }
    
    public boolean getVerifyChecksum(){
      return verifyChecksum;
    }
  }
}
