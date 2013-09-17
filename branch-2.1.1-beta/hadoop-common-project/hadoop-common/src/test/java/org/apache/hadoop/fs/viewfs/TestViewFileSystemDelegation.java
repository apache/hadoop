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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.*;
import static org.junit.Assert.*;

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
    fs1 = setupFileSystem(new URI("fs1:/"), FakeFileSystem.class);
    fs2 = setupFileSystem(new URI("fs2:/"), FakeFileSystem.class);
    viewFs = FileSystem.get(FsConstants.VIEWFS_URI, conf);
  }

  static FakeFileSystem setupFileSystem(URI uri, Class clazz)
      throws Exception {
    String scheme = uri.getScheme();
    conf.set("fs."+scheme+".impl", clazz.getName());
    FakeFileSystem fs = (FakeFileSystem)FileSystem.get(uri, conf);
    assertEquals(uri, fs.getUri());
    Path targetPath = new FileSystemTestHelper().getAbsoluteTestRootPath(fs);
    ConfigUtil.addLink(conf, "/mounts/"+scheme, targetPath.toUri());
    return fs;
  }

  @Test
  public void testSanity() {
    assertEquals("fs1:/", fs1.getUri().toString());
    assertEquals("fs2:/", fs2.getUri().toString());
  }
  
  @Test
  public void testVerifyChecksum() throws Exception {
    checkVerifyChecksum(false);
    checkVerifyChecksum(true);
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
