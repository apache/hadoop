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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.BeforeClass;

/**
 * Tests for ACL operation through FileContext APIs
 */
public class TestFileContextAcl extends FSAclBaseTest {

  @BeforeClass
  public static void init() throws Exception {
    conf = new Configuration();
    startCluster();
  }

  @Override
  protected FileSystem createFileSystem() throws Exception {
    FileContextFS fcFs = new FileContextFS();
    fcFs.initialize(FileSystem.getDefaultUri(conf), conf);
    return fcFs;
  }

  /*
   * To Re-use the FSAclBaseTest's testcases, creating a filesystem
   * implementation which works based on fileContext. In this only overriding
   * acl related methods, other operations will happen using normal filesystem
   * itself which is out of scope for this test
   */
  public static class FileContextFS extends DistributedFileSystem {

    private FileContext fc;

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
      super.initialize(uri, conf);
      fc = FileContext.getFileContext(conf);
    }

    @Override
    public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
        throws IOException {
      fc.modifyAclEntries(path, aclSpec);
    }

    @Override
    public void removeAclEntries(Path path, List<AclEntry> aclSpec)
        throws IOException {
      fc.removeAclEntries(path, aclSpec);
    }

    @Override
    public void removeDefaultAcl(Path path) throws IOException {
      fc.removeDefaultAcl(path);
    }

    @Override
    public void removeAcl(Path path) throws IOException {
      fc.removeAcl(path);
    }

    @Override
    public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
      fc.setAcl(path, aclSpec);
    }

    @Override
    public AclStatus getAclStatus(Path path) throws IOException {
      return fc.getAclStatus(path);
    }
  }
}
