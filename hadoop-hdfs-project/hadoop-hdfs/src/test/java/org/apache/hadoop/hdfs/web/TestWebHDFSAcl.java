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

import org.apache.hadoop.hdfs.server.namenode.FSAclBaseTest;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests ACL APIs via WebHDFS.
 */
public class TestWebHDFSAcl extends FSAclBaseTest {

  @BeforeClass
  public static void init() throws Exception {
    conf = WebHdfsTestUtil.createConf();
    startCluster();
  }

  /**
   * We need to skip this test on WebHDFS, because WebHDFS currently cannot
   * resolve symlinks.
   */
  @Override
  @Test
  @Ignore
  public void testDefaultAclNewSymlinkIntermediate() {
  }

  /**
   * Overridden to provide a WebHdfsFileSystem wrapper for the super-user.
   *
   * @return WebHdfsFileSystem for super-user
   * @throws Exception if creation fails
   */
  @Override
  protected WebHdfsFileSystem createFileSystem() throws Exception {
    return WebHdfsTestUtil.getWebHdfsFileSystem(conf, WebHdfsFileSystem.SCHEME);
  }

  /**
   * Overridden to provide a WebHdfsFileSystem wrapper for a specific user.
   *
   * @param user UserGroupInformation specific user
   * @return WebHdfsFileSystem for specific user
   * @throws Exception if creation fails
   */
  @Override
  protected WebHdfsFileSystem createFileSystem(UserGroupInformation user)
      throws Exception {
    return WebHdfsTestUtil.getWebHdfsFileSystemAs(user, conf,
      WebHdfsFileSystem.SCHEME);
  }
}
