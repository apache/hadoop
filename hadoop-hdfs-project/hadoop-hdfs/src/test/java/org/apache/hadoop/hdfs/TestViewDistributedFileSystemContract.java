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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class TestViewDistributedFileSystemContract
    extends TestHDFSFileSystemContract {
  private static MiniDFSCluster cluster;
  private static String defaultWorkingDirectory;
  private static Configuration conf = new HdfsConfiguration();

  @BeforeClass
  public static void init() throws IOException {
    final File basedir = GenericTestUtils.getRandomizedTestDir();
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY,
        FileSystemContractBaseTest.TEST_UMASK);
    cluster = new MiniDFSCluster.Builder(conf, basedir)
        .numDataNodes(2)
        .build();
    defaultWorkingDirectory =
        "/user/" + UserGroupInformation.getCurrentUser().getShortUserName();
  }

  @Before
  public void setUp() throws Exception {
    conf.set("fs.hdfs.impl", ViewDistributedFileSystem.class.getName());
    URI defaultFSURI =
        URI.create(conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
    ConfigUtil.addLink(conf, defaultFSURI.getHost(), "/user",
        defaultFSURI);
    ConfigUtil.addLinkFallback(conf, defaultFSURI.getHost(),
        defaultFSURI);
    fs = FileSystem.get(conf);
  }

  @AfterClass
  public static void tearDownAfter() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Override
  protected String getDefaultWorkingDirectory() {
    return defaultWorkingDirectory;
  }

  @Test
  public void testRenameRootDirForbidden() throws Exception {
    LambdaTestUtils.intercept(AccessControlException.class,
        "InternalDir of ViewFileSystem is readonly", () -> {
          super.testRenameRootDirForbidden();
        });
  }
}
