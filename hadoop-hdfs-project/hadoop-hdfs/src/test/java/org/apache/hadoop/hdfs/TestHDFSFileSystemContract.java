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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.LeaseRecoverable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.SafeMode;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.fs.CommonPathCapabilities.LEASE_RECOVERABLE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHDFSFileSystemContract extends FileSystemContractBaseTest {
  
  private MiniDFSCluster cluster;
  private String defaultWorkingDirectory;

  @Before
  public void setUp() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY,
        FileSystemContractBaseTest.TEST_UMASK);
    File basedir = GenericTestUtils.getRandomizedTestDir();
    cluster = new MiniDFSCluster.Builder(conf, basedir).numDataNodes(2)
        .build();
    fs = cluster.getFileSystem();
    defaultWorkingDirectory = "/user/" + 
           UserGroupInformation.getCurrentUser().getShortUserName();
  }
  
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Override
  protected String getDefaultWorkingDirectory() {
    return defaultWorkingDirectory;
  }

  @Override
  protected int getGlobalTimeout() {
    return 60 * 1000;
  }

  @Test
  public void testAppend() throws IOException {
    AppendTestUtil.testAppend(fs, new Path("/testAppend/f"));
  }

  @Test
  public void testFileSystemCapabilities() throws Exception {
    final Path p = new Path("testFileSystemCapabilities");
    // ViewFileSystem does not support LeaseRecoverable and SafeMode.
    if (fs instanceof DistributedFileSystem) {
      final boolean leaseRecovery = fs.hasPathCapability(p, LEASE_RECOVERABLE);
      assertThat(leaseRecovery).describedAs("path capabilities %s=%s in %s", LEASE_RECOVERABLE,
          leaseRecovery, fs).isTrue();
      assertThat(fs).describedAs("filesystem %s", fs).isInstanceOf(LeaseRecoverable.class);
      assertThat(fs).describedAs("filesystem %s", fs).isInstanceOf(SafeMode.class);
    }
  }
}
