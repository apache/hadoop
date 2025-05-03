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

import java.io.IOException;
import java.net.URISyntaxException;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;

public class TestFcHdfsPermission extends FileContextPermissionBase {
  
  private static final FileContextTestHelper fileContextTestHelper =
      new FileContextTestHelper("/tmp/TestFcHdfsPermission");
  private static FileContext fc;

  private static MiniDFSCluster cluster;
  private static Path defaultWorkingDirectory;
  
  @Override
  protected FileContextTestHelper getFileContextHelper() {
    return fileContextTestHelper;
  }
  
  @Override
  protected FileContext getFileContext() {
    return fc;
  }
  
  @BeforeAll
  public static void clusterSetupAtBegining()
                                    throws IOException, LoginException, URISyntaxException  {
    Configuration conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    fc = FileContext.getFileContext(cluster.getURI(0), conf);
    defaultWorkingDirectory = fc.makeQualified( new Path("/user/" + 
        UserGroupInformation.getCurrentUser().getShortUserName()));
    fc.mkdir(defaultWorkingDirectory, FileContext.DEFAULT_PERM, true);
  }

      
  @AfterAll
  public static void ClusterShutdownAtEnd() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  @Override
  @BeforeEach
  public void setUp() throws Exception {
    super.setUp();
  }
  
  @Override
  @AfterEach
  public void tearDown() throws Exception {
    super.tearDown();
  }
}
