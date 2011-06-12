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
import java.net.URISyntaxException;
import java.util.List;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestViewFileSystemHdfs extends ViewFileSystemBaseTest {

  private static MiniDFSCluster cluster;
  private static Path defaultWorkingDirectory;
  private static Configuration CONF = new Configuration();
  private static FileSystem fHdfs;
  
  @BeforeClass
  public static void clusterSetupAtBegining() throws IOException,
      LoginException, URISyntaxException {
    SupportsBlocks = true;
    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(2).build();
    cluster.waitClusterUp();
    cluster.getNamesystem().getDelegationTokenSecretManager().startThreads();
    fHdfs = cluster.getFileSystem();
    defaultWorkingDirectory = fHdfs.makeQualified( new Path("/user/" + 
        UserGroupInformation.getCurrentUser().getShortUserName()));
    fHdfs.mkdirs(defaultWorkingDirectory);
  }

      
  @AfterClass
  public static void ClusterShutdownAtEnd() throws Exception {
    cluster.shutdown();   
  }

  @Before
  public void setUp() throws Exception {
    // create the test root on local_fs
    fsTarget = fHdfs;
    super.setUp();
    
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  /*
   * This overides the default implementation since hdfs does have delegation
   * tokens.
   */
  @Override
  @Test
  public void testGetDelegationTokens() throws IOException {
    List<Token<?>> delTokens = 
        fsView.getDelegationTokens("sanjay");
    Assert.assertEquals(7, delTokens.size()); 
  }

}
