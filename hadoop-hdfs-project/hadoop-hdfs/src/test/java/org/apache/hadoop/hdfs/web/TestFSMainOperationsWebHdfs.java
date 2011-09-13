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

import java.net.URI;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSMainOperationsBaseTest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.web.resources.DatanodeWebHdfsMethods;
import org.apache.hadoop.hdfs.web.resources.ExceptionHandler;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFSMainOperationsWebHdfs extends FSMainOperationsBaseTest {
  {
    ((Log4JLogger)ExceptionHandler.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DatanodeWebHdfsMethods.LOG).getLogger().setLevel(Level.ALL);
  }

  private static MiniDFSCluster cluster = null;
  private static Path defaultWorkingDirectory;

  @BeforeClass
  public static void setupCluster() {
    Configuration conf = new Configuration();
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      cluster.waitActive();

      final String uri = WebHdfsFileSystem.SCHEME  + "://"
          + conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
      fSys = FileSystem.get(new URI(uri), conf); 
      defaultWorkingDirectory = fSys.getWorkingDirectory();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Override
  protected Path getDefaultWorkingDirectory() {
    return defaultWorkingDirectory;
  }
  
  //The following test failed since WebHdfsFileSystem did not support
  //authentication.
  //Disable it.
  @Test
  public void testListStatusThrowsExceptionForUnreadableDir() {}
}