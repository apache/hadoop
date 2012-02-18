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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;


public class TestTransferFsImage {

  private static final File TEST_DIR = new File(
      System.getProperty("test.build.data","build/test/data"));

  /**
   * Regression test for HDFS-1997. Test that, if an exception
   * occurs on the client side, it is properly reported as such,
   * and reported to the associated NNStorage object.
   */
  @Test
  public void testClientSideException() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(0).build();
    NNStorage mockStorage = Mockito.mock(NNStorage.class);
    List<File> localPath = Collections.<File>singletonList(
        new File("/xxxxx-does-not-exist/blah"));
       
    try {
      String fsName = NameNode.getHostPortString(
          cluster.getNameNode().getHttpAddress());
      String id = "getimage=1&txid=0";

      TransferFsImage.getFileClient(fsName, id, localPath, mockStorage, false);      
      fail("Didn't get an exception!");
    } catch (IOException ioe) {
      Mockito.verify(mockStorage).reportErrorOnFile(localPath.get(0));
      assertTrue(
          "Unexpected exception: " + StringUtils.stringifyException(ioe),
          ioe.getMessage().contains("Unable to download to any storage"));
    } finally {
      cluster.shutdown();      
    }
  }
  
  /**
   * Similar to the above test, except that there are multiple local files
   * and one of them can be saved.
   */
  @Test
  public void testClientSideExceptionOnJustOneDir() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(0).build();
    NNStorage mockStorage = Mockito.mock(NNStorage.class);
    List<File> localPaths = ImmutableList.of(
        new File("/xxxxx-does-not-exist/blah"),
        new File(TEST_DIR, "testfile")    
        );
       
    try {
      String fsName = NameNode.getHostPortString(
          cluster.getNameNode().getHttpAddress());
      String id = "getimage=1&txid=0";

      TransferFsImage.getFileClient(fsName, id, localPaths, mockStorage, false);      
      Mockito.verify(mockStorage).reportErrorOnFile(localPaths.get(0));
      assertTrue("The valid local file should get saved properly",
          localPaths.get(1).length() > 0);
    } finally {
      cluster.shutdown();      
    }
  }
}
