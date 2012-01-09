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
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil.CouldNotCatchUpException;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestFailureToReadEdits {
  private static final String TEST_DIR1 = "/test1";
  private static final String TEST_DIR2 = "/test2";
  private static final String TEST_DIR3 = "/test3";

  /**
   * Test that the standby NN won't double-replay earlier edits if it encounters
   * a failure to read a later edit.
   */
  @Test
  public void testFailuretoReadEdits() throws IOException,
      ServiceFailedException, URISyntaxException, InterruptedException {
    Configuration conf = new Configuration();
    HAUtil.setAllowStandbyReads(conf, true);
    
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(0)
      .build();
    
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);
      
      Runtime mockRuntime = mock(Runtime.class);
      
      NameNode nn1 = cluster.getNameNode(0);
      NameNode nn2 = cluster.getNameNode(1);
      nn2.getNamesystem().getEditLogTailer().setSleepTime(250);
      nn2.getNamesystem().getEditLogTailer().interrupt();
      nn2.getNamesystem().getEditLogTailer().setRuntime(mockRuntime);
      
      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
      fs.mkdirs(new Path(TEST_DIR1));
      HATestUtil.waitForStandbyToCatchUp(nn1, nn2);
      
      // If these two ops are applied twice, the first op will throw an
      // exception the second time its replayed.
      fs.setOwner(new Path(TEST_DIR1), "foo", "bar");
      fs.delete(new Path(TEST_DIR1), true);
      
      // This op should get applied just fine.
      fs.mkdirs(new Path(TEST_DIR2));
      
      // This is the op the mocking will cause to fail to be read.
      fs.mkdirs(new Path(TEST_DIR3));
      
      FSEditLog spyEditLog = spy(nn2.getNamesystem().getEditLogTailer()
          .getEditLog());
      LimitedEditLogAnswer answer = new LimitedEditLogAnswer(); 
      doAnswer(answer).when(spyEditLog).selectInputStreams(
          anyLong(), anyLong(), anyBoolean());
      nn2.getNamesystem().getEditLogTailer().setEditLog(spyEditLog);
      
      try {
        HATestUtil.waitForStandbyToCatchUp(nn1, nn2);
        fail("Standby fully caught up, but should not have been able to");
      } catch (HATestUtil.CouldNotCatchUpException e) {
        verify(mockRuntime, times(0)).exit(anyInt());
      }
      
      // Null because it was deleted.
      assertNull(NameNodeAdapter.getFileInfo(nn2,
          TEST_DIR1, false));
      // Should have been successfully created.
      assertTrue(NameNodeAdapter.getFileInfo(nn2,
          TEST_DIR2, false).isDir());
      // Null because it hasn't been created yet.
      assertNull(NameNodeAdapter.getFileInfo(nn2,
          TEST_DIR3, false));
      
      // Now let the standby read ALL the edits.
      answer.setThrowExceptionOnRead(false);
      HATestUtil.waitForStandbyToCatchUp(nn1, nn2);
      
      // Null because it was deleted.
      assertNull(NameNodeAdapter.getFileInfo(nn2,
          TEST_DIR1, false));
      // Should have been successfully created.
      assertTrue(NameNodeAdapter.getFileInfo(nn2,
          TEST_DIR2, false).isDir());
      // Should now have been successfully created.
      assertTrue(NameNodeAdapter.getFileInfo(nn2,
          TEST_DIR3, false).isDir());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  private static class LimitedEditLogAnswer
      implements Answer<Collection<EditLogInputStream>> {
    
    private boolean throwExceptionOnRead = true;

    @SuppressWarnings("unchecked")
    @Override
    public Collection<EditLogInputStream> answer(InvocationOnMock invocation)
        throws Throwable {
      Collection<EditLogInputStream> streams = (Collection<EditLogInputStream>)
          invocation.callRealMethod();
  
      if (!throwExceptionOnRead) {
        return streams;
      } else {
        Collection<EditLogInputStream> ret = new LinkedList<EditLogInputStream>();
        for (EditLogInputStream stream : streams) {
          EditLogInputStream spyStream = spy(stream);
          doAnswer(new Answer<FSEditLogOp>() {

            @Override
            public FSEditLogOp answer(InvocationOnMock invocation)
                throws Throwable {
              FSEditLogOp op = (FSEditLogOp) invocation.callRealMethod();
              if (throwExceptionOnRead &&
                  TEST_DIR3.equals(NameNodeAdapter.getMkdirOpPath(op))) {
                throw new IOException("failed to read op creating " + TEST_DIR3);
              } else {
                return op;
              }
            }
            
          }).when(spyStream).readOp();
          ret.add(spyStream);
        }
        return ret;
      }
    }
    
    public void setThrowExceptionOnRead(boolean throwExceptionOnRead) {
      this.throwExceptionOnRead = throwExceptionOnRead;
    }
  }
  
}
