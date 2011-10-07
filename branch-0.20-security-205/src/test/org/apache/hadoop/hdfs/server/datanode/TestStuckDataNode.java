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
package org.apache.hadoop.hdfs.server.datanode;

import junit.framework.TestCase;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestFileAppend4.DelayAnswer;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeInstrumentation;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * This class tests the building blocks that are needed to
 * support HDFS appends.
 */
public class TestStuckDataNode extends TestCase {
  {
    ((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }

  /** This creates a slow writer and check to see
   * if pipeline heartbeats work fine
   */
  public void testStuckDataNode() throws Exception {
    final int DATANODE_NUM = 3;
    Configuration conf = new Configuration();
    final int timeout = 8000;
    conf.setInt("dfs.socket.timeout",timeout);

    final Path p = new Path("/pipelineHeartbeat/foo");
    System.out.println("p=" + p);

    MiniDFSCluster cluster = new MiniDFSCluster(conf, DATANODE_NUM, true, null);
    DistributedFileSystem fs = (DistributedFileSystem)cluster.getFileSystem();

    DataNodeInstrumentation metrics = spy(cluster.getDataNodes().get(0).myMetrics);    
    DelayAnswer delayAnswer = new DelayAnswer(); 
    doAnswer(delayAnswer).when(metrics).incrBytesWritten(anyInt());

    try {
    	// create a new file.
    	FSDataOutputStream stm = fs.create(p);
    	stm.write(1);
    	stm.sync();
    	stm.write(2);
    	stm.close();

    	// verify that entire file is good
    	FSDataInputStream in = fs.open(p);
    	assertEquals(1, in.read());
    	assertEquals(2, in.read());
    	in.close();
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

}
