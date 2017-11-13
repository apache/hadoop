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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;
import org.slf4j.event.Level;

/**
 * This class tests client lease recovery.
 */
public class TestFileCreationClient {
  static final String DIR = "/" + TestFileCreationClient.class.getSimpleName() + "/";

  {
    GenericTestUtils.setLogLevel(DataNode.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(LeaseManager.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(FSNamesystem.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(InterDatanodeProtocol.LOG, Level.TRACE);
  }

  /** Test lease recovery Triggered by DFSClient. */
  @Test
  public void testClientTriggeredLeaseRecovery() throws Exception {
    final int REPLICATION = 3;
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION).build();

    try {
      final FileSystem fs = cluster.getFileSystem();
      final Path dir = new Path("/wrwelkj");
      
      SlowWriter[] slowwriters = new SlowWriter[10];
      for(int i = 0; i < slowwriters.length; i++) {
        slowwriters[i] = new SlowWriter(fs, new Path(dir, "file" + i));
      }

      try {
        for(int i = 0; i < slowwriters.length; i++) {
          slowwriters[i].start();
        }

        Thread.sleep(1000);                       // let writers get started

        //stop a datanode, it should have least recover.
        cluster.stopDataNode(AppendTestUtil.nextInt(REPLICATION));
        
        //let the slow writer writes a few more seconds
        System.out.println("Wait a few seconds");
        Thread.sleep(5000);
      }
      finally {
        for(int i = 0; i < slowwriters.length; i++) {
          if (slowwriters[i] != null) {
            slowwriters[i].running = false;
            slowwriters[i].interrupt();
          }
        }
        for(int i = 0; i < slowwriters.length; i++) {
          if (slowwriters[i] != null) {
            slowwriters[i].join();
          }
        }
      }

      //Verify the file
      System.out.println("Verify the file");
      for(int i = 0; i < slowwriters.length; i++) {
        System.out.println(slowwriters[i].filepath + ": length="
            + fs.getFileStatus(slowwriters[i].filepath).getLen());
        FSDataInputStream in = null;
        try {
          in = fs.open(slowwriters[i].filepath);
          for(int j = 0, x; (x = in.read()) != -1; j++) {
            assertEquals(j, x);
          }
        }
        finally {
          IOUtils.closeStream(in);
        }
      }
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  static class SlowWriter extends Thread {
    final FileSystem fs;
    final Path filepath;
    boolean running = true;
    
    SlowWriter(FileSystem fs, Path filepath) {
      super(SlowWriter.class.getSimpleName() + ":" + filepath);
      this.fs = fs;
      this.filepath = filepath;
    }

    @Override
    public void run() {
      FSDataOutputStream out = null;
      int i = 0;
      try {
        out = fs.create(filepath);
        for(; running; i++) {
          System.out.println(getName() + " writes " + i);
          out.write(i);
          out.hflush();
          sleep(100);
        }
      }
      catch(Exception e) {
        System.out.println(getName() + " dies: e=" + e);
      }
      finally {
        System.out.println(getName() + ": i=" + i);
        IOUtils.closeStream(out);
      }
    }        
  }
}
