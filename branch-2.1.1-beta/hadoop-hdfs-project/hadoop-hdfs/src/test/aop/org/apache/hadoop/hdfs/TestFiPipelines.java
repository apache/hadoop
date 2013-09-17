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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fi.FiTestUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.BlockReceiverAspects;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFiPipelines {
  public static final Log LOG = LogFactory.getLog(TestFiPipelines.class);

  private static short REPL_FACTOR = 3;
  private static final int RAND_LIMIT = 2000;

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private static Configuration conf;
  Random rand = new Random(RAND_LIMIT);

  static {
    initLoggers();
    setConfiguration();
  }

  @Before
  public void startUpCluster() throws IOException {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPL_FACTOR).build();
    fs = (DistributedFileSystem) cluster.getFileSystem();
  }

  @After
  synchronized public void shutDownCluster() throws IOException {
    if (cluster != null) cluster.shutdown();
  }

  /**
   * Test initiates and sets actions created by injection framework. The actions
   * work with both aspects of sending acknologment packets in a pipeline.
   * Creates and closes a file of certain length < packet size.
   * Injected actions will check if number of visible bytes at datanodes equals
   * to number of acknoleged bytes
   *
   * @throws IOException in case of an error
   */
  @Test
  public void pipeline_04() throws IOException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    if(LOG.isDebugEnabled()) {
      LOG.debug("Running " + METHOD_NAME);
    }

    final PipelinesTestUtil.PipelinesTest pipst =
      (PipelinesTestUtil.PipelinesTest) PipelinesTestUtil.initTest();

    pipst.fiCallSetNumBytes.set(new PipelinesTestUtil.ReceivedCheckAction(METHOD_NAME));
    pipst.fiCallSetBytesAcked.set(new PipelinesTestUtil.AckedCheckAction(METHOD_NAME));

    Path filePath = new Path("/" + METHOD_NAME + ".dat");
    FSDataOutputStream fsOut = fs.create(filePath);
    TestPipelines.writeData(fsOut, 2);
    fs.close();
  }

  /**
   * Similar to pipeline_04 but sends many packets into a pipeline 
   * @throws IOException in case of an error
   */
  @Test
  public void pipeline_05() throws IOException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    if(LOG.isDebugEnabled()) {
      LOG.debug("Running " + METHOD_NAME);
    }

    final PipelinesTestUtil.PipelinesTest pipst =
      (PipelinesTestUtil.PipelinesTest) PipelinesTestUtil.initTest();

    pipst.fiCallSetNumBytes.set(new PipelinesTestUtil.ReceivedCheckAction(METHOD_NAME));
    pipst.fiCallSetBytesAcked.set(new PipelinesTestUtil.AckedCheckAction(METHOD_NAME));

    Path filePath = new Path("/" + METHOD_NAME + ".dat");
    FSDataOutputStream fsOut = fs.create(filePath);
    for (int i = 0; i < 17; i++) {
      TestPipelines.writeData(fsOut, 23);
    }
    fs.close();
  } 

  /**
   * This quite tricky test prevents acknowledgement packets from a datanode
   * This should block any write attempts after ackQueue is full.
   * Test is blocking, so the MiniDFSCluster has to be killed harshly.
   * @throws IOException in case of an error
   */
  @Test
  public void pipeline_06() throws IOException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final int MAX_PACKETS = 80;
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("Running " + METHOD_NAME);
    }

    final PipelinesTestUtil.PipelinesTest pipst =
      (PipelinesTestUtil.PipelinesTest) PipelinesTestUtil.initTest();

    pipst.setSuspend(true); // This is ack. suspend test
    Path filePath = new Path("/" + METHOD_NAME + ".dat");
    FSDataOutputStream fsOut = fs.create(filePath);

    int cnt = 0;
    try {
      // At this point let's start an external checker thread, which will
      // verify the test's results and shutdown the MiniDFSCluster for us,
      // because what it's gonna do has BLOCKING effect on datanodes 
      QueueChecker cq = new QueueChecker(pipst, MAX_PACKETS);
      cq.start();
      // The following value is explained by the fact that size of a packet isn't
      // necessary equals to the value of
      // DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY
      // The actual logic is expressed in DFSClient#computePacketChunkSize
      int bytesToSend = 700;
      while (cnt < 100 && pipst.getSuspend()) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("_06(): " + cnt++ + " sending another " +
              bytesToSend + " bytes");
        }
        TestPipelines.writeData(fsOut, bytesToSend);
      }
    } catch (Exception e) {
      LOG.warn("Getting unexpected exception: ", e);
    }
    if(LOG.isDebugEnabled()) {
      LOG.debug("Last queued packet number " + pipst.getLastQueued());
    }
    assertTrue("Shouldn't be able to send more than 81 packet", pipst.getLastQueued() <= 81);
  }

  private class QueueChecker extends Thread {
    PipelinesTestUtil.PipelinesTest test;
    final int MAX;
    boolean done = false;
    
    public QueueChecker(PipelinesTestUtil.PipelinesTest handle, int maxPackets) {
      test = handle;
      MAX = maxPackets;
    }

    @Override
    public void run() {
      while (!done) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("_06: checking for the limit " + test.getLastQueued() + 
              " and " + MAX);
        }
        if (test.getLastQueued() >= MAX) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("FI: Resume packets acking");
          }
          test.setSuspend(false); //Do not suspend ack sending any more
          done = true;
        }
        if (!done)
          try {
            if(LOG.isDebugEnabled()) {
              LOG.debug("_06: MAX isn't reached yet. Current=" +
                  test.getLastQueued());
            }
            sleep(100);
          } catch (InterruptedException e) { }
      }

      assertTrue("Shouldn't be able to send more than 81 packet", test.getLastQueued() <= 81);
      try {
        if(LOG.isDebugEnabled()) {
          LOG.debug("_06: shutting down the cluster");
        }
        // It has to be done like that, because local version of shutDownCluster()
        // won't work, because it tries to close an instance of FileSystem too.
        // Which is where the waiting is happening.
        if (cluster !=null )
          shutDownCluster();
      } catch (Exception e) {
        e.printStackTrace();
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("End QueueChecker thread");
      }
    }
  }
  
  private static void setConfiguration() {
    conf = new Configuration();
    int customPerChecksumSize = 700;
    int customBlockSize = customPerChecksumSize * 3;
    conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, 100);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, customPerChecksumSize);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, customBlockSize);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, customBlockSize / 2);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 0);
  }

  private static void initLoggers() {
    ((Log4JLogger) NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) LogFactory.getLog(FSNamesystem.class)).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) TestFiPipelines.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) DFSClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) FiTestUtil.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) BlockReceiverAspects.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) DFSClientAspects.LOG).getLogger().setLevel(Level.ALL);
  }

}
