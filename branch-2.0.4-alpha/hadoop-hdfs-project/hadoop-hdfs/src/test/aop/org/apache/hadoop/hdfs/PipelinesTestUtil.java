/*
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
import java.util.LinkedList;

import org.apache.hadoop.fi.DataTransferTestUtil;
import org.apache.hadoop.fi.FiTestUtil;
import org.apache.hadoop.fi.PipelineTest;
import org.apache.hadoop.fi.FiTestUtil.ActionContainer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;

public class PipelinesTestUtil extends DataTransferTestUtil {
  /**
   * {@inheritDoc}
   */
  public static PipelineTest initTest() {
    return thepipelinetest = new PipelinesTest();
  }

  /**
   * Storing acknowleged bytes num. action for fault injection tests
   */
  public static class ReceivedCheckAction implements FiTestUtil.Action<NodeBytes, IOException> {
    String name;
    LinkedList<NodeBytes> rcv = ((PipelinesTest) getPipelineTest()).received;
    LinkedList<NodeBytes> ack = ((PipelinesTest) getPipelineTest()).acked;

    /**
     * @param name of the test
     */
   public ReceivedCheckAction(String name) {
     this.name = name;
   }

    @Override
    public void run(NodeBytes nb) throws IOException {
      synchronized (rcv) {
        rcv.add(nb);
        for (NodeBytes n : rcv) {
          long counterPartsBytes = -1;
          NodeBytes counterPart = null;
          if (ack.size() > rcv.indexOf(n)) {
            counterPart = ack.get(rcv.indexOf(n));
            counterPartsBytes = counterPart.bytes;
          }
          assertTrue("FI: Wrong receiving length",
              counterPartsBytes <= n.bytes);
          if(FiTestUtil.LOG.isDebugEnabled()) {
            FiTestUtil.LOG.debug("FI: before compare of Recv bytes. Expected "
                + n.bytes + ", got " + counterPartsBytes);
          }
        }
      }
    }
  }

  /**
   * Storing acknowleged bytes num. action for fault injection tests
   */
  public static class AckedCheckAction implements FiTestUtil.Action<NodeBytes, IOException> {
    String name;
    LinkedList<NodeBytes> rcv = ((PipelinesTest) getPipelineTest()).received;
    LinkedList<NodeBytes> ack = ((PipelinesTest) getPipelineTest()).acked;

    /**
     * @param name of the test
     */
    public AckedCheckAction(String name) {
      this.name = name;
    }

    /**
     * {@inheritDoc}
     */
    public void run(NodeBytes nb) throws IOException {
      synchronized (ack) {
        ack.add(nb);
        for (NodeBytes n : ack) {
          NodeBytes counterPart = null;
          long counterPartsBytes = -1;
          if (rcv.size() > ack.indexOf(n)) { 
            counterPart = rcv.get(ack.indexOf(n));
            counterPartsBytes = counterPart.bytes;
          }
          assertTrue("FI: Wrong acknowledged length",
              counterPartsBytes == n.bytes);
          if(FiTestUtil.LOG.isDebugEnabled()) {
            FiTestUtil.LOG.debug(
                "FI: before compare of Acked bytes. Expected " +
                n.bytes + ", got " + counterPartsBytes);
          }
        }
      }
    }
  }

  /**
   * Class adds new types of action
   */
  public static class PipelinesTest extends DataTransferTest {
    LinkedList<NodeBytes> received = new LinkedList<NodeBytes>();
    LinkedList<NodeBytes> acked = new LinkedList<NodeBytes>();

    public final ActionContainer<NodeBytes, IOException> fiCallSetNumBytes =
      new ActionContainer<NodeBytes, IOException>();
    public final ActionContainer<NodeBytes, IOException> fiCallSetBytesAcked =
      new ActionContainer<NodeBytes, IOException>();
    
    private static boolean suspend = false;
    private static long lastQueuedPacket = -1;
    
    public void setSuspend(boolean flag) {
      suspend = flag;
    }
    public boolean getSuspend () {
      return suspend;
    }
    public void setVerified(long packetNum) {
      PipelinesTest.lastQueuedPacket = packetNum;
    }
    public long getLastQueued() {
      return lastQueuedPacket;
    }
  }

  public static class NodeBytes {
    DatanodeID id;
    long bytes;
    public NodeBytes(DatanodeID id, long bytes) {
      this.id = id;
      this.bytes = bytes;
    }
  }
}
