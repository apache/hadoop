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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fi.DataTransferTestUtil;
import org.apache.hadoop.fi.Pipeline;
import org.apache.hadoop.fi.PipelineTest;
import org.apache.hadoop.fi.ProbabilityModel;
import org.apache.hadoop.fi.DataTransferTestUtil.DataTransferTest;
import org.apache.hadoop.hdfs.server.datanode.BlockReceiver.PacketResponder;
import org.apache.hadoop.hdfs.PipelinesTestUtil.PipelinesTest;
import org.apache.hadoop.hdfs.PipelinesTestUtil.NodeBytes;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

/**
 * This aspect takes care about faults injected into datanode.BlockReceiver 
 * class 
 */
privileged public aspect BlockReceiverAspects {
  public static final Log LOG = LogFactory.getLog(BlockReceiverAspects.class);

  BlockReceiver BlockReceiver.PacketResponder.getReceiver(){
    LOG.info("FI: getReceiver() " + getClass().getName());
    return BlockReceiver.this;
  }

  pointcut callReceivePacket(BlockReceiver blockreceiver) :
    call(* receivePacket(..)) && target(blockreceiver);
	
  before(BlockReceiver blockreceiver
      ) throws IOException : callReceivePacket(blockreceiver) {
    final String dnName = blockreceiver.getDataNode().getMachineName();
    final DatanodeID dnId = blockreceiver.getDataNode().getDatanodeId();
    LOG.info("FI: callReceivePacket, datanode=" + dnName);
    DataTransferTest dtTest = DataTransferTestUtil.getDataTransferTest();
    if (dtTest != null)
      dtTest.fiCallReceivePacket.run(dnId);

    if (ProbabilityModel.injectCriteria(BlockReceiver.class.getSimpleName())) {
      LOG.info("Before the injection point");
      Thread.dumpStack();
      throw new DiskOutOfSpaceException ("FI: injected fault point at " + 
        thisJoinPoint.getStaticPart( ).getSourceLocation());
    }
  }
  
  pointcut callWritePacketToDisk(BlockReceiver blockreceiver) :
    call(* writePacketToDisk(..)) && target(blockreceiver);

  before(BlockReceiver blockreceiver
      ) throws IOException : callWritePacketToDisk(blockreceiver) {
    LOG.info("FI: callWritePacketToDisk");
    DataTransferTest dtTest = DataTransferTestUtil.getDataTransferTest();
    if (dtTest != null)
      dtTest.fiCallWritePacketToDisk.run(
          blockreceiver.getDataNode().getDatanodeId());
  }

  pointcut afterDownstreamStatusRead(BlockReceiver.PacketResponder responder):
    call(void PipelineAck.readFields(InputStream)) && this(responder);

  after(BlockReceiver.PacketResponder responder)
      throws IOException: afterDownstreamStatusRead(responder) {
    final DataNode d = responder.getReceiver().getDataNode();
    DataTransferTest dtTest = DataTransferTestUtil.getDataTransferTest();
    if (dtTest != null)
      dtTest.fiAfterDownstreamStatusRead.run(d.getDatanodeId());
  }

    // Pointcuts and advises for TestFiPipelines  
  pointcut callSetNumBytes(BlockReceiver br, long offset) : 
    call (void ReplicaInPipelineInterface.setNumBytes(long)) 
    && withincode (int BlockReceiver.receivePacket(long, long, boolean, int, int))
    && args(offset) 
    && this(br);
  
  after(BlockReceiver br, long offset) : callSetNumBytes(br, offset) {
    LOG.debug("FI: Received bytes To: " + br.datanode.getStorageId() + ": " + offset);
    PipelineTest pTest = DataTransferTestUtil.getDataTransferTest();
    if (pTest == null) {
      LOG.debug("FI: no pipeline has been found in receiving");
      return;
    }
    if (!(pTest instanceof PipelinesTest)) {
      return;
    }
    NodeBytes nb = new NodeBytes(br.datanode.getDatanodeId(), offset);
    try {
      ((PipelinesTest)pTest).fiCallSetNumBytes.run(nb);
    } catch (IOException e) {
      LOG.fatal("FI: no exception is expected here!");
    }
  }
  
  // Pointcuts and advises for TestFiPipelines  
  pointcut callSetBytesAcked(PacketResponder pr, long acked) : 
    call (void ReplicaInPipelineInterface.setBytesAcked(long)) 
    && withincode (void PacketResponder.run())
    && args(acked) 
    && this(pr);

  after (PacketResponder pr, long acked) : callSetBytesAcked (pr, acked) {
    PipelineTest pTest = DataTransferTestUtil.getDataTransferTest();
    if (pTest == null) {
      LOG.debug("FI: no pipeline has been found in acking");
      return;
    }
    LOG.debug("FI: Acked total bytes from: " + 
        pr.getReceiver().datanode.getStorageId() + ": " + acked);
    if (pTest instanceof PipelinesTest) {
      bytesAckedService((PipelinesTest)pTest, pr, acked);
    }
  }

  private void bytesAckedService 
      (final PipelinesTest pTest, final PacketResponder pr, final long acked) {
    NodeBytes nb = new NodeBytes(pr.getReceiver().datanode.getDatanodeId(), acked);
    try {
      pTest.fiCallSetBytesAcked.run(nb);
    } catch (IOException e) {
      LOG.fatal("No exception should be happening at this point");
      assert false;
    }
  }
  
  pointcut preventAckSending () :
    call (void PipelineAck.write(OutputStream)) 
    && within (PacketResponder);

  static int ackCounter = 0;
  void around () : preventAckSending () {
    PipelineTest pTest = DataTransferTestUtil.getDataTransferTest();

    if (pTest == null) { 
      LOG.debug("FI: remove first ack as expected");
      proceed();
      return;
    }
    if (!(pTest instanceof PipelinesTest)) {
      LOG.debug("FI: remove first ack as expected");
      proceed();
      return;
    }
    if (((PipelinesTest)pTest).getSuspend()) {
        LOG.debug("FI: suspend the ack");
        return;
    }
    LOG.debug("FI: remove first ack as expected");
    proceed();
  }
  // End of pointcuts and advises for TestFiPipelines  

  pointcut pipelineClose(BlockReceiver blockreceiver, long offsetInBlock, long seqno,
      boolean lastPacketInBlock, int len, int endOfHeader) :
    call (* BlockReceiver.receivePacket(long, long, boolean, int, int))
      && this(blockreceiver)
      && args(offsetInBlock, seqno, lastPacketInBlock, len, endOfHeader);

  before(BlockReceiver blockreceiver, long offsetInBlock, long seqno,
      boolean lastPacketInBlock, int len, int endOfHeader
      ) throws IOException : pipelineClose(blockreceiver, offsetInBlock, seqno,
          lastPacketInBlock, len, endOfHeader) {
    if (len == 0) {
      final DatanodeID dnId = blockreceiver.getDataNode().getDatanodeId();
      LOG.info("FI: pipelineClose, datanode=" + dnId.getName()
          + ", offsetInBlock=" + offsetInBlock
          + ", seqno=" + seqno
          + ", lastPacketInBlock=" + lastPacketInBlock
          + ", len=" + len
          + ", endOfHeader=" + endOfHeader);
  
      final DataTransferTest test = DataTransferTestUtil.getDataTransferTest();
      if (test != null) {
        test.fiPipelineClose.run(dnId);
      }
    }
  }

  pointcut pipelineAck(BlockReceiver.PacketResponder packetresponder) :
    call (void PipelineAck.readFields(InputStream))
      && this(packetresponder);

  after(BlockReceiver.PacketResponder packetresponder) throws IOException
      : pipelineAck(packetresponder) {
    final DatanodeID dnId = packetresponder.getReceiver().getDataNode().getDatanodeId();
    LOG.info("FI: fiPipelineAck, datanode=" + dnId);

    final DataTransferTest test = DataTransferTestUtil.getDataTransferTest();
    if (test != null) {
      test.fiPipelineAck.run(dnId);
    }
  }

  pointcut blockFileClose(BlockReceiver blockreceiver) :
    call(void close())
      && withincode(void BlockReceiver.close())
      && this(blockreceiver);

  after(BlockReceiver blockreceiver) throws IOException : blockFileClose(blockreceiver) {
    final DatanodeID dnId = blockreceiver.getDataNode().getDatanodeId();
    LOG.info("FI: blockFileClose, datanode=" + dnId);

    final DataTransferTest test = DataTransferTestUtil.getDataTransferTest();
    if (test != null) {
      test.fiBlockFileClose.run(dnId);
    }
  }
}
