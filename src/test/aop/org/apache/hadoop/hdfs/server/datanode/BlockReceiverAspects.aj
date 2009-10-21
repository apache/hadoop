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

import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fi.DataTransferTestUtil;
import org.apache.hadoop.fi.ProbabilityModel;
import org.apache.hadoop.fi.DataTransferTestUtil.DataTransferTest;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Status;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

/**
 * This aspect takes care about faults injected into datanode.BlockReceiver 
 * class 
 */
public privileged aspect BlockReceiverAspects {
  public static final Log LOG = LogFactory.getLog(BlockReceiverAspects.class);

  pointcut callReceivePacket(BlockReceiver blockreceiver) :
    call (* OutputStream.write(..))
      && withincode (* BlockReceiver.receivePacket(..))
// to further limit the application of this aspect a very narrow 'target' can be used as follows
//  && target(DataOutputStream)
      && !within(BlockReceiverAspects +)
      && this(blockreceiver);
	
  before(BlockReceiver blockreceiver
      ) throws IOException : callReceivePacket(blockreceiver) {
    LOG.info("FI: callReceivePacket");
    DataTransferTest dtTest = DataTransferTestUtil.getDataTransferTest();
    if (dtTest != null)
      dtTest.fiCallReceivePacket.run(
          blockreceiver.getDataNode().getDatanodeRegistration());

    if (ProbabilityModel.injectCriteria(BlockReceiver.class.getSimpleName())) {
      LOG.info("Before the injection point");
      Thread.dumpStack();
      throw new DiskOutOfSpaceException ("FI: injected fault point at " + 
        thisJoinPoint.getStaticPart( ).getSourceLocation());
    }
  }

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
      final DatanodeRegistration dr = blockreceiver.getDataNode().getDatanodeRegistration();
      LOG.info("FI: pipelineClose, datanode=" + dr
          + ", offsetInBlock=" + offsetInBlock
          + ", seqno=" + seqno
          + ", lastPacketInBlock=" + lastPacketInBlock
          + ", len=" + len
          + ", endOfHeader=" + endOfHeader);
  
      final DataTransferTest test = DataTransferTestUtil.getDataTransferTest();
      if (test != null) {
        test.fiPipelineClose.run(dr);
      }
    }
  }

  pointcut pipelineAck(BlockReceiver.PacketResponder packetresponder) :
    call (Status Status.read(DataInput))
      && this(packetresponder);

  after(BlockReceiver.PacketResponder packetresponder) throws IOException
      : pipelineAck(packetresponder) {
    final DatanodeRegistration dr = packetresponder.receiver.getDataNode().getDatanodeRegistration();
    LOG.info("FI: fiPipelineAck, datanode=" + dr);

    final DataTransferTest test = DataTransferTestUtil.getDataTransferTest();
    if (test != null) {
      test.fiPipelineAck.run(dr);
    }
  }

  pointcut blockFileClose(BlockReceiver blockreceiver) :
    call(void close())
      && withincode(void BlockReceiver.close())
      && this(blockreceiver);

  after(BlockReceiver blockreceiver) throws IOException : blockFileClose(blockreceiver) {
    final DatanodeRegistration dr = blockreceiver.getDataNode().getDatanodeRegistration();
    LOG.info("FI: blockFileClose, datanode=" + dr);

    final DataTransferTest test = DataTransferTestUtil.getDataTransferTest();
    if (test != null) {
      test.fiBlockFileClose.run(dr);
    }
  }
}
