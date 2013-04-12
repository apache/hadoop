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
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fi.DataTransferTestUtil;
import org.apache.hadoop.fi.DataTransferTestUtil.DataTransferTest;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.datatransfer.Receiver;

/** Aspect for DataTransferProtocol */
public aspect DataTransferProtocolAspects {
  public static final Log LOG = LogFactory.getLog(
      DataTransferProtocolAspects.class);
  /*
  {
    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)InterDatanodeProtocol.LOG).getLogger().setLevel(Level.ALL);
  }
  */

  pointcut receiverOp(DataXceiver dataxceiver):
    call(Op Receiver.readOp()) && target(dataxceiver);

  after(DataXceiver dataxceiver) returning(Op op): receiverOp(dataxceiver) {
    LOG.info("FI: receiverOp " + op + ", datanode="
        + dataxceiver.getDataNode().getDatanodeId().getName());    
  }

  pointcut statusRead(DataXceiver dataxceiver):
    call(BlockOpResponseProto BlockOpResponseProto.parseFrom(InputStream)) && this(dataxceiver);

  after(DataXceiver dataxceiver) returning(BlockOpResponseProto status
      ) throws IOException: statusRead(dataxceiver) {
    final DataNode d = dataxceiver.getDataNode();
    LOG.info("FI: statusRead " + status + ", datanode="
        + d.getDatanodeId().getName());    
    DataTransferTest dtTest = DataTransferTestUtil.getDataTransferTest();
    if (dtTest != null) 
      dtTest.fiStatusRead.run(d.getDatanodeId());
  }

  pointcut receiverOpWriteBlock(DataXceiver dataxceiver):
    call(void Receiver.opWriteBlock(DataInputStream)) && target(dataxceiver);

  before(DataXceiver dataxceiver
      ) throws IOException: receiverOpWriteBlock(dataxceiver) {
    LOG.info("FI: receiverOpWriteBlock");
    DataTransferTest dtTest = DataTransferTestUtil.getDataTransferTest();
    if (dtTest != null)
      dtTest.fiReceiverOpWriteBlock.run(
          dataxceiver.getDataNode().getDatanodeId());
  }
}
