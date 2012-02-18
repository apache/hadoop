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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fi.DataTransferTestUtil;
import org.apache.hadoop.fi.PipelineTest;
import org.apache.hadoop.fi.DataTransferTestUtil.DataTransferTest;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSOutputStream.DataStreamer;
import org.apache.hadoop.hdfs.PipelinesTestUtil.PipelinesTest;
import org.junit.Assert;

/** Aspects for DFSClient */
privileged public aspect DFSClientAspects {
  public static final Log LOG = LogFactory.getLog(DFSClientAspects.class);

  pointcut callCreateBlockOutputStream(DataStreamer datastreamer):
    call(* createBlockOutputStream(..)) && target(datastreamer);

  before(DataStreamer datastreamer) : callCreateBlockOutputStream(datastreamer) {
    Assert.assertFalse(datastreamer.hasError);
    Assert.assertEquals(-1, datastreamer.errorIndex);
  }

  pointcut pipelineInitNonAppend(DataStreamer datastreamer):
    callCreateBlockOutputStream(datastreamer) 
    && cflow(execution(* nextBlockOutputStream(..)))
    && within(DataStreamer);

  after(DataStreamer datastreamer) returning : pipelineInitNonAppend(datastreamer) {
    LOG.info("FI: after pipelineInitNonAppend: hasError="
        + datastreamer.hasError + " errorIndex=" + datastreamer.errorIndex);
    if (datastreamer.hasError) {
      DataTransferTest dtTest = DataTransferTestUtil.getDataTransferTest();
      if (dtTest != null)
        dtTest.fiPipelineInitErrorNonAppend.run(datastreamer.errorIndex);
    }
  }

  pointcut pipelineInitAppend(DataStreamer datastreamer):
    callCreateBlockOutputStream(datastreamer) 
    && cflow(execution(* initAppend(..)))
    && within(DataStreamer);

  after(DataStreamer datastreamer) returning : pipelineInitAppend(datastreamer) {
    LOG.info("FI: after pipelineInitAppend: hasError=" + datastreamer.hasError
        + " errorIndex=" + datastreamer.errorIndex);
  }

  pointcut pipelineErrorAfterInit(DataStreamer datastreamer):
    call(* processDatanodeError())
    && within (DFSOutputStream.DataStreamer)
    && target(datastreamer);

  before(DataStreamer datastreamer) : pipelineErrorAfterInit(datastreamer) {
    LOG.info("FI: before pipelineErrorAfterInit: errorIndex="
        + datastreamer.errorIndex);
    DataTransferTest dtTest = DataTransferTestUtil.getDataTransferTest();
    if (dtTest != null )
      dtTest.fiPipelineErrorAfterInit.run(datastreamer.errorIndex);
  }

  pointcut pipelineClose(DFSOutputStream out):
    call(void flushInternal())
    && withincode (void DFSOutputStream.close())
    && this(out);

  before(DFSOutputStream out) : pipelineClose(out) {
    LOG.info("FI: before pipelineClose:");
  }

  pointcut checkAckQueue(DFSOutputStream stream):
    call (void DFSOutputStream.waitAndQueueCurrentPacket())
    && withincode (void DFSOutputStream.writeChunk(..))
    && this(stream);

  after(DFSOutputStream stream) : checkAckQueue (stream) {
    DFSOutputStream.Packet cp = stream.currentPacket;
    PipelineTest pTest = DataTransferTestUtil.getDataTransferTest();
    if (pTest != null && pTest instanceof PipelinesTest) {
      LOG.debug("FI: Recording packet # " + cp.seqno
          + " where queuing has occurred");
      ((PipelinesTest) pTest).setVerified(cp.seqno);
    }
  }
}
