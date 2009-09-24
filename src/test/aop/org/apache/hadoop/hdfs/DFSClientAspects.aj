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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fi.DataTransferTestUtil;
import org.apache.hadoop.hdfs.DFSClient.DFSOutputStream.DataStreamer;

import org.junit.Assert;

/** Aspects for DFSClient */
public aspect DFSClientAspects {
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
    try {
      if (datastreamer.hasError) {
        DataTransferTestUtil.getDataTransferTest().fiPipelineInitErrorNonAppend
            .run(datastreamer.errorIndex);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
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
    && within (DFSClient.DFSOutputStream.DataStreamer)
    && target(datastreamer);

  before(DataStreamer datastreamer) : pipelineErrorAfterInit(datastreamer) {
    LOG.info("FI: before pipelineErrorAfterInit: errorIndex="
        + datastreamer.errorIndex);
    try {
      DataTransferTestUtil.getDataTransferTest().fiPipelineErrorAfterInit
          .run(datastreamer.errorIndex);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
