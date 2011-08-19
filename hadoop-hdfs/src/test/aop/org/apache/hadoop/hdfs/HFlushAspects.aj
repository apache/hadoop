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
import org.apache.hadoop.fi.PipelineTest;
import org.apache.hadoop.fi.FiHFlushTestUtil.HFlushTest;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public aspect HFlushAspects {
  public static final Log LOG = LogFactory.getLog(HFlushAspects.class);

  pointcut hflushCall (DFSOutputStream outstream) :
    execution(void DFSOutputStream.hflush(..))
    && target (outstream); 
  
  /** This advise is suppose to initiate a call to the action (fiCallHFlush)
   *  which will throw DiskErrorException if a pipeline has been created
   *  and datanodes used are belong to that very pipeline
   */
  after (DFSOutputStream streamer) throws IOException : hflushCall(streamer) {
    LOG.info("FI: hflush for any datanode");    
    LOG.info("FI: hflush " + thisJoinPoint.getThis());
    DatanodeInfo[] nodes = streamer.getPipeline();
    if (nodes == null) {
        LOG.info("No pipeline is built");
        return;
    }
    PipelineTest pt = DataTransferTestUtil.getPipelineTest();
    if (pt == null) {
        LOG.info("No test has been initialized");    
        return;
    }
    if (pt instanceof HFlushTest)
      for (int i=0; i<nodes.length; i++) {
        try {
          ((HFlushTest)pt).fiCallHFlush.run(nodes[i]);
        } catch (IOException ioe) {
          ((HFlushTest)pt).fiErrorOnCallHFlush.run(i);
        }
      }
  }
}
