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

package org.apache.hadoop.fi;

import java.io.IOException;

import org.apache.hadoop.fi.FiTestUtil.ActionContainer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;

/** Helper methods and actions for hflush() fault injection tests */
public class FiHFlushTestUtil extends DataTransferTestUtil {

  /** {@inheritDoc} */
  public static PipelineTest initTest() {
    return thepipelinetest = new HFlushTest();
  }
  
  /** Disk error action for fault injection tests */
  public static class DerrAction extends DataTransferTestUtil.DataNodeAction {
    /**
     * @param currentTest The name of the test
     * @param index       The index of the datanode
     */
    public DerrAction(String currentTest, int index) {
      super(currentTest, index);
    }

    /** {@inheritDoc} */
    public void run(DatanodeID id) throws IOException {
      final Pipeline p = getPipelineTest().getPipelineForDatanode(id);
      if (p == null) {
        return;
      }
      if (p.contains(index, id)) {
        final String s = super.toString(id);
        FiTestUtil.LOG.info(s);
        throw new DiskErrorException(s);
      }
    }
  }
  
  /** Class adds new type of action */
  public static class HFlushTest extends DataTransferTest {
    public final ActionContainer<DatanodeID, IOException> fiCallHFlush = 
      new ActionContainer<DatanodeID, IOException>();
    public final ActionContainer<Integer, RuntimeException> fiErrorOnCallHFlush = 
      new ActionContainer<Integer, RuntimeException>();
  }
}