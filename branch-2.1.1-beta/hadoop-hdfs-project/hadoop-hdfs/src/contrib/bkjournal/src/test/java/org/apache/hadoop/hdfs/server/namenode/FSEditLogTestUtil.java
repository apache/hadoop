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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.OpInstanceCache;

/**
 * Utilities for testing edit logs
 */
public class FSEditLogTestUtil {
  private static OpInstanceCache cache = new OpInstanceCache();

  public static FSEditLogOp getNoOpInstance() {
    return FSEditLogOp.LogSegmentOp.getInstance(cache,
        FSEditLogOpCodes.OP_END_LOG_SEGMENT);
  }

  public static long countTransactionsInStream(EditLogInputStream in) 
      throws IOException {
    FSEditLogLoader.EditLogValidation validation = FSEditLogLoader.validateEditLog(in);
    return (validation.getEndTxId() - in.getFirstTxId()) + 1;
  }
}
