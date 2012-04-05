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
package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;

import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;

/**
 * OfflineEditsBinaryLoader loads edits from a binary edits file
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class OfflineEditsBinaryLoader implements OfflineEditsLoader {
  private OfflineEditsVisitor visitor;
  private EditLogInputStream inputStream;
  private boolean fixTxIds;
  private long nextTxId;
  
  /**
   * Constructor
   */
  public OfflineEditsBinaryLoader(OfflineEditsVisitor visitor,
        EditLogInputStream inputStream) {
    this.visitor = visitor;
    this.inputStream = inputStream;
    this.fixTxIds = false;
    this.nextTxId = -1;
  }

  /**
   * Loads edits file, uses visitor to process all elements
   */
  public void loadEdits() throws IOException {
    try {
      visitor.start(inputStream.getVersion());
      while (true) {
        FSEditLogOp op = inputStream.readOp();
        if (op == null)
          break;
        if (fixTxIds) {
          if (nextTxId <= 0) {
            nextTxId = op.getTransactionId();
            if (nextTxId <= 0) {
              nextTxId = 1;
            }
          }
          op.setTransactionId(nextTxId);
          nextTxId++;
        }
        visitor.visitOp(op);
      }
      visitor.close(null);
    } catch(IOException e) {
      // Tell the visitor to clean up, then re-throw the exception
      visitor.close(e);
      throw e;
    }
  }
  
  public void setFixTxIds() {
    fixTxIds = true;
  }
}