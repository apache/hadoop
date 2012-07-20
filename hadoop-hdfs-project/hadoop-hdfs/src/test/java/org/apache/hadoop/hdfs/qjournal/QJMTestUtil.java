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
package org.apache.hadoop.hdfs.qjournal;

import java.util.Arrays;

import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.io.DataOutputBuffer;

public abstract class QJMTestUtil {

  public static byte[] createTxnData(int startTxn, int numTxns) throws Exception {
    DataOutputBuffer buf = new DataOutputBuffer();
    FSEditLogOp.Writer writer = new FSEditLogOp.Writer(buf);
    
    for (long txid = startTxn; txid < startTxn + numTxns; txid++) {
      FSEditLogOp op = NameNodeAdapter.createMkdirOp("tx " + txid);
      op.setTransactionId(txid);
      writer.writeOp(op);
    }
    
    return Arrays.copyOf(buf.getData(), buf.getLength());
  }
  
}
