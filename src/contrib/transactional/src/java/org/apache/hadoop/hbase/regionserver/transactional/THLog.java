/**
 * Copyright 2009 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver.transactional;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HLog;
import org.apache.hadoop.hbase.regionserver.HLogKey;
import org.apache.hadoop.hbase.regionserver.LogRollListener;
import org.apache.hadoop.io.SequenceFile;

/**
 * Add support for transactional operations to the regionserver's
 * write-ahead-log.
 * 
 */
class THLog extends HLog {

  public THLog(FileSystem fs, Path dir, Configuration conf,
      LogRollListener listener) throws IOException {
    super(fs, dir, conf, listener);
  }

  @Override
  protected SequenceFile.Writer createWriter(Path path) throws IOException {
    return super.createWriter(path, THLogKey.class, KeyValue.class);
  }

  @Override
  protected HLogKey makeKey(byte[] regionName, byte[] tableName, long seqNum,
      long now) {
    return new THLogKey(regionName, tableName, seqNum, now);
  }

  public void writeStartToLog(HRegionInfo regionInfo, final long transactionId) throws IOException {
    this.append(regionInfo, System.currentTimeMillis(),
        THLogKey.TrxOp.START, transactionId);
  }

  public void writeUpdateToLog(HRegionInfo regionInfo, final long transactionId, final Put update)
      throws IOException {
    this.append(regionInfo, update, transactionId);
  }

  public void writeDeleteToLog(HRegionInfo regionInfo, final long transactionId, final Delete delete)
      throws IOException {
    // FIXME
  }

  public void writeCommitToLog(HRegionInfo regionInfo, final long transactionId) throws IOException {
    this.append(regionInfo, System.currentTimeMillis(),
        THLogKey.TrxOp.COMMIT, transactionId);
  }

  public void writeAbortToLog(HRegionInfo regionInfo, final long transactionId) throws IOException {
    this.append(regionInfo, System.currentTimeMillis(),
        THLogKey.TrxOp.ABORT, transactionId);
  }
  
  /**
   * Write a general transaction op to the log. This covers: start, commit, and
   * abort.
   * 
   * @param regionInfo
   * @param now
   * @param txOp
   * @param transactionId
   * @throws IOException
   */
  public void append(HRegionInfo regionInfo, long now, THLogKey.TrxOp txOp,
      long transactionId) throws IOException {
    THLogKey key = new THLogKey(regionInfo.getRegionName(), regionInfo
        .getTableDesc().getName(), -1, now, txOp, transactionId);
    super.append(regionInfo, key, new KeyValue(new byte [0], 0, 0)); // Empty KeyValue 
  }

  /**
   * Write a transactional update to the log.
   * 
   * @param regionInfo
   * @param now
   * @param update
   * @param transactionId
   * @throws IOException
   */
  public void append(HRegionInfo regionInfo, Put update, long transactionId)
      throws IOException {

    long commitTime = System.currentTimeMillis(); // FIXME ?

    THLogKey key = new THLogKey(regionInfo.getRegionName(), regionInfo
        .getTableDesc().getName(), -1, commitTime, THLogKey.TrxOp.OP,
        transactionId);

    for (KeyValue value : convertToKeyValues(update)) {
      super.append(regionInfo, key, value);
    }
  }

  private List<KeyValue> convertToKeyValues(Put update) {
    List<KeyValue> edits = new ArrayList<KeyValue>();

    for (List<KeyValue> kvs : update.getFamilyMap().values()) {
      for (KeyValue kv : kvs) {
        edits.add(kv);
      }
    }
    return edits;
  }
}
