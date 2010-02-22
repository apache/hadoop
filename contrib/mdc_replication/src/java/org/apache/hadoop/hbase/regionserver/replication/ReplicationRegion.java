/**
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.regionserver.replication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Specialized version of HRegion to handle replication. In particular,
 * it replays all edits from the reconstruction log.
 */
public class ReplicationRegion extends HRegion {

  static final Log LOG = LogFactory.getLog(ReplicationRegion.class);

  private final ReplicationSource replicationSource;

  public ReplicationRegion(Path basedir, HLog log, FileSystem fs, Configuration conf,
      HRegionInfo regionInfo, FlushRequester flushListener,
      ReplicationSource repSource) {
    super(basedir, log, fs, conf, regionInfo, flushListener);
    this.replicationSource = repSource;
  }


  protected void doReconstructionLog(final Path oldLogFile,
      final long minSeqId, final long maxSeqId, final Progressable reporter)
      throws UnsupportedEncodingException, IOException {
    super.doReconstructionLog(oldLogFile, minSeqId, maxSeqId, reporter);

    if(this.replicationSource == null) {
      return;
    }

    if (oldLogFile == null || !getFilesystem().exists(oldLogFile)) {
      return;
    }

    FileStatus[] stats = getFilesystem().listStatus(oldLogFile);
    if (stats == null || stats.length == 0) {
      LOG.warn("Passed reconstruction log " + oldLogFile
          + " is zero-length");
    }

    HLog.Reader reader = HLog.getReader(getFilesystem(), oldLogFile, getConf());
    try {
      HLog.Entry entry;
      while ((entry = reader.next()) != null) {
        HLogKey key = entry.getKey();
        KeyValue val = entry.getEdit();
        if (key.getLogSeqNum() < maxSeqId) {
          continue;
        }

        // Don't replicate catalog entries and meta information like
        // complete log flush.
        if(!(Bytes.equals(key.getTablename(),ROOT_TABLE_NAME) ||
            Bytes.equals(key.getTablename(),META_TABLE_NAME)) &&
            !Bytes.equals(val.getFamily(), HLog.METAFAMILY)   &&
            key.getScope() == REPLICATION_SCOPE_GLOBAL) {
          this.replicationSource.enqueueLog(entry);
        }

      }
    } finally {
      reader.close();
    }

    
  }
}
