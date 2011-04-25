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

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.List;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

/**
 * Class for testing WALObserver coprocessor.
 *
 * It will monitor WAL writing and restoring, and modify passed-in WALEdit, i.e,
 * ignore specified columns when writing, or add a KeyValue. On the other
 * side, it checks whether the ignored column is still in WAL when Restoreed
 * at region reconstruct.
 */
public class SampleRegionWALObserver extends BaseRegionObserver
implements WALObserver {

  private static final Log LOG = LogFactory.getLog(SampleRegionWALObserver.class);

  private byte[] tableName;
  private byte[] row;
  private byte[] ignoredFamily;
  private byte[] ignoredQualifier;
  private byte[] addedFamily;
  private byte[] addedQualifier;
  private byte[] changedFamily;
  private byte[] changedQualifier;

  private boolean preWALWriteCalled = false;
  private boolean postWALWriteCalled = false;
  private boolean preWALRestoreCalled = false;
  private boolean postWALRestoreCalled = false;

  /**
   * Set values: with a table name, a column name which will be ignored, and
   * a column name which will be added to WAL.
   */
  public void setTestValues(byte[] tableName, byte[] row, byte[] igf, byte[] igq,
      byte[] chf, byte[] chq, byte[] addf, byte[] addq) {
    this.row = row;
    this.tableName = tableName;
    this.ignoredFamily = igf;
    this.ignoredQualifier = igq;
    this.addedFamily = addf;
    this.addedQualifier = addq;
    this.changedFamily = chf;
    this.changedQualifier = chq;
  }


  @Override
  public void postWALWrite(ObserverContext<WALCoprocessorEnvironment> env,
      HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
    postWALWriteCalled = true;
  }

  @Override
  public boolean preWALWrite(ObserverContext<WALCoprocessorEnvironment> env,
      HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
    boolean bypass = false;
    // check table name matches or not.
    if (!Arrays.equals(HRegionInfo.getTableName(info.getRegionName()), this.tableName)) {
      return bypass;
    }
    preWALWriteCalled = true;
    // here we're going to remove one keyvalue from the WALEdit, and add
    // another one to it.
    List<KeyValue> kvs = logEdit.getKeyValues();
    KeyValue deletedKV = null;
    for (KeyValue kv : kvs) {
      // assume only one kv from the WALEdit matches.
      byte[] family = kv.getFamily();
      byte[] qulifier = kv.getQualifier();

      if (Arrays.equals(family, ignoredFamily) &&
          Arrays.equals(qulifier, ignoredQualifier)) {
        LOG.debug("Found the KeyValue from WALEdit which should be ignored.");
        deletedKV = kv;
      }
      if (Arrays.equals(family, changedFamily) &&
          Arrays.equals(qulifier, changedQualifier)) {
        LOG.debug("Found the KeyValue from WALEdit which should be changed.");
        kv.getBuffer()[kv.getValueOffset()] += 1;
      }
    }
    kvs.add(new KeyValue(row, addedFamily, addedQualifier));
    if (deletedKV != null) {
      LOG.debug("About to delete a KeyValue from WALEdit.");
      kvs.remove(deletedKV);
    }
    return bypass;
  }

  /**
   * Triggered before  {@link org.apache.hadoop.hbase.regionserver.HRegion} when WAL is
   * Restoreed.
   */
  @Override
  public void preWALRestore(ObserverContext<RegionCoprocessorEnvironment> env,
      HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
    preWALRestoreCalled = true;
  }

  /**
   * Triggered after {@link org.apache.hadoop.hbase.regionserver.HRegion} when WAL is
   * Restoreed.
   */
  @Override
  public void postWALRestore(ObserverContext<RegionCoprocessorEnvironment> env,
      HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
    postWALRestoreCalled = true;
  }

  public boolean isPreWALWriteCalled() {
    return preWALWriteCalled;
  }

  public boolean isPostWALWriteCalled() {
    return postWALWriteCalled;
  }

  public boolean isPreWALRestoreCalled() {
    LOG.debug(SampleRegionWALObserver.class.getName() +
      ".isPreWALRestoreCalled is called.");
    return preWALRestoreCalled;
  }

  public boolean isPostWALRestoreCalled() {
    LOG.debug(SampleRegionWALObserver.class.getName() +
      ".isPostWALRestoreCalled is called.");
    return postWALRestoreCalled;
  }
}
