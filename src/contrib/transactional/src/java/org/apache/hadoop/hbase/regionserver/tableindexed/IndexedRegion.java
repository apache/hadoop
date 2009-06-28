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
package org.apache.hadoop.hbase.regionserver.tableindexed;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Leases;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.tableindexed.IndexSpecification;
import org.apache.hadoop.hbase.client.tableindexed.IndexedTableDescriptor;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HLog;
import org.apache.hadoop.hbase.regionserver.transactional.TransactionalRegion;
import org.apache.hadoop.hbase.util.Bytes;

class IndexedRegion extends TransactionalRegion {

  private static final Log LOG = LogFactory.getLog(IndexedRegion.class);

  private final HBaseConfiguration conf;
  private final IndexedTableDescriptor indexTableDescriptor;
  private Map<IndexSpecification, HTable> indexSpecToTable = new HashMap<IndexSpecification, HTable>();

  public IndexedRegion(final Path basedir, final HLog log, final FileSystem fs,
      final HBaseConfiguration conf, final HRegionInfo regionInfo,
      final FlushRequester flushListener, Leases trxLeases) throws IOException {
    super(basedir, log, fs, conf, regionInfo, flushListener, trxLeases);
    this.indexTableDescriptor = new IndexedTableDescriptor(regionInfo.getTableDesc());
    this.conf = conf;
  }

  private synchronized HTable getIndexTable(IndexSpecification index)
      throws IOException {
    HTable indexTable = indexSpecToTable.get(index);
    if (indexTable == null) {
      indexTable = new HTable(conf, index.getIndexedTableName(super
          .getRegionInfo().getTableDesc().getName()));
      indexSpecToTable.put(index, indexTable);
    }
    return indexTable;
  }

  private Collection<IndexSpecification> getIndexes() {
    return indexTableDescriptor.getIndexes();
  }

  /**
   * @param batchUpdate
   * @param lockid
   * @param writeToWAL if true, then we write this update to the log
   * @throws IOException
   */
  @Override
  public void put(Put put, Integer lockId, boolean writeToWAL)
      throws IOException {
    updateIndexes(put); // Do this first because will want to see the old row
    super.put(put, lockId, writeToWAL);
  }

  private void updateIndexes(Put put) throws IOException {
    List<IndexSpecification> indexesToUpdate = new LinkedList<IndexSpecification>();

    // Find the indexes we need to update
    for (IndexSpecification index : getIndexes()) {
      if (possiblyAppliesToIndex(index, put)) {
        indexesToUpdate.add(index);
      }
    }

    if (indexesToUpdate.size() == 0) {
      return;
    }

    NavigableSet<byte[]> neededColumns = getColumnsForIndexes(indexesToUpdate);
    NavigableMap<byte[], byte[]> newColumnValues = getColumnsFromPut(put);

    Get oldGet = new Get(put.getRow());
    for (byte [] neededCol : neededColumns) {
      oldGet.addColumn(neededCol);  
    }
    
    Result oldResult = super.get(oldGet, null);
    
    // Add the old values to the new if they are not there
    if (oldResult != null && oldResult.raw() != null) {
      for (KeyValue oldKV : oldResult.raw()) {
        if (!newColumnValues.containsKey(oldKV.getColumn())) {
          newColumnValues.put(oldKV.getColumn(), oldKV.getValue());
        }
      }
    }
    
    Iterator<IndexSpecification> indexIterator = indexesToUpdate.iterator();
    while (indexIterator.hasNext()) {
      IndexSpecification indexSpec = indexIterator.next();
      if (!IndexMaintenanceUtils.doesApplyToIndex(indexSpec, newColumnValues)) {
        indexIterator.remove();
      }
    }

    SortedMap<byte[], byte[]> oldColumnValues = convertToValueMap(oldResult);
    
    for (IndexSpecification indexSpec : indexesToUpdate) {
      removeOldIndexEntry(indexSpec, put.getRow(), oldColumnValues);
      updateIndex(indexSpec, put.getRow(), newColumnValues);
    }
  }
  
  private void updateIndexes(Delete delete) {
    // FIXME
    // Handle delete batch updates. Go back and get the next older values
//    for (BatchOperation op : batchUpdate) {
//      if (!op.isPut()) { 
//        Cell current = oldColumnCells.get(op.getColumn());
//        if (current != null) {
//          // TODO: Fix this profligacy!!! St.Ack
//          Cell [] older = Cell.createSingleCellArray(super.get(batchUpdate.getRow(),
//              op.getColumn(), current.getTimestamp(), 1));
//          if (older != null && older.length > 0) {
//            newColumnValues.put(op.getColumn(), older[0].getValue());
//          }
//        }
//      }
//    }
  }

  /** Return the columns needed for the update. */
  private NavigableSet<byte[]> getColumnsForIndexes(Collection<IndexSpecification> indexes) {
    NavigableSet<byte[]> neededColumns = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    for (IndexSpecification indexSpec : indexes) {
      for (byte[] col : indexSpec.getAllColumns()) {
        neededColumns.add(col);
      }
    }
    return neededColumns;
  }

  private void removeOldIndexEntry(IndexSpecification indexSpec, byte[] row,
      SortedMap<byte[], byte[]> oldColumnValues) throws IOException {
    for (byte[] indexedCol : indexSpec.getIndexedColumns()) {
      if (!oldColumnValues.containsKey(indexedCol)) {
        LOG.debug("Index [" + indexSpec.getIndexId()
            + "] not trying to remove old entry for row ["
            + Bytes.toString(row) + "] because col ["
            + Bytes.toString(indexedCol) + "] is missing");
        return;
      }
    }

    byte[] oldIndexRow = indexSpec.getKeyGenerator().createIndexKey(row,
        oldColumnValues);
    LOG.debug("Index [" + indexSpec.getIndexId() + "] removing old entry ["
        + Bytes.toString(oldIndexRow) + "]");
    getIndexTable(indexSpec).delete(new Delete(oldIndexRow));
  }
  
  private NavigableMap<byte[], byte[]> getColumnsFromPut(Put put) {
    NavigableMap<byte[], byte[]> columnValues = new TreeMap<byte[], byte[]>(
        Bytes.BYTES_COMPARATOR);
    for (List<KeyValue> familyPuts : put.getFamilyMap().values()) {
      for (KeyValue kv : familyPuts) {
        columnValues.put(kv.getColumn(), kv.getValue());
      }
    }
    return columnValues;
  }

  /** Ask if this put *could* apply to the index. It may actually apply if some of the columns needed are missing.
   * 
   * @param indexSpec
   * @param put
   * @return true if possibly apply.
   */
  private boolean possiblyAppliesToIndex(IndexSpecification indexSpec, Put put) {
    for (List<KeyValue> familyPuts : put.getFamilyMap().values()) {
      for (KeyValue kv : familyPuts) {
        if (indexSpec.containsColumn(kv.getColumn())) {
          return true;
        }
      }
    }
    return false;
  }

  // FIXME: This call takes place in an RPC, and requires an RPC. This makes for
  // a likely deadlock if the number of RPCs we are trying to serve is >= the
  // number of handler threads.
  private void updateIndex(IndexSpecification indexSpec, byte[] row,
      SortedMap<byte[], byte[]> columnValues) throws IOException {
    Put indexUpdate = IndexMaintenanceUtils.createIndexUpdate(indexSpec, row, columnValues);
    getIndexTable(indexSpec).put(indexUpdate); 
    LOG.debug("Index [" + indexSpec.getIndexId() + "] adding new entry ["
        + Bytes.toString(indexUpdate.getRow()) + "] for row ["
        + Bytes.toString(row) + "]");

  }

  @Override
  public void delete(Delete delete, final Integer lockid, boolean writeToWAL)
      throws IOException {

    if (!getIndexes().isEmpty()) {
      // Need all columns
      NavigableSet<byte[]> neededColumns = getColumnsForIndexes(getIndexes());

      Get get = new Get(delete.getRow());
      for (byte [] col : neededColumns) {
       get.addColumn(col);
      }
      
      Result oldRow = super.get(get, null);
      SortedMap<byte[], byte[]> oldColumnValues = convertToValueMap(oldRow);
      
      
      for (IndexSpecification indexSpec : getIndexes()) {
        removeOldIndexEntry(indexSpec, delete.getRow(), oldColumnValues);
      }

      // Handle if there is still a version visible.
      if (delete.getTimeStamp() != HConstants.LATEST_TIMESTAMP) {
        get.setTimeRange(1, delete.getTimeStamp());
        oldRow = super.get(get, null);
        SortedMap<byte[], byte[]> currentColumnValues = convertToValueMap(oldRow);
        
        for (IndexSpecification indexSpec : getIndexes()) {
          if (IndexMaintenanceUtils.doesApplyToIndex(indexSpec, currentColumnValues)) {
            updateIndex(indexSpec, delete.getRow(), currentColumnValues);
          }
        }
      }
    }
    super.delete(delete, lockid, writeToWAL);
  }

  private SortedMap<byte[], byte[]> convertToValueMap(Result result) {
    SortedMap<byte[], byte[]> currentColumnValues = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    
    if (result == null || result.raw() == null) {
      return currentColumnValues;
    }
    
    for(KeyValue kv : result.list()) {
      currentColumnValues.put(kv.getColumn(), kv.getValue());
    }
    return currentColumnValues;
  }
}
