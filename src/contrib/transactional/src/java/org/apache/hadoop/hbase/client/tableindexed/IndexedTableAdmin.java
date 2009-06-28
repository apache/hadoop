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
package org.apache.hadoop.hbase.client.tableindexed;

import java.io.IOException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ColumnNameParseException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.regionserver.tableindexed.IndexMaintenanceUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Extension of HBaseAdmin that creates indexed tables.
 * 
 */
public class IndexedTableAdmin extends HBaseAdmin {

  private static final Log LOG = LogFactory.getLog(IndexedTableAdmin.class);

  /**
   * Constructor
   * 
   * @param conf Configuration object
   * @throws MasterNotRunningException
   */
  public IndexedTableAdmin(HBaseConfiguration conf)
      throws MasterNotRunningException {
    super(conf);
  }

  /**
   * Creates a new indexed table
   * 
   * @param desc table descriptor for table
   * 
   * @throws IOException
   */
  public void createIndexedTable(IndexedTableDescriptor desc) throws IOException {
    super.createTable(desc.getBaseTableDescriptor());
    this.createIndexTables(desc);
  }

  private void createIndexTables(IndexedTableDescriptor indexDesc) throws IOException {
    byte[] baseTableName = indexDesc.getBaseTableDescriptor().getName();
    for (IndexSpecification indexSpec : indexDesc.getIndexes()) {
      HTableDescriptor indexTableDesc = createIndexTableDesc(baseTableName,
          indexSpec);
      super.createTable(indexTableDesc);
    }
  }

  private HTableDescriptor createIndexTableDesc(byte[] baseTableName,
      IndexSpecification indexSpec) throws ColumnNameParseException {
    HTableDescriptor indexTableDesc = new HTableDescriptor(indexSpec
        .getIndexedTableName(baseTableName));
    Set<byte[]> families = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    families.add(IndexedTable.INDEX_COL_FAMILY);
    for (byte[] column : indexSpec.getAllColumns()) {
      families.add(Bytes.add(HStoreKey.getFamily(column),
          new byte[] { HStoreKey.COLUMN_FAMILY_DELIMITER }));
    }

    for (byte[] colFamily : families) {
      indexTableDesc.addFamily(new HColumnDescriptor(colFamily));
    }

    return indexTableDesc;
  }
  
  /** Remove an index for a table. 
   * @throws IOException 
   * 
   */
  public void removeIndex(byte[] baseTableName, String indexId) throws IOException {
    super.disableTable(baseTableName);
    HTableDescriptor desc = super.getTableDescriptor(baseTableName);
    IndexedTableDescriptor indexDesc = new IndexedTableDescriptor(desc);
    IndexSpecification spec = indexDesc.getIndex(indexId);
    indexDesc.removeIndex(indexId);
    this.disableTable(spec.getIndexedTableName(baseTableName));
    this.deleteTable(spec.getIndexedTableName(baseTableName));
    super.modifyTable(baseTableName, desc);
    super.enableTable(baseTableName);
  }
  
  /** Add an index to a table. */
  public void addIndex(byte []baseTableName, IndexSpecification indexSpec) throws IOException {
    LOG.warn("Adding index to existing table ["+Bytes.toString(baseTableName)+"], this may take a long time");
    // TODO, make table read-only
    LOG.warn("Not putting table in readonly, if its being written to, the index may get out of sync");
    HTableDescriptor indexTableDesc = createIndexTableDesc(baseTableName, indexSpec);
    super.createTable(indexTableDesc);
    super.disableTable(baseTableName);
    IndexedTableDescriptor indexDesc = new IndexedTableDescriptor(super.getTableDescriptor(baseTableName));
    indexDesc.addIndex(indexSpec);
    super.modifyTable(baseTableName, indexDesc.getBaseTableDescriptor());
    super.enableTable(baseTableName);
    reIndexTable(baseTableName, indexSpec);
  }

  private void reIndexTable(byte[] baseTableName, IndexSpecification indexSpec) throws IOException {
    HTable baseTable = new HTable(baseTableName);
    HTable indexTable = new HTable(indexSpec.getIndexedTableName(baseTableName));
    for (RowResult rowResult : baseTable.getScanner(indexSpec.getAllColumns())) {
      SortedMap<byte[], byte[]> columnValues = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
      for (Entry<byte[], Cell> entry : rowResult.entrySet()) {
        columnValues.put(entry.getKey(), entry.getValue().getValue());
      }
      if (IndexMaintenanceUtils.doesApplyToIndex(indexSpec, columnValues)) {
        Put indexUpdate = IndexMaintenanceUtils.createIndexUpdate(indexSpec, rowResult.getRow(), columnValues);
        indexTable.put(indexUpdate);
      }
    }
  }
}
