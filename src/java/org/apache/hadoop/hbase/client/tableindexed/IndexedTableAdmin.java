/**
 * Copyright 2008 The Apache Software Foundation
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
import java.util.TreeSet;

import org.apache.hadoop.hbase.ColumnNameParseException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Extension of HBaseAdmin that creates indexed tables.
 * 
 */
public class IndexedTableAdmin extends HBaseAdmin {

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
   * Creates a new table
   * 
   * @param desc table descriptor for table
   * 
   * @throws IllegalArgumentException if the table name is reserved
   * @throws MasterNotRunningException if master is not running
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence and
   * attempt-at-creation).
   * @throws IOException
   */
  @Override
  public void createTable(HTableDescriptor desc) throws IOException {
    super.createTable(desc);
    this.createIndexTables(desc);
  }

  private void createIndexTables(HTableDescriptor tableDesc) throws IOException {
    byte[] baseTableName = tableDesc.getName();
    for (IndexSpecification indexSpec : tableDesc.getIndexes()) {
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
}
