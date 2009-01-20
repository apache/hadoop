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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.client.transactional.TransactionalTable;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;

/** HTable extended with indexed support. */
public class IndexedTable extends TransactionalTable {

  // FIXME, these belong elsewhere
  public static final byte[] INDEX_COL_FAMILY_NAME = Bytes.toBytes("__INDEX__");
  public static final byte[] INDEX_COL_FAMILY = Bytes.add(
      INDEX_COL_FAMILY_NAME, new byte[] { HStoreKey.COLUMN_FAMILY_DELIMITER });
  public static final byte[] INDEX_BASE_ROW_COLUMN = Bytes.add(
      INDEX_COL_FAMILY, Bytes.toBytes("ROW"));

  static final Log LOG = LogFactory.getLog(IndexedTable.class);

  private Map<String, HTable> indexIdToTable = new HashMap<String, HTable>();

  public IndexedTable(final HBaseConfiguration conf, final byte[] tableName)
      throws IOException {
    super(conf, tableName);

    for (IndexSpecification spec : super.getTableDescriptor().getIndexes()) {
      indexIdToTable.put(spec.getIndexId(), new HTable(conf, spec
          .getIndexedTableName(tableName)));
    }
  }

  /**
   * Open up an indexed scanner. Results will come back in the indexed order,
   * but will contain RowResults from the original table.
   * 
   * @param indexId the id of the index to use
   * @param indexStartRow (created from the IndexKeyGenerator)
   * @param indexColumns in the index table
   * @param indexFilter filter to run on the index'ed table. This can only use
   * columns that have been added to the index.
   * @param baseColumns from the original table
   * @return scanner
   * @throws IOException
   * @throws IndexNotFoundException
   */
  public Scanner getIndexedScanner(String indexId, final byte[] indexStartRow,
      byte[][] indexColumns, final RowFilterInterface indexFilter,
      final byte[][] baseColumns) throws IOException, IndexNotFoundException {
    IndexSpecification indexSpec = super.getTableDescriptor().getIndex(indexId);
    if (indexSpec == null) {
      throw new IndexNotFoundException("Index " + indexId
          + " not defined in table "
          + super.getTableDescriptor().getNameAsString());
    }
    verifyIndexColumns(indexColumns, indexSpec);
    // TODO, verify/remove index columns from baseColumns

    HTable indexTable = indexIdToTable.get(indexId);

    byte[][] allIndexColumns;
    if (indexColumns != null) {
      allIndexColumns = new byte[indexColumns.length + 1][];
      System
          .arraycopy(indexColumns, 0, allIndexColumns, 0, indexColumns.length);
      allIndexColumns[indexColumns.length] = INDEX_BASE_ROW_COLUMN;
    } else {
      byte[][] allColumns = indexSpec.getAllColumns();
      allIndexColumns = new byte[allColumns.length + 1][];
      System.arraycopy(allColumns, 0, allIndexColumns, 0, allColumns.length);
      allIndexColumns[allColumns.length] = INDEX_BASE_ROW_COLUMN;
    }

    Scanner indexScanner = indexTable.getScanner(allIndexColumns,
        indexStartRow, indexFilter);

    return new ScannerWrapper(indexScanner, baseColumns);
  }

  private void verifyIndexColumns(byte[][] requestedColumns,
      IndexSpecification indexSpec) {
    if (requestedColumns == null) {
      return;
    }
    for (byte[] requestedColumn : requestedColumns) {
      boolean found = false;
      for (byte[] indexColumn : indexSpec.getAllColumns()) {
        if (Bytes.equals(requestedColumn, indexColumn)) {
          found = true;
          break;
        }
      }
      if (!found) {
        throw new RuntimeException("Column [" + Bytes.toString(requestedColumn)
            + "] not in index " + indexSpec.getIndexId());
      }
    }
  }

  private class ScannerWrapper implements Scanner {

    private Scanner indexScanner;
    private byte[][] columns;

    public ScannerWrapper(Scanner indexScanner, byte[][] columns) {
      this.indexScanner = indexScanner;
      this.columns = columns;
    }

    /** {@inheritDoc} */
    public RowResult next() throws IOException {
        RowResult[] result = next(1);
        if (result == null || result.length < 1)
          return null;
        return result[0];
    }

    /** {@inheritDoc} */
    public RowResult[] next(int nbRows) throws IOException {
      RowResult[] indexResult = indexScanner.next(nbRows);
      if (indexResult == null) {
        return null;
      }
      RowResult[] result = new RowResult[indexResult.length];
      for (int i = 0; i < indexResult.length; i++) {
        RowResult row = indexResult[i];
        byte[] baseRow = row.get(INDEX_BASE_ROW_COLUMN).getValue();
        LOG.debug("next index row [" + Bytes.toString(row.getRow())
            + "] -> base row [" + Bytes.toString(baseRow) + "]");
        HbaseMapWritable<byte[], Cell> colValues =
          new HbaseMapWritable<byte[], Cell>();
        if (columns != null && columns.length > 0) {
          LOG.debug("Going to base table for remaining columns");
          RowResult baseResult = IndexedTable.this.getRow(baseRow, columns);
          
          if (baseResult != null) {
            colValues.putAll(baseResult);
          }
        }
        for (Entry<byte[], Cell> entry : row.entrySet()) {
          byte[] col = entry.getKey();
          if (HStoreKey.matchingFamily(INDEX_COL_FAMILY_NAME, col)) {
            continue;
          }
          colValues.put(col, entry.getValue());
        }
        result[i] = new RowResult(baseRow, colValues);
      }
      return result;
    }

    /** {@inheritDoc} */
    public void close() {
      indexScanner.close();
    }

    /** {@inheritDoc} */
    public Iterator<RowResult> iterator() {
      // FIXME, copied from HTable.ClientScanner. Extract this to common base
      // class?
      return new Iterator<RowResult>() {
        RowResult next = null;

        public boolean hasNext() {
          if (next == null) {
            try {
              next = ScannerWrapper.this.next();
              return next != null;
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
          return true;
        }

        public RowResult next() {
          if (!hasNext()) {
            return null;
          }
          RowResult temp = next;
          next = null;
          return temp;
        }

        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

  }
}
