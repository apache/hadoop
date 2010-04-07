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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.transactional.TransactionalTable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

/** HTable extended with indexed support. */
public class IndexedTable extends TransactionalTable {

  // TODO move these schema constants elsewhere
  public static final byte[] INDEX_COL_FAMILY_NAME = Bytes.toBytes("__INDEX__");
  public static final byte[] INDEX_COL_FAMILY = Bytes.add(
      INDEX_COL_FAMILY_NAME, KeyValue.COLUMN_FAMILY_DELIM_ARRAY);
  public static final byte[] INDEX_BASE_ROW = Bytes.toBytes("ROW");
  public static final byte[] INDEX_BASE_ROW_COLUMN = Bytes.add(
      INDEX_COL_FAMILY, INDEX_BASE_ROW);

  static final Log LOG = LogFactory.getLog(IndexedTable.class);

  private final IndexedTableDescriptor indexedTableDescriptor;
  private Map<String, HTable> indexIdToTable = new HashMap<String, HTable>();

  public IndexedTable(final HBaseConfiguration conf, final byte[] tableName)
      throws IOException {
    super(conf, tableName);
    this.indexedTableDescriptor = new IndexedTableDescriptor(super.getTableDescriptor());
    for (IndexSpecification spec : this.indexedTableDescriptor.getIndexes()) {
      indexIdToTable.put(spec.getIndexId(), new HTable(conf, spec
          .getIndexedTableName(tableName)));
    }
  }

  public IndexedTableDescriptor getIndexedTableDescriptor() {
    return this.indexedTableDescriptor;
  }
  
  /**
   * Open up an indexed scanner. Results will come back in the indexed order,
   * but will contain RowResults from the original table.
   * 
   * @param indexId the id of the index to use
   * @param indexStartRow (created from the IndexKeyGenerator)
   * @param indexStopRow (created from the IndexKeyGenerator)
   * @param indexColumns in the index table
   * @param indexFilter filter to run on the index'ed table. This can only use
   * columns that have been added to the index.
   * @param baseColumns from the original table
   * @return scanner
   * @throws IOException
   * @throws IndexNotFoundException
   */
  public ResultScanner getIndexedScanner(String indexId, final byte[] indexStartRow,  final byte[] indexStopRow,
      byte[][] indexColumns, final Filter indexFilter,
      final byte[][] baseColumns) throws IOException, IndexNotFoundException {
    IndexSpecification indexSpec = this.indexedTableDescriptor.getIndex(indexId);
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

    Scan indexScan = new Scan();
    indexScan.setFilter(indexFilter);
    for(byte [] column : allIndexColumns) {
      byte [][] famQf = KeyValue.parseColumn(column);
      if(famQf.length == 1) {
        indexScan.addFamily(famQf[0]);
      } else {
        indexScan.addColumn(famQf[0], famQf[1]);
      }
    }
    if (indexStartRow != null) {
      indexScan.setStartRow(indexStartRow);
    }
    if (indexStopRow != null) {
      indexScan.setStopRow(indexStopRow);
    }
    ResultScanner indexScanner = indexTable.getScanner(indexScan);

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

  private class ScannerWrapper implements ResultScanner {

    private ResultScanner indexScanner;
    private byte[][] columns;

    public ScannerWrapper(ResultScanner indexScanner, byte[][] columns) {
      this.indexScanner = indexScanner;
      this.columns = columns;
    }

    /** {@inheritDoc} */
    public Result next() throws IOException {
        Result[] result = next(1);
        if (result == null || result.length < 1)
          return null;
        return result[0];
    }

    /** {@inheritDoc} */
    public Result[] next(int nbRows) throws IOException {
      Result[] indexResult = indexScanner.next(nbRows);
      if (indexResult == null) {
        return null;
      }
      Result[] result = new Result[indexResult.length];
      for (int i = 0; i < indexResult.length; i++) {
        Result row = indexResult[i];
        
        byte[] baseRow = row.getValue(INDEX_COL_FAMILY_NAME, INDEX_BASE_ROW);
        if (baseRow == null) {
          throw new IllegalStateException("Missing base row for indexed row: ["+Bytes.toString(row.getRow())+"]");
        }
        LOG.debug("next index row [" + Bytes.toString(row.getRow())
            + "] -> base row [" + Bytes.toString(baseRow) + "]");
        Result baseResult = null;
        if (columns != null && columns.length > 0) {
          LOG.debug("Going to base table for remaining columns");
          Get baseGet = new Get(baseRow);
          for(byte [] column : columns) {
            byte [][] famQf = KeyValue.parseColumn(column);
            if(famQf.length == 1) {
              baseGet.addFamily(famQf[0]);
            } else {
              baseGet.addColumn(famQf[0], famQf[1]);
            }
          }
          baseResult = IndexedTable.this.get(baseGet);
        }
        
        List<KeyValue> results = new ArrayList<KeyValue>();
        for (KeyValue indexKV : row.list()) {
          if (indexKV.matchingFamily(INDEX_COL_FAMILY_NAME)) {
            continue;
          }
          results.add(new KeyValue(baseRow, indexKV.getFamily(), 
              indexKV.getQualifier(), indexKV.getTimestamp(), KeyValue.Type.Put, 
              indexKV.getValue()));
        }
        
        if (baseResult != null) {
          List<KeyValue> list = baseResult.list();
          if (list != null) {
            results.addAll(list);
          }
        }
        
        result[i] = new Result(results);
      }
      return result;
    }

    /** {@inheritDoc} */
    public void close() {
      indexScanner.close();
    }
    
    // Copied from HTable.ClientScanner.iterator()
    public Iterator<Result> iterator() {
      return new Iterator<Result>() {
        // The next RowResult, possibly pre-read
        Result next = null;
        
        // return true if there is another item pending, false if there isn't.
        // this method is where the actual advancing takes place, but you need
        // to call next() to consume it. hasNext() will only advance if there
        // isn't a pending next().
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

        // get the pending next item and advance the iterator. returns null if
        // there is no next item.
        public Result next() {
          // since hasNext() does the real advancing, we call this to determine
          // if there is a next before proceeding.
          if (!hasNext()) {
            return null;
          }
          
          // if we get to here, then hasNext() has given us an item to return.
          // we want to return the item and then null out the next pointer, so
          // we use a temporary variable.
          Result temp = next;
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
