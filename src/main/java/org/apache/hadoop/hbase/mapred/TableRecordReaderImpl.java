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
package org.apache.hadoop.hbase.mapred;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.util.StringUtils;


/**
 * Iterate over an HBase table data, return (Text, RowResult) pairs
 */
public class TableRecordReaderImpl {
  static final Log LOG = LogFactory.getLog(TableRecordReaderImpl.class);

  private byte [] startRow;
  private byte [] endRow;
  private byte [] lastSuccessfulRow;
  private Filter trrRowFilter;
  private ResultScanner scanner;
  private HTable htable;
  private byte [][] trrInputColumns;

  /**
   * Restart from survivable exceptions by creating a new scanner.
   *
   * @param firstRow
   * @throws IOException
   */
  public void restart(byte[] firstRow) throws IOException {
    if ((endRow != null) && (endRow.length > 0)) {
      if (trrRowFilter != null) {
        Scan scan = new Scan(firstRow, endRow);
        scan.addColumns(trrInputColumns);
        scan.setFilter(trrRowFilter);
        scan.setCacheBlocks(false);
        this.scanner = this.htable.getScanner(scan);
      } else {
        LOG.debug("TIFB.restart, firstRow: " +
            Bytes.toStringBinary(firstRow) + ", endRow: " +
            Bytes.toStringBinary(endRow));
        Scan scan = new Scan(firstRow, endRow);
        scan.addColumns(trrInputColumns);
        this.scanner = this.htable.getScanner(scan);
      }
    } else {
      LOG.debug("TIFB.restart, firstRow: " +
          Bytes.toStringBinary(firstRow) + ", no endRow");

      Scan scan = new Scan(firstRow);
      scan.addColumns(trrInputColumns);
//      scan.setFilter(trrRowFilter);
      this.scanner = this.htable.getScanner(scan);
    }
  }

  /**
   * Build the scanner. Not done in constructor to allow for extension.
   *
   * @throws IOException
   */
  public void init() throws IOException {
    restart(startRow);
  }

  byte[] getStartRow() {
    return this.startRow;
  }
  /**
   * @param htable the {@link HTable} to scan.
   */
  public void setHTable(HTable htable) {
    this.htable = htable;
  }

  /**
   * @param inputColumns the columns to be placed in {@link Result}.
   */
  public void setInputColumns(final byte [][] inputColumns) {
    this.trrInputColumns = inputColumns;
  }

  /**
   * @param startRow the first row in the split
   */
  public void setStartRow(final byte [] startRow) {
    this.startRow = startRow;
  }

  /**
   *
   * @param endRow the last row in the split
   */
  public void setEndRow(final byte [] endRow) {
    this.endRow = endRow;
  }

  /**
   * @param rowFilter the {@link Filter} to be used.
   */
  public void setRowFilter(Filter rowFilter) {
    this.trrRowFilter = rowFilter;
  }

  public void close() {
    this.scanner.close();
  }

  /**
   * @return ImmutableBytesWritable
   *
   * @see org.apache.hadoop.mapred.RecordReader#createKey()
   */
  public ImmutableBytesWritable createKey() {
    return new ImmutableBytesWritable();
  }

  /**
   * @return RowResult
   *
   * @see org.apache.hadoop.mapred.RecordReader#createValue()
   */
  public Result createValue() {
    return new Result();
  }

  public long getPos() {
    // This should be the ordinal tuple in the range;
    // not clear how to calculate...
    return 0;
  }

  public float getProgress() {
    // Depends on the total number of tuples and getPos
    return 0;
  }

  /**
   * @param key HStoreKey as input key.
   * @param value MapWritable as input value
   * @return true if there was more data
   * @throws IOException
   */
  public boolean next(ImmutableBytesWritable key, Result value)
  throws IOException {
    Result result;
    try {
      result = this.scanner.next();
    } catch (DoNotRetryIOException e) {
      throw e;
    } catch (IOException e) {
      LOG.debug("recovered from " + StringUtils.stringifyException(e));
      if (lastSuccessfulRow == null) {
        LOG.warn("We are restarting the first next() invocation," +
            " if your mapper's restarted a few other times like this" +
            " then you should consider killing this job and investigate" +
            " why it's taking so long.");
      }
      if (lastSuccessfulRow == null) {
        restart(startRow);
      } else {
        restart(lastSuccessfulRow);
        this.scanner.next();    // skip presumed already mapped row
      }
      result = this.scanner.next();
    }

    if (result != null && result.size() > 0) {
      key.set(result.getRow());
      lastSuccessfulRow = key.get();
      Writables.copyWritable(result, value);
      return true;
    }
    return false;
  }
}
