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
package org.apache.hadoop.hbase.mapred;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.filter.RowFilterSet;
import org.apache.hadoop.hbase.filter.StopRowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchRowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;

/**
 * A Base for {@link TableInputFormat}s. Receives a {@link HTable}, a
 * byte[] of input columns and optionally a {@link RowFilterInterface}.
 * Subclasses may use other TableRecordReader implementations.
 * <p>
 * An example of a subclass:
 * <pre>
 *   class ExampleTIF extends TableInputFormatBase implements JobConfigurable {
 *
 *     public void configure(JobConf job) {
 *       HTable exampleTable = new HTable(new HBaseConfiguration(job),
 *         Bytes.toBytes("exampleTable"));
 *       // mandatory
 *       setHTable(exampleTable);
 *       Text[] inputColumns = new byte [][] { Bytes.toBytes("columnA"),
 *         Bytes.toBytes("columnB") };
 *       // mandatory
 *       setInputColumns(inputColumns);
 *       RowFilterInterface exampleFilter = new RegExpRowFilter("keyPrefix.*");
 *       // optional
 *       setRowFilter(exampleFilter);
 *     }
 *
 *     public void validateInput(JobConf job) throws IOException {
 *     }
 *  }
 * </pre>
 */

@Deprecated
public abstract class TableInputFormatBase
implements InputFormat<ImmutableBytesWritable, RowResult> {
  final Log LOG = LogFactory.getLog(TableInputFormatBase.class);
  private byte [][] inputColumns;
  private HTable table;
  private TableRecordReader tableRecordReader;
  private RowFilterInterface rowFilter;

  /**
   * Iterate over an HBase table data, return (Text, RowResult) pairs
   */
  protected class TableRecordReader
  implements RecordReader<ImmutableBytesWritable, RowResult> {
    private byte [] startRow;
    private byte [] endRow;
    private byte [] lastRow;
    private RowFilterInterface trrRowFilter;
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
          final Set<RowFilterInterface> rowFiltersSet =
            new HashSet<RowFilterInterface>();
          rowFiltersSet.add(new WhileMatchRowFilter(new StopRowFilter(endRow)));
          rowFiltersSet.add(trrRowFilter);
          Scan scan = new Scan(startRow);
          scan.addColumns(trrInputColumns);
//          scan.setFilter(new RowFilterSet(RowFilterSet.Operator.MUST_PASS_ALL,
//              rowFiltersSet));
          this.scanner = this.htable.getScanner(scan);
        } else {
          Scan scan = new Scan(firstRow, endRow);
          scan.addColumns(trrInputColumns);
          this.scanner = this.htable.getScanner(scan);
        }
      } else {
        Scan scan = new Scan(firstRow);
        scan.addColumns(trrInputColumns);
//        scan.setFilter(trrRowFilter);
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

    /**
     * @param htable the {@link HTable} to scan.
     */
    public void setHTable(HTable htable) {
      this.htable = htable;
    }

    /**
     * @param inputColumns the columns to be placed in {@link RowResult}.
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
     * @param rowFilter the {@link RowFilterInterface} to be used.
     */
    public void setRowFilter(RowFilterInterface rowFilter) {
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
    public RowResult createValue() {
      return new RowResult();
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
    public boolean next(ImmutableBytesWritable key, RowResult value)
    throws IOException {
      Result result;
      try {
        result = this.scanner.next();
      } catch (UnknownScannerException e) {
        LOG.debug("recovered from " + StringUtils.stringifyException(e));  
        restart(lastRow);
        this.scanner.next();    // skip presumed already mapped row
        result = this.scanner.next();
      }

      if (result != null && result.size() > 0) {
        key.set(result.getRow());
        lastRow = key.get();
        Writables.copyWritable(result.getRowResult(), value);
        return true;
      }
      return false;
    }
  }

  /**
   * Builds a TableRecordReader. If no TableRecordReader was provided, uses
   * the default.
   *
   * @see org.apache.hadoop.mapred.InputFormat#getRecordReader(InputSplit,
   *      JobConf, Reporter)
   */
  public RecordReader<ImmutableBytesWritable, RowResult> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter)
  throws IOException {
    TableSplit tSplit = (TableSplit) split;
    TableRecordReader trr = this.tableRecordReader;
    // if no table record reader was provided use default
    if (trr == null) {
      trr = new TableRecordReader();
    }
    trr.setStartRow(tSplit.getStartRow());
    trr.setEndRow(tSplit.getEndRow());
    trr.setHTable(this.table);
    trr.setInputColumns(this.inputColumns);
    trr.setRowFilter(this.rowFilter);
    trr.init();
    return trr;
  }

  /**
   * Calculates the splits that will serve as input for the map tasks.
   * <ul>
   * Splits are created in number equal to the smallest between numSplits and
   * the number of {@link HRegion}s in the table. If the number of splits is
   * smaller than the number of {@link HRegion}s then splits are spanned across
   * multiple {@link HRegion}s and are grouped the most evenly possible. In the
   * case splits are uneven the bigger splits are placed first in the
   * {@link InputSplit} array.
   *
   * @param job the map task {@link JobConf}
   * @param numSplits a hint to calculate the number of splits (mapred.map.tasks).
   *
   * @return the input splits
   *
   * @see org.apache.hadoop.mapred.InputFormat#getSplits(org.apache.hadoop.mapred.JobConf, int)
   */
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    byte [][] startKeys = this.table.getStartKeys();
    if (startKeys == null || startKeys.length == 0) {
      throw new IOException("Expecting at least one region");
    }
    if (this.table == null) {
      throw new IOException("No table was provided");
    }
    if (this.inputColumns == null || this.inputColumns.length == 0) {
      throw new IOException("Expecting at least one column");
    }
    int realNumSplits = numSplits > startKeys.length? startKeys.length:
      numSplits;
    InputSplit[] splits = new InputSplit[realNumSplits];
    int middle = startKeys.length / realNumSplits;
    int startPos = 0;
    for (int i = 0; i < realNumSplits; i++) {
      int lastPos = startPos + middle;
      lastPos = startKeys.length % realNumSplits > i ? lastPos + 1 : lastPos;
      String regionLocation = table.getRegionLocation(startKeys[startPos]).
        getServerAddress().getHostname(); 
      splits[i] = new TableSplit(this.table.getTableName(),
        startKeys[startPos], ((i + 1) < realNumSplits) ? startKeys[lastPos]:
          HConstants.EMPTY_START_ROW, regionLocation);
      LOG.info("split: " + i + "->" + splits[i]);
      startPos = lastPos;
    }
    return splits;
  }

  /**
   * @param inputColumns to be passed in {@link RowResult} to the map task.
   */
  protected void setInputColumns(byte [][] inputColumns) {
    this.inputColumns = inputColumns;
  }

  /**
   * Allows subclasses to get the {@link HTable}.
   */
  protected HTable getHTable() {
    return this.table;
  }

  /**
   * Allows subclasses to set the {@link HTable}.
   *
   * @param table to get the data from
   */
  protected void setHTable(HTable table) {
    this.table = table;
  }

  /**
   * Allows subclasses to set the {@link TableRecordReader}.
   *
   * @param tableRecordReader
   *                to provide other {@link TableRecordReader} implementations.
   */
  protected void setTableRecordReader(TableRecordReader tableRecordReader) {
    this.tableRecordReader = tableRecordReader;
  }

  /**
   * Allows subclasses to set the {@link RowFilterInterface} to be used.
   *
   * @param rowFilter
   */
  protected void setRowFilter(RowFilterInterface rowFilter) {
    this.rowFilter = rowFilter;
  }
}
