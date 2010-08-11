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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;

/**
 * A base for {@link TableInputFormat}s. Receives a {@link HTable}, an
 * {@link Scan} instance that defines the input columns etc. Subclasses may use
 * other TableRecordReader implementations.
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
public abstract class TableInputFormatBase
extends InputFormat<ImmutableBytesWritable, Result> {

  final Log LOG = LogFactory.getLog(TableInputFormatBase.class);

  /** Holds the details for the internal scanner. */
  private Scan scan = null;
  /** The table to scan. */
  private HTable table = null;
  /** The reader scanning the table, can be a custom one. */
  private TableRecordReader tableRecordReader = null;


  /**
   * Builds a TableRecordReader. If no TableRecordReader was provided, uses
   * the default.
   *
   * @param split  The split to work with.
   * @param context  The current context.
   * @return The newly created record reader.
   * @throws IOException When creating the reader fails.
   * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(
   *   org.apache.hadoop.mapreduce.InputSplit,
   *   org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  public RecordReader<ImmutableBytesWritable, Result> createRecordReader(
      InputSplit split, TaskAttemptContext context)
  throws IOException {
    TableSplit tSplit = (TableSplit) split;
    TableRecordReader trr = this.tableRecordReader;
    // if no table record reader was provided use default
    if (trr == null) {
      trr = new TableRecordReader();
    }
    Scan sc = new Scan(this.scan);
    sc.setStartRow(tSplit.getStartRow());
    sc.setStopRow(tSplit.getEndRow());
    trr.setScan(sc);
    trr.setHTable(table);
    trr.init();
    return trr;
  }

  /**
   * Calculates the splits that will serve as input for the map tasks. The
   * number of splits matches the number of regions in a table.
   *
   * @param context  The current job context.
   * @return The list of input splits.
   * @throws IOException When creating the list of splits fails.
   * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(
   *   org.apache.hadoop.mapreduce.JobContext)
   */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
	if (table == null) {
	    throw new IOException("No table was provided.");
	}
    Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
    if (keys == null || keys.getFirst() == null ||
        keys.getFirst().length == 0) {
      throw new IOException("Expecting at least one region.");
    }
    int count = 0;
    List<InputSplit> splits = new ArrayList<InputSplit>(keys.getFirst().length);
    for (int i = 0; i < keys.getFirst().length; i++) {
      if ( !includeRegionInSplit(keys.getFirst()[i], keys.getSecond()[i])) {
        continue;
      }
      String regionLocation = table.getRegionLocation(keys.getFirst()[i]).
        getServerAddress().getHostname();
      byte[] startRow = scan.getStartRow();
      byte[] stopRow = scan.getStopRow();
      // determine if the given start an stop key fall into the region
      if ((startRow.length == 0 || keys.getSecond()[i].length == 0 ||
           Bytes.compareTo(startRow, keys.getSecond()[i]) < 0) &&
          (stopRow.length == 0 ||
           Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {
        byte[] splitStart = startRow.length == 0 ||
          Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ?
            keys.getFirst()[i] : startRow;
        byte[] splitStop = (stopRow.length == 0 ||
          Bytes.compareTo(keys.getSecond()[i], stopRow) <= 0) &&
          keys.getSecond()[i].length > 0 ?
            keys.getSecond()[i] : stopRow;
        InputSplit split = new TableSplit(table.getTableName(),
          splitStart, splitStop, regionLocation);
        splits.add(split);
        if (LOG.isDebugEnabled())
          LOG.debug("getSplits: split -> " + (count++) + " -> " + split);
      }
    }
    return splits;
  }

  /**
   *
   *
   * Test if the given region is to be included in the InputSplit while splitting
   * the regions of a table.
   * <p>
   * This optimization is effective when there is a specific reasoning to exclude an entire region from the M-R job,
   * (and hence, not contributing to the InputSplit), given the start and end keys of the same. <br>
   * Useful when we need to remember the last-processed top record and revisit the [last, current) interval for M-R processing,
   * continuously. In addition to reducing InputSplits, reduces the load on the region server as well, due to the ordering of the keys.
   * <br>
   * <br>
   * Note: It is possible that <code>endKey.length() == 0 </code> , for the last (recent) region.
   * <br>
   * Override this method, if you want to bulk exclude regions altogether from M-R. By default, no region is excluded( i.e. all regions are included).
   *
   *
   * @param startKey Start key of the region
   * @param endKey End key of the region
   * @return true, if this region needs to be included as part of the input (default).
   *
   */
  protected boolean includeRegionInSplit(final byte[] startKey, final byte [] endKey) {
    return true;
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
   * @param table  The table to get the data from.
   */
  protected void setHTable(HTable table) {
    this.table = table;
  }

  /**
   * Gets the scan defining the actual details like columns etc.
   *
   * @return The internal scan instance.
   */
  public Scan getScan() {
    if (this.scan == null) this.scan = new Scan();
    return scan;
  }

  /**
   * Sets the scan defining the actual details like columns etc.
   *
   * @param scan  The scan to set.
   */
  public void setScan(Scan scan) {
    this.scan = scan;
  }

  /**
   * Allows subclasses to set the {@link TableRecordReader}.
   *
   * @param tableRecordReader A different {@link TableRecordReader}
   *   implementation.
   */
  protected void setTableRecordReader(TableRecordReader tableRecordReader) {
    this.tableRecordReader = tableRecordReader;
  }

}
