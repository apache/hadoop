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
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Writables;
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
   * Iterate over an HBase table data, return (ImmutableBytesWritable, Result) 
   * pairs.
   */
  protected class TableRecordReader
  extends RecordReader<ImmutableBytesWritable, Result> {
    
    private ResultScanner scanner = null;
    private Scan scan = null;
    private HTable htable = null;
    private byte[] lastRow = null;
    private ImmutableBytesWritable key = null;
    private Result value = null;

    /**
     * Restart from survivable exceptions by creating a new scanner.
     *
     * @param firstRow  The first row to start at.
     * @throws IOException When restarting fails.
     */
    public void restart(byte[] firstRow) throws IOException {
      Scan newScan = new Scan(scan);
      newScan.setStartRow(firstRow);
      this.scanner = this.htable.getScanner(newScan);      
    }

    /**
     * Build the scanner. Not done in constructor to allow for extension.
     *
     * @throws IOException When restarting the scan fails. 
     */
    public void init() throws IOException {
      restart(scan.getStartRow());
    }

    /**
     * Sets the HBase table.
     * 
     * @param htable  The {@link HTable} to scan.
     */
    public void setHTable(HTable htable) {
      this.htable = htable;
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
     * Closes the split.
     * 
     * @see org.apache.hadoop.mapreduce.RecordReader#close()
     */
    @Override
    public void close() {
      this.scanner.close();
    }

    /**
     * Returns the current key.
     *  
     * @return The current key.
     * @throws IOException
     * @throws InterruptedException When the job is aborted.
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
     */
    @Override
    public ImmutableBytesWritable getCurrentKey() throws IOException,
        InterruptedException {
      return key;
    }

    /**
     * Returns the current value.
     * 
     * @return The current value.
     * @throws IOException When the value is faulty.
     * @throws InterruptedException When the job is aborted.
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
     */
    @Override
    public Result getCurrentValue() throws IOException, InterruptedException {
      return value;
    }

    /**
     * Initializes the reader.
     * 
     * @param inputsplit  The split to work with.
     * @param context  The current task context.
     * @throws IOException When setting up the reader fails.
     * @throws InterruptedException When the job is aborted.
     * @see org.apache.hadoop.mapreduce.RecordReader#initialize(
     *   org.apache.hadoop.mapreduce.InputSplit, 
     *   org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public void initialize(InputSplit inputsplit,
        TaskAttemptContext context) throws IOException,
        InterruptedException {
    }

    /**
     * Positions the record reader to the next record.
     *  
     * @return <code>true</code> if there was another record.
     * @throws IOException When reading the record failed.
     * @throws InterruptedException When the job was aborted.
     * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (key == null) key = new ImmutableBytesWritable();
      if (value == null) value = new Result();
      Result result;
      try {
        result = this.scanner.next();
      } catch (UnknownScannerException e) {
        LOG.debug("recovered from " + StringUtils.stringifyException(e));  
        restart(lastRow);
        scanner.next();    // skip presumed already mapped row
        result = scanner.next();
      }
      if (result != null && result.size() > 0) {
        key.set(result.getRow());
        lastRow = key.get();
        Writables.copyWritable(result, value);
        return true;
      }
      return false;
    }

    /**
     * The current progress of the record reader through its data.
     * 
     * @return A number between 0.0 and 1.0, the fraction of the data read.
     * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
     */
    @Override
    public float getProgress() {
      // Depends on the total number of tuples
      return 0;
    }
  }

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
    Scan sc = new Scan(scan);
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
    byte [][] startKeys = table.getStartKeys();
    if (startKeys == null || startKeys.length == 0) {
      throw new IOException("Expecting at least one region.");
    }
    if (table == null) {
      throw new IOException("No table was provided.");
    }
    if (!scan.hasFamilies()) {
      throw new IOException("Expecting at least one column.");
    }
    int realNumSplits = startKeys.length;
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
    return Arrays.asList(splits);
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
    if (scan == null) scan = new Scan();
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
