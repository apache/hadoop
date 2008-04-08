/**
 * Copyright 2007 The Apache Software Foundation
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
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Convert HBase tabular data into a format that is consumable by Map/Reduce
 */
public class TableInputFormat
implements InputFormat<Text, RowResult>, JobConfigurable {  
  protected final Log LOG = LogFactory.getLog(TableInputFormat.class);

  /**
   * space delimited list of columns 
   * @see org.apache.hadoop.hbase.regionserver.HAbstractScanner for column name wildcards
   */
  public static final String COLUMN_LIST = "hbase.mapred.tablecolumns";
  
  private Text m_tableName;
  Text[] m_cols;
  HTable m_table;

  /**
   * Iterate over an HBase table data,
   * return (Text, RowResult) pairs
   */
  class TableRecordReader implements RecordReader<Text, RowResult> {
    private final Scanner m_scanner;

    /**
     * Constructor
     * @param startRow (inclusive)
     * @param endRow (exclusive)
     * @throws IOException
     */
    public TableRecordReader(Text startRow, Text endRow) throws IOException {
      if (endRow != null && endRow.getLength() > 0) {
        this.m_scanner = m_table.getScanner(m_cols, startRow, endRow);
      } else {
        this.m_scanner = m_table.getScanner(m_cols, startRow);
      }
    }

    /** {@inheritDoc} */
    public void close() throws IOException {
      this.m_scanner.close();
    }

    /**
     * @return Text
     *
     * @see org.apache.hadoop.mapred.RecordReader#createKey()
     */
    public Text createKey() {
      return new Text();
    }

    /**
     * @return RowResult
     *
     * @see org.apache.hadoop.mapred.RecordReader#createValue()
     */
    @SuppressWarnings("unchecked")
    public RowResult createValue() {
      return new RowResult();
    }

    /** {@inheritDoc} */
    public long getPos() {
      // This should be the ordinal tuple in the range; 
      // not clear how to calculate...
      return 0;
    }

    /** {@inheritDoc} */
    public float getProgress() {
      // Depends on the total number of tuples and getPos
      return 0;
    }

    /**
     * @param key HStoreKey as input key.
     * @param value MapWritable as input value
     * 
     * Converts Scanner.next() to
     * Text, RowResult
     * 
     * @return true if there was more data
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public boolean next(Text key, RowResult value) throws IOException {
      RowResult result = m_scanner.next();
      boolean hasMore = result != null;
      if (hasMore) {
        Writables.copyWritable(result.getRow(), key);
        Writables.copyWritable(result, value);
      }
      return hasMore;
    }

  }

  /** {@inheritDoc} */
  public RecordReader<Text, RowResult> getRecordReader(
      InputSplit split,
      @SuppressWarnings("unused") JobConf job,
      @SuppressWarnings("unused") Reporter reporter)
  throws IOException {  
    TableSplit tSplit = (TableSplit)split;
    return new TableRecordReader(tSplit.getStartRow(), tSplit.getEndRow());
  }

  /**
   * A split will be created for each HRegion of the input table
   *
   * @see org.apache.hadoop.mapred.InputFormat#getSplits(org.apache.hadoop.mapred.JobConf, int)
   */
  @SuppressWarnings("unused")
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    Text[] startKeys = m_table.getStartKeys();
    if(startKeys == null || startKeys.length == 0) {
      throw new IOException("Expecting at least one region");
    }
    InputSplit[] splits = new InputSplit[startKeys.length];
    for(int i = 0; i < startKeys.length; i++) {
      splits[i] = new TableSplit(m_tableName, startKeys[i],
          ((i + 1) < startKeys.length) ? startKeys[i + 1] : new Text());
      if (LOG.isDebugEnabled()) {
        LOG.debug("split: " + i + "->" + splits[i]);
      }
    }
    return splits;
  }

  /** {@inheritDoc} */
  public void configure(JobConf job) {
    Path[] tableNames = job.getInputPaths();
    m_tableName = new Text(tableNames[0].getName());
    String colArg = job.get(COLUMN_LIST);
    String[] colNames = colArg.split(" ");
    m_cols = new Text[colNames.length];
    for(int i = 0; i < m_cols.length; i++) {
      m_cols[i] = new Text(colNames[i]);
    }
    try {
      m_table = new HTable(new HBaseConfiguration(job), m_tableName);
    } catch (Exception e) {
      LOG.error(e);
    }
  }

  /** {@inheritDoc} */
  public void validateInput(JobConf job) throws IOException {
    // expecting exactly one path
    Path[] tableNames = job.getInputPaths();
    if(tableNames == null || tableNames.length > 1) {
      throw new IOException("expecting one table name");
    }

    // expecting at least one column
    String colArg = job.get(COLUMN_LIST);
    if(colArg == null || colArg.length() == 0) {
      throw new IOException("expecting at least one column");
    }
  }
}
