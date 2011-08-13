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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.util.StringUtils;

/**
 * Iterate over an HBase table data, return (ImmutableBytesWritable, Result)
 * pairs.
 */
public class TableRecordReaderImpl {


  static final Log LOG = LogFactory.getLog(TableRecordReader.class);

  private ResultScanner scanner = null;
  private Scan scan = null;
  private HTable htable = null;
  private byte[] lastSuccessfulRow = null;
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
   *
   */
  public void close() {
    this.scanner.close();
  }

  /**
   * Returns the current key.
   *
   * @return The current key.
   * @throws IOException
   * @throws InterruptedException When the job is aborted.
   */
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
   */
  public Result getCurrentValue() throws IOException, InterruptedException {
    return value;
  }


  /**
   * Positions the record reader to the next record.
   *
   * @return <code>true</code> if there was another record.
   * @throws IOException When reading the record failed.
   * @throws InterruptedException When the job was aborted.
   */
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (key == null) key = new ImmutableBytesWritable();
    if (value == null) value = new Result();
    try {
      value = this.scanner.next();
    } catch (IOException e) {
      LOG.debug("recovered from " + StringUtils.stringifyException(e));
      if (lastSuccessfulRow == null) {
        LOG.warn("We are restarting the first next() invocation," +
            " if your mapper's restarted a few other times like this" +
            " then you should consider killing this job and investigate" +
            " why it's taking so long.");
      }
      if (lastSuccessfulRow == null) {
        restart(scan.getStartRow());
      } else {
        restart(lastSuccessfulRow);
        scanner.next();    // skip presumed already mapped row
      }
      value = scanner.next();
    }
    if (value != null && value.size() > 0) {
      key.set(value.getRow());
      lastSuccessfulRow = key.get();
      return true;
    }
    return false;
  }

  /**
   * The current progress of the record reader through its data.
   *
   * @return A number between 0.0 and 1.0, the fraction of the data read.
   */
  public float getProgress() {
    // Depends on the total number of tuples
    return 0;
  }

}
