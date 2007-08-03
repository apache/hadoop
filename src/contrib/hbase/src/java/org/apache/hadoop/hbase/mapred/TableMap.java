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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.hbase.io.KeyedDataArrayWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * Scan an HBase table to sort by a specified sort column.
 * If the column does not exist, the record is not passed to Reduce.
 *
 */
public abstract class TableMap extends MapReduceBase implements Mapper {

  private static final Logger LOG = Logger.getLogger(TableMap.class.getName());

  private TableOutputCollector m_collector;

  /** constructor*/
  public TableMap() {
    m_collector = new TableOutputCollector();
  }

  /**
   * Use this before submitting a TableMap job. It will
   * appropriately set up the JobConf.
   * 
   * @param table table name
   * @param columns columns to scan
   * @param mapper mapper class
   * @param job job configuration
   */
  public static void initJob(String table, String columns, 
      Class<? extends TableMap> mapper, JobConf job) {
    
    job.setInputFormat(TableInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(KeyedDataArrayWritable.class);
    job.setMapperClass(mapper);
    job.setInputPath(new Path(table));
    job.set(TableInputFormat.COLUMN_LIST, columns);
  }

  /** {@inheritDoc} */
  @Override
  public void configure(JobConf job) {
    super.configure(job);
  }

  /**
   * Input:
   * @param key is of type HStoreKey
   * @param value is of type KeyedDataArrayWritable
   * @param output output collector
   * @param reporter object to use for status updates
   * @throws IOException
   * 
   * Output:
   * The key is a specific column, including the input key or any value
   * The value is of type LabeledData
   */
  public void map(WritableComparable key, Writable value,
      OutputCollector output, Reporter reporter) throws IOException {
    
    LOG.debug("start map");
    if(m_collector.collector == null) {
      m_collector.collector = output;
    }
    map((HStoreKey)key, (KeyedDataArrayWritable)value, m_collector, reporter);
    LOG.debug("end map");
  }

  /**
   * Call a user defined function on a single HBase record, represented
   * by a key and its associated record value.
   * 
   * @param key
   * @param value
   * @param output
   * @param reporter
   * @throws IOException
   */
  public abstract void map(HStoreKey key, KeyedDataArrayWritable value, 
      TableOutputCollector output, Reporter reporter) throws IOException;
}
