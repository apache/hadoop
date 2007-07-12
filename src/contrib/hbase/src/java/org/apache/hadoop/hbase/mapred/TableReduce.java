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
import java.util.Iterator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/**
 * Write a table, sorting by the input key
 */
public abstract class TableReduce extends MapReduceBase implements Reducer {
  private static final Logger LOG =
    Logger.getLogger(TableReduce.class.getName());

  TableOutputCollector m_collector;

  /** Constructor */
  public TableReduce() {
    m_collector = new TableOutputCollector();
  }

  /**
   * Use this before submitting a TableReduce job. It will
   * appropriately set up the JobConf.
   * 
   * @param table
   * @param reducer
   * @param job
   */
  public static void initJob(String table, Class<? extends TableReduce> reducer,
      JobConf job) {
    
    job.setOutputFormat(TableOutputFormat.class);
    job.setReducerClass(reducer);
    job.set(TableOutputFormat.OUTPUT_TABLE, table);
  }

  /**
   * Create a unique key for table insertion by appending a local
   * counter the given key.
   *
   * @see org.apache.hadoop.mapred.Reducer#reduce(org.apache.hadoop.io.WritableComparable, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
   */
  @SuppressWarnings("unchecked")
  public void reduce(WritableComparable key, Iterator values,
      OutputCollector output, Reporter reporter) throws IOException {
    LOG.debug("start reduce");
    if(m_collector.collector == null) {
      m_collector.collector = output;
    }
    reduce((Text)key, values, m_collector, reporter);
    LOG.debug("end reduce");
  }

  /**
   * 
   * @param key
   * @param values
   * @param output
   * @param reporter
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public abstract void reduce(Text key, Iterator values, 
      TableOutputCollector output, Reporter reporter) throws IOException;

}
