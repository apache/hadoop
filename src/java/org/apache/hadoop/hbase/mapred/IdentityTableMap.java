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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Pass the given key and record as-is to reduce
 */
@Deprecated
public class IdentityTableMap
extends MapReduceBase
implements TableMap<ImmutableBytesWritable, RowResult> {

  /** constructor */
  public IdentityTableMap() {
    super();
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
  @SuppressWarnings("unchecked")
  public static void initJob(String table, String columns,
    Class<? extends TableMap> mapper, JobConf job) {
    TableMapReduceUtil.initTableMapJob(table, columns, mapper,
      ImmutableBytesWritable.class,
      RowResult.class, job);
  }

  /**
   * Pass the key, value to reduce
   * @param key 
   * @param value 
   * @param output 
   * @param reporter 
   * @throws IOException 
   */
  public void map(ImmutableBytesWritable key, RowResult value,
      OutputCollector<ImmutableBytesWritable,RowResult> output,
      Reporter reporter) throws IOException {
    
    // convert 
    output.collect(key, value);
  }
}
