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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * Convenience class that simply writes each key, record pair to the configured 
 * HBase table.
 */
public class IdentityTableReducer 
extends TableReducer<ImmutableBytesWritable, Put> {

  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(IdentityTableReducer.class);
  
  /**
   * Writes each given record, consisting of the key and the given values, to
   * the HBase table.
   * 
   * @param key  The current row key.
   * @param values  The values for the given row.
   * @param context  The context of the reduce. 
   * @throws IOException When writing the record fails.
   * @throws InterruptedException When the job gets interrupted.
   */
  public void reduce(ImmutableBytesWritable key, Iterator<Put> values,
      Context context) throws IOException, InterruptedException {
    while(values.hasNext()) {
      context.write(key, values.next());
    }
  }
  
}
