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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.OutputFormat;

/**
 * Convenience class that simply writes all values (which must be
 * {@link org.apache.hadoop.hbase.client.Put Put} or
 * {@link org.apache.hadoop.hbase.client.Delete Delete} instances)
 * passed to it out to the configured HBase table. This works in combination
 * with {@link TableOutputFormat} which actually does the writing to HBase.<p>
 *
 * Keys are passed along but ignored in TableOutputFormat.  However, they can
 * be used to control how your values will be divided up amongst the specified
 * number of reducers. <p>
 *
 * You can also use the {@link TableMapReduceUtil} class to set up the two
 * classes in one step:
 * <blockquote><code>
 * TableMapReduceUtil.initTableReducerJob("table", IdentityTableReducer.class, job);
 * </code></blockquote>
 * This will also set the proper {@link TableOutputFormat} which is given the
 * <code>table</code> parameter. The
 * {@link org.apache.hadoop.hbase.client.Put Put} or
 * {@link org.apache.hadoop.hbase.client.Delete Delete} define the
 * row and columns implicitly.
 */
public class IdentityTableReducer
extends TableReducer<Writable, Writable, Writable> {

  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(IdentityTableReducer.class);

  /**
   * Writes each given record, consisting of the row key and the given values,
   * to the configured {@link OutputFormat}. It is emitting the row key and each
   * {@link org.apache.hadoop.hbase.client.Put Put} or
   * {@link org.apache.hadoop.hbase.client.Delete Delete} as separate pairs.
   *
   * @param key  The current row key.
   * @param values  The {@link org.apache.hadoop.hbase.client.Put Put} or
   *   {@link org.apache.hadoop.hbase.client.Delete Delete} list for the given
   *   row.
   * @param context  The context of the reduce.
   * @throws IOException When writing the record fails.
   * @throws InterruptedException When the job gets interrupted.
   */
  @Override
  public void reduce(Writable key, Iterable<Writable> values, Context context)
  throws IOException, InterruptedException {
    for(Writable putOrDelete : values) {
      context.write(key, putOrDelete);
    }
  }
}