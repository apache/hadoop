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

import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Emits sorted Puts.
 * Reads in all Puts from passed Iterator, sorts them, then emits
 * Puts in sorted order.  If lots of columns per row, it will use lots of
 * memory sorting.
 * @see HFileOutputFormat
 * @see KeyValueSortReducer
 */
public class PutSortReducer extends
    Reducer<ImmutableBytesWritable, Put, ImmutableBytesWritable, KeyValue> {
  
  @Override
  protected void reduce(
      ImmutableBytesWritable row,
      java.lang.Iterable<Put> puts,
      Reducer<ImmutableBytesWritable, Put,
              ImmutableBytesWritable, KeyValue>.Context context)
      throws java.io.IOException, InterruptedException
  {
    TreeSet<KeyValue> map = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
  
    for (Put p : puts) {
      for (List<KeyValue> kvs : p.getFamilyMap().values()) {
        for (KeyValue kv : kvs) {
          map.add(kv.clone());
        }
      }
    }
    context.setStatus("Read " + map.getClass());
    int index = 0;
    for (KeyValue kv : map) {
      context.write(row, kv);
      if (index > 0 && index % 100 == 0)
        context.setStatus("Wrote " + index);
    }
  }
}
