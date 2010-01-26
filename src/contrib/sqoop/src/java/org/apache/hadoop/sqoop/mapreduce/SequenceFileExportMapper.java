/**
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

package org.apache.hadoop.sqoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;

import org.apache.hadoop.sqoop.lib.SqoopRecord;

/**
 * Reads a SqoopRecord from the SequenceFile in which it's packed and emits
 * that DBWritable to the DBOutputFormat for writeback to the database.
 */
public class SequenceFileExportMapper
    extends AutoProgressMapper<LongWritable, SqoopRecord, SqoopRecord, NullWritable> {

  public SequenceFileExportMapper() {
  }

  public void map(LongWritable key, SqoopRecord val, Context context)
      throws IOException, InterruptedException {
    context.write(val, NullWritable.get());
  }
}
