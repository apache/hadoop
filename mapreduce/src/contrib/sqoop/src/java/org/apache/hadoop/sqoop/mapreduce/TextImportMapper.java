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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * Converts an input record into a string representation and emit it.
 */
public class TextImportMapper
    extends AutoProgressMapper<LongWritable, DBWritable, Text, NullWritable> {

  private Text outkey;

  public TextImportMapper() {
    outkey = new Text();
  }

  public void map(LongWritable key, DBWritable val, Context context)
      throws IOException, InterruptedException {
    outkey.set(val.toString());
    context.write(outkey, NullWritable.get());
  }
}
