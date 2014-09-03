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
package org.apache.hadoop.mapred.nativetask.kvtest;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class HashSumReducer<KTYPE, VTYPE> extends Reducer<KTYPE, VTYPE, KTYPE, IntWritable> {

  ByteArrayOutputStream os = new ByteArrayOutputStream();
  DataOutputStream dos = new DataOutputStream(os);

  @Override
  public void reduce(KTYPE key, Iterable<VTYPE> values, Context context)
    throws IOException, InterruptedException {
    int hashSum = 0;
    for (final VTYPE val : values) {
      if (val instanceof Writable) {
        os.reset();
        ((Writable) val).write(dos);
        final int hash = Arrays.hashCode(os.toByteArray());
        hashSum += hash;
      }
    }

    context.write(key, new IntWritable(hashSum));
  }
}
