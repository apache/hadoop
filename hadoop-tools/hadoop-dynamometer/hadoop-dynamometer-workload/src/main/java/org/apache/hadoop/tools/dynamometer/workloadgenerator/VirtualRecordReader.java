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
package org.apache.hadoop.tools.dynamometer.workloadgenerator;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


/**
 * A simple fake record reader which simply runs for some time duration.
 */
@SuppressWarnings("unchecked")
public class VirtualRecordReader<K, V> extends RecordReader<K, V> {
  private int durationMs;
  private long startTimestampInMs;
  private long endTimestampInMs;
  private int numRows = 1;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    durationMs = conf.getInt(CreateFileMapper.DURATION_MIN_KEY, 0) * 60 * 1000;
    startTimestampInMs = conf.getInt(WorkloadDriver.START_TIMESTAMP_MS, 0);
    endTimestampInMs = startTimestampInMs + durationMs;
  }

  // The map function per split should be invoked only once.
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (numRows > 0) {
      numRows--;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return (K) NullWritable.get();
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return (V) NullWritable.get();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    long remainingMs = endTimestampInMs - System.currentTimeMillis();
    return (remainingMs * 100.0f) / durationMs;
  }

  @Override
  public void close() throws IOException {
    // do Nothing
  }
};
