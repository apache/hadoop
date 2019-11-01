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
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


/**
 * An input format which does not read any input, but rather starts a
 * configurable number of mappers and runs them for a configurable duration.
 */
public class VirtualInputFormat<K, V> extends FileInputFormat<K, V> {
  // Number of splits = Number of mappers. Creates fakeSplits to launch
  // the required number of mappers
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    Configuration conf = job.getConfiguration();
    int numMappers = conf.getInt(CreateFileMapper.NUM_MAPPERS_KEY, -1);
    if (numMappers == -1) {
      throw new IOException("Number of mappers should be provided as input");
    }
    List<InputSplit> splits = new ArrayList<InputSplit>(numMappers);
    for (int i = 0; i < numMappers; i++) {
      splits.add(new VirtualInputSplit());
    }
    return splits;
  }

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new VirtualRecordReader<>();
  }
}
