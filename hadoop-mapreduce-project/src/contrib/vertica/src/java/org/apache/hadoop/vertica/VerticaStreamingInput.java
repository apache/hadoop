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

package org.apache.hadoop.vertica;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class VerticaStreamingInput extends InputFormat<Text, Text> {

  public RecordReader<Text, Text> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    try {
      return new VerticaStreamingRecordReader((VerticaInputSplit) split,
          context.getConfiguration());
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    return VerticaUtil.getSplits(context);
  }
}
