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
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class VerticaRecordReader extends
    RecordReader<LongWritable, VerticaRecord> {
  ResultSet results = null;
  long start = 0;
  int pos = 0;
  long length = 0;
  VerticaInputSplit split = null;
  LongWritable key = null;
  VerticaRecord value = null;

  public VerticaRecordReader(VerticaInputSplit split, Configuration job)
      throws Exception {
    // run query for this segment
    this.split = split;
    split.configure(job);
    start = split.getStart();
    length = split.getLength();
    results = split.executeQuery();
  }

  /** {@inheritDoc} */
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    key = new LongWritable();
    try {
      pos++;
      value = new VerticaRecord(results, false);
    } catch (SQLException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
  }

  /** {@inheritDoc} */
  public void close() throws IOException {
    try {
      split.close();
    } catch (SQLException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
  }

  /** {@inheritDoc} */
  public long getPos() throws IOException {
    return pos;
  }

  /** {@inheritDoc} */
  public float getProgress() throws IOException {
    // TODO: figure out why length would be 0
    if (length == 0)
      return 1;
    return pos / length;
  }

  /** {@inheritDoc} */
  public boolean next(LongWritable key, VerticaRecord value) throws IOException {
    key.set(pos + start);
    pos++;
    try {
      return value.next();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public VerticaRecord getCurrentValue() throws IOException,
      InterruptedException {
    return value;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    key.set(pos + start);
    pos++;
    try {
      return value.next();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

}
