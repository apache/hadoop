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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class VerticaStreamingRecordReader extends RecordReader<Text, Text> {
  ResultSet results = null;
  VerticaRecord internalRecord = null;
  long start = 0;
  int pos = 0;
  long length = 0;
  VerticaInputSplit split = null;
  String delimiter = VerticaConfiguration.DELIMITER;
  String terminator = VerticaConfiguration.RECORD_TERMINATER;
  Text key = new Text();
  Text value = new Text();

  public VerticaStreamingRecordReader(VerticaInputSplit split,
      Configuration conf) throws Exception {
    // run query for this segment
    this.split = split;
    split.configure(conf);
    start = split.getStart();
    length = split.getLength();
    results = split.executeQuery();
    internalRecord = new VerticaRecord(results, false);

    VerticaConfiguration vtconfig = new VerticaConfiguration(conf);
    delimiter = vtconfig.getInputDelimiter();
    terminator = vtconfig.getInputRecordTerminator();
  }

  /** {@inheritDoc} */
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    // nothing
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
  public Text getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  /** {@inheritDoc} */
  public Text getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  /** {@inheritDoc} */
  public boolean nextKeyValue() throws IOException {
    key.set(new Long(pos + start).toString());
    pos++;
    try {
      if (internalRecord.next()) {
        value.set(internalRecord.toSQLString(delimiter, terminator));
        return true;
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return false;
  }

}
