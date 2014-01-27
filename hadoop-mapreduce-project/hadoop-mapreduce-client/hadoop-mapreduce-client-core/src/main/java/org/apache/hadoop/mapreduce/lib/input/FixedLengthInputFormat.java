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
package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * FixedLengthInputFormat is an input format used to read input files
 * which contain fixed length records.  The content of a record need not be
 * text.  It can be arbitrary binary data.  Users must configure the record
 * length property by calling:
 * FixedLengthInputFormat.setRecordLength(conf, recordLength);<br><br> or
 * conf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, recordLength);
 * <br><br>
 * @see FixedLengthRecordReader
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FixedLengthInputFormat
    extends FileInputFormat<LongWritable, BytesWritable> {

  public static final String FIXED_RECORD_LENGTH =
      "fixedlengthinputformat.record.length"; 

  /**
   * Set the length of each record
   * @param conf configuration
   * @param recordLength the length of a record
   */
  public static void setRecordLength(Configuration conf, int recordLength) {
    conf.setInt(FIXED_RECORD_LENGTH, recordLength);
  }

  /**
   * Get record length value
   * @param conf configuration
   * @return the record length, zero means none was set
   */
  public static int getRecordLength(Configuration conf) {
    return conf.getInt(FIXED_RECORD_LENGTH, 0);
  }

  @Override
  public RecordReader<LongWritable, BytesWritable>
      createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    int recordLength = getRecordLength(context.getConfiguration());
    if (recordLength <= 0) {
      throw new IOException("Fixed record length " + recordLength
          + " is invalid.  It should be set to a value greater than zero");
    }
    return new FixedLengthRecordReader(recordLength);
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    final CompressionCodec codec = 
        new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    return (null == codec);
  } 

}
