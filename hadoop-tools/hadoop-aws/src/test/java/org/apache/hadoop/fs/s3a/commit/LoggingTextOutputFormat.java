/*
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

package org.apache.hadoop.fs.s3a.commit;

import java.io.DataOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A subclass of {@link TextOutputFormat} which logs what is happening, and
 * returns a {@link LoggingLineRecordWriter} which allows the caller
 * to get the destination path.
 * @param <K> key
 * @param <V> value
 */
public class LoggingTextOutputFormat<K, V> extends TextOutputFormat<K, V> {
  private static final Logger LOG =
      LoggerFactory.getLogger(LoggingTextOutputFormat.class);

  public static final String NAME
      = "org.apache.hadoop.fs.s3a.commit.LoggingTextOutputFormat";

  @Override
  public LoggingLineRecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    Configuration conf = job.getConfiguration();
    boolean isCompressed = getCompressOutput(job);
    String keyValueSeparator = conf.get(SEPARATOR, "\t");
    CompressionCodec codec = null;
    String extension = "";
    if (isCompressed) {
      Class<? extends CompressionCodec> codecClass =
          getOutputCompressorClass(job, GzipCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, conf);
      extension = codec.getDefaultExtension();
    }
    Path file = getDefaultWorkFile(job, extension);
    FileSystem fs = file.getFileSystem(conf);
    FSDataOutputStream fileOut = fs.create(file, false);
    LOG.debug("Creating LineRecordWriter with destination {}", file);
    if (isCompressed) {
      return new LoggingLineRecordWriter<>(
          file, new DataOutputStream(codec.createOutputStream(fileOut)),
          keyValueSeparator);
    } else {
      return new LoggingLineRecordWriter<>(file, fileOut, keyValueSeparator);
    }
  }

  /**
   * Write a line; counts the number of lines written and logs @ debug in the
   * {@code close()} call.
   * @param <K> key
   * @param <V> value
   */
  public static class LoggingLineRecordWriter<K, V>
      extends LineRecordWriter<K, V> {
    private final Path dest;
    private long lines;

    public LoggingLineRecordWriter(Path dest, DataOutputStream out,
        String keyValueSeparator) {
      super(out, keyValueSeparator);
      this.dest = dest;
    }

    public LoggingLineRecordWriter(DataOutputStream out, Path dest) {
      super(out);
      this.dest = dest;
    }

    @Override
    public synchronized void write(K key, V value) throws IOException {
      super.write(key, value);
      lines++;
    }

    public synchronized void close(TaskAttemptContext context)
        throws IOException {
      LOG.debug("Closing output file {} with {} lines :{}",
          dest, lines, out);
      out.close();
    }

    public Path getDest() {
      return dest;
    }

    public long getLines() {
      return lines;
    }
  }

  /**
   * Bind to a configuration for job submission.
   * @param conf configuration
   */
  public static void bind(Configuration conf) {
    conf.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR,
        LoggingTextOutputFormat.class,
        OutputFormat.class);
  }
}
