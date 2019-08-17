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

package org.apache.hadoop.streaming.mapreduce;

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.impl.FutureIOSupport;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.streaming.StreamUtil;

/**
 * An input format that selects a RecordReader based on a JobConf property. This
 * should be used only for non-standard record reader such as
 * StreamXmlRecordReader. For all other standard record readers, the appropriate
 * input format classes should be used.
 */
public class StreamInputFormat extends KeyValueTextInputFormat {

  @Override
  public RecordReader<Text, Text> createRecordReader(InputSplit genericSplit,
      TaskAttemptContext context) throws IOException {

    Configuration conf = context.getConfiguration();

    String c = conf.get("stream.recordreader.class");
    if (c == null || c.indexOf("LineRecordReader") >= 0) {
      return super.createRecordReader(genericSplit, context);
    }

    // handling non-standard record reader (likely StreamXmlRecordReader)
    FileSplit split = (FileSplit) genericSplit;
    // LOG.info("getRecordReader start.....split=" + split);
    context.setStatus(split.toString());
    context.progress();

    // Open the file and seek to the start of the split
    Path path = split.getPath();
    FileSystem fs = path.getFileSystem(conf);
    // open the file
    final FutureDataInputStreamBuilder builder = fs.openFile(path);
    FutureIOSupport.propagateOptions(builder, conf,
        MRJobConfig.INPUT_FILE_OPTION_PREFIX,
        MRJobConfig.INPUT_FILE_MANDATORY_PREFIX);
    FSDataInputStream in = FutureIOSupport.awaitFuture(builder.build());

    // Factory dispatch based on available params..
    Class readerClass;

    {
      readerClass = StreamUtil.goodClassOrNull(conf, c, null);
      if (readerClass == null) {
        throw new RuntimeException("Class not found: " + c);
      }
    }
    Constructor ctor;

    try {
      ctor = readerClass.getConstructor(new Class[] { FSDataInputStream.class,
          FileSplit.class, TaskAttemptContext.class, Configuration.class,
          FileSystem.class });
    } catch (NoSuchMethodException nsm) {
      throw new RuntimeException(nsm);
    }

    RecordReader<Text, Text> reader;
    try {
      reader = (RecordReader<Text, Text>) ctor.newInstance(new Object[] { in,
          split, context, conf, fs });
    } catch (Exception nsm) {
      throw new RuntimeException(nsm);
    }
    return reader;

  }

}
