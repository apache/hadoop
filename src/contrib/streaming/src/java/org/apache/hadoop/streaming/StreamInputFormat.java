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

package org.apache.hadoop.streaming;

import java.io.*;
import java.lang.reflect.*;
import java.util.ArrayList;

import org.apache.commons.logging.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.mapred.*;

/** An input format that performs globbing on DFS paths and
 * selects a RecordReader based on a JobConf property.
 * @author Michel Tourn
 */
public class StreamInputFormat extends InputFormatBase {

  // an InputFormat should be public with the synthetic public default constructor
  // JobTracker's JobInProgress will instantiate with clazz.newInstance() (and a custom ClassLoader)

  protected static final Log LOG = LogFactory.getLog(StreamInputFormat.class.getName());

  static boolean isGzippedInput(JobConf job) {
    String val = job.get(StreamBaseRecordReader.CONF_NS + "compression");
    return "gzip".equals(val);
  }

  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

    if (isGzippedInput(job)) {
      return getFullFileSplits(job);
    } else {
      return super.getSplits(job, numSplits);
    }
  }

  /** For the compressed-files case: override InputFormatBase to produce one split. */
  FileSplit[] getFullFileSplits(JobConf job) throws IOException {
    Path[] files = listPaths(job);
    int numSplits = files.length;
    ArrayList splits = new ArrayList(numSplits);
    for (int i = 0; i < files.length; i++) {
      Path file = files[i];
      long splitSize = file.getFileSystem(job).getLength(file);
      splits.add(new FileSplit(file, 0, splitSize, job));
    }
    return (FileSplit[]) splits.toArray(new FileSplit[splits.size()]);
  }

  public RecordReader getRecordReader(final InputSplit genericSplit, 
                                      JobConf job,
                                      Reporter reporter) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    FileSystem fs = split.getPath().getFileSystem(job);
    LOG.info("getRecordReader start.....split=" + split);
    reporter.setStatus(split.toString());

    final long start = split.getStart();
    final long end = start + split.getLength();

    String splitName = split.getPath() + ":" + start + "-" + end;
    final FSDataInputStream in = fs.open(split.getPath());

    // will open the file and seek to the start of the split
    // Factory dispatch based on available params..
    Class readerClass;
    String c = job.get("stream.recordreader.class");
    if (c == null) {
      readerClass = StreamLineRecordReader.class;
    } else {
      readerClass = StreamUtil.goodClassOrNull(c, null);
      if (readerClass == null) {
        throw new RuntimeException("Class not found: " + c);
      }
    }

    Constructor ctor;
    try {
      ctor = readerClass.getConstructor(new Class[] { FSDataInputStream.class, FileSplit.class,
          Reporter.class, JobConf.class, FileSystem.class });
    } catch (NoSuchMethodException nsm) {
      throw new RuntimeException(nsm);
    }

    StreamBaseRecordReader reader;
    try {
      reader = (StreamBaseRecordReader) ctor.newInstance(new Object[] { in, split, reporter, job,
          fs });
    } catch (Exception nsm) {
      throw new RuntimeException(nsm);
    }

    reader.init();

    if (reader instanceof StreamSequenceRecordReader) {
      // override k/v class types with types stored in SequenceFile
      StreamSequenceRecordReader ss = (StreamSequenceRecordReader) reader;
      job.setInputKeyClass(ss.rin_.getKeyClass());
      job.setInputValueClass(ss.rin_.getValueClass());
    }

    return reader;
  }

}
