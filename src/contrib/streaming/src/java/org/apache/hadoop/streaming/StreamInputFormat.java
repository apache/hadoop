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
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.mapred.*;

/** An input format that performs globbing on DFS paths and
 * selects a RecordReader based on a JobConf property.
 * @author Michel Tourn
 */
public class StreamInputFormat extends InputFormatBase {

  // an InputFormat should be public with the synthetic public default constructor
  // JobTracker's JobInProgress will instantiate with clazz.newInstance() (and a custom ClassLoader)

  protected static final Log LOG = LogFactory.getLog(StreamInputFormat.class.getName());

  /** This implementation always returns true. */
  public boolean[] areValidInputDirectories(FileSystem fileSys, Path[] inputDirs) throws IOException {
    boolean[] b = new boolean[inputDirs.length];
    for (int i = 0; i < inputDirs.length; ++i) {
      b[i] = true;
    }
    return b;
  }

  static boolean isGzippedInput(JobConf job) {
    String val = job.get(StreamBaseRecordReader.CONF_NS + "compression");
    return "gzip".equals(val);
  }

  public FileSplit[] getSplits(FileSystem fs, JobConf job, int numSplits) throws IOException {

    if (isGzippedInput(job)) {
      return getFullFileSplits(fs, job);
    } else {
      return super.getSplits(fs, job, numSplits);
    }
  }

  /** For the compressed-files case: override InputFormatBase to produce one split. */
  FileSplit[] getFullFileSplits(FileSystem fs, JobConf job) throws IOException {
    Path[] files = listPaths(fs, job);
    int numSplits = files.length;
    ArrayList splits = new ArrayList(numSplits);
    for (int i = 0; i < files.length; i++) {
      Path file = files[i];
      long splitSize = fs.getLength(file);
      splits.add(new FileSplit(file, 0, splitSize));
    }
    return (FileSplit[]) splits.toArray(new FileSplit[splits.size()]);
  }

  protected Path[] listPaths(FileSystem fs, JobConf job) throws IOException {
    Path[] globs = job.getInputPaths();
    ArrayList list = new ArrayList();
    int dsup = globs.length;
    for (int d = 0; d < dsup; d++) {
      String leafName = globs[d].getName();
      LOG.info("StreamInputFormat: globs[" + d + "] leafName = " + leafName);
      Path[] paths;
      Path dir;
      PathFilter filter = new GlobFilter(fs, leafName);
      dir = new Path(globs[d].getParent().toString());
      if (dir == null) dir = new Path(".");
      paths = fs.listPaths(dir, filter);
      list.addAll(Arrays.asList(paths));
    }
    return (Path[]) list.toArray(new Path[] {});
  }

  class GlobFilter implements PathFilter {

    public GlobFilter(FileSystem fs, String glob) {
      fs_ = fs;
      pat_ = Pattern.compile(globToRegexp(glob));
    }

    String globToRegexp(String glob) {
      String re = glob;
      re = re.replaceAll("\\.", "\\\\.");
      re = re.replaceAll("\\+", "\\\\+");
      re = re.replaceAll("\\*", ".*");
      re = re.replaceAll("\\?", ".");
      LOG.info("globToRegexp: |" + glob + "|  ->  |" + re + "|");
      return re;
    }

    public boolean accept(Path pathname) {
      boolean acc = !fs_.isChecksumFile(pathname);
      if (acc) {
        acc = pat_.matcher(pathname.getName()).matches();
      }
      LOG.info("matches " + pat_ + ", " + pathname + " = " + acc);
      return acc;
    }

    Pattern pat_;
    FileSystem fs_;
  }

  public RecordReader getRecordReader(FileSystem fs, final FileSplit split, JobConf job,
      Reporter reporter) throws IOException {
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
