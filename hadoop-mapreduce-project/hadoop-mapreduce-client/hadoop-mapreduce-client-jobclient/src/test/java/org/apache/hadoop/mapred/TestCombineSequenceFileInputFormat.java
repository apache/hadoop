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

package org.apache.hadoop.mapred;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.BitSet;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapred.lib.CombineSequenceFileInputFormat;
import org.junit.Test;

public class TestCombineSequenceFileInputFormat {
  private static final Log LOG =
    LogFactory.getLog(TestCombineSequenceFileInputFormat.class);

  private static Configuration conf = new Configuration();
  private static FileSystem localFs = null;

  static {
    try {
      conf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(conf);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }

  @SuppressWarnings("deprecation")
  private static Path workDir =
    new Path(new Path(System.getProperty("test.build.data", "/tmp")),
             "TestCombineSequenceFileInputFormat").makeQualified(localFs);

  @Test(timeout=10000)
  public void testFormat() throws Exception {
    JobConf job = new JobConf(conf);

    Reporter reporter = Reporter.NULL;

    Random random = new Random();
    long seed = random.nextLong();
    LOG.info("seed = "+seed);
    random.setSeed(seed);

    localFs.delete(workDir, true);

    FileInputFormat.setInputPaths(job, workDir);

    final int length = 10000;
    final int numFiles = 10;

    // create a file with various lengths
    createFiles(length, numFiles, random);

    // create a combine split for the files
    InputFormat<IntWritable, BytesWritable> format =
      new CombineSequenceFileInputFormat<IntWritable, BytesWritable>();
    IntWritable key = new IntWritable();
    BytesWritable value = new BytesWritable();
    for (int i = 0; i < 3; i++) {
      int numSplits =
        random.nextInt(length/(SequenceFile.SYNC_INTERVAL/20))+1;
      LOG.info("splitting: requesting = " + numSplits);
      InputSplit[] splits = format.getSplits(job, numSplits);
      LOG.info("splitting: got =        " + splits.length);

      // we should have a single split as the length is comfortably smaller than
      // the block size
      assertEquals("We got more than one splits!", 1, splits.length);
      InputSplit split = splits[0];
      assertEquals("It should be CombineFileSplit",
        CombineFileSplit.class, split.getClass());

      // check each split
      BitSet bits = new BitSet(length);
      RecordReader<IntWritable, BytesWritable> reader =
        format.getRecordReader(split, job, reporter);
      try {
        while (reader.next(key, value)) {
          assertFalse("Key in multiple partitions.", bits.get(key.get()));
          bits.set(key.get());
        }
      } finally {
        reader.close();
      }
      assertEquals("Some keys in no partition.", length, bits.cardinality());
    }
  }

  private static class Range {
    private final int start;
    private final int end;

    Range(int start, int end) {
      this.start = start;
      this.end = end;
    }

    @Override
    public String toString() {
      return "(" + start + ", " + end + ")";
    }
  }

  private static Range[] createRanges(int length, int numFiles, Random random) {
    // generate a number of files with various lengths
    Range[] ranges = new Range[numFiles];
    for (int i = 0; i < numFiles; i++) {
      int start = i == 0 ? 0 : ranges[i-1].end;
      int end = i == numFiles - 1 ?
        length :
        (length/numFiles)*(2*i + 1)/2 + random.nextInt(length/numFiles) + 1;
      ranges[i] = new Range(start, end);
    }
    return ranges;
  }

  private static void createFiles(int length, int numFiles, Random random)
    throws IOException {
    Range[] ranges = createRanges(length, numFiles, random);

    for (int i = 0; i < numFiles; i++) {
      Path file = new Path(workDir, "test_" + i + ".seq");
      // create a file with length entries
      @SuppressWarnings("deprecation")
      SequenceFile.Writer writer =
        SequenceFile.createWriter(localFs, conf, file,
                                  IntWritable.class, BytesWritable.class);
      Range range = ranges[i];
      try {
        for (int j = range.start; j < range.end; j++) {
          IntWritable key = new IntWritable(j);
          byte[] data = new byte[random.nextInt(10)];
          random.nextBytes(data);
          BytesWritable value = new BytesWritable(data);
          writer.append(key, value);
        }
      } finally {
        writer.close();
      }
    }
  }
}
