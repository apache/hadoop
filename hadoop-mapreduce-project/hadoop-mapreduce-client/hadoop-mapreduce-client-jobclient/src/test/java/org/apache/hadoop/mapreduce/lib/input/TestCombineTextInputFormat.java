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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

public class TestCombineTextInputFormat {
  private static final Log LOG =
    LogFactory.getLog(TestCombineTextInputFormat.class);

  private static Configuration defaultConf = new Configuration();
  private static FileSystem localFs = null;

  static {
    try {
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }

  private static Path workDir =
    new Path(new Path(System.getProperty("test.build.data", "."), "data"),
             "TestCombineTextInputFormat");

  @Test(timeout=10000)
  public void testFormat() throws Exception {
    Job job = Job.getInstance(new Configuration(defaultConf));

    Random random = new Random();
    long seed = random.nextLong();
    LOG.info("seed = " + seed);
    random.setSeed(seed);

    localFs.delete(workDir, true);
    FileInputFormat.setInputPaths(job, workDir);

    final int length = 10000;
    final int numFiles = 10;

    // create files with various lengths
    createFiles(length, numFiles, random);

    // create a combined split for the files
    CombineTextInputFormat format = new CombineTextInputFormat();
    for (int i = 0; i < 3; i++) {
      int numSplits = random.nextInt(length/20) + 1;
      LOG.info("splitting: requesting = " + numSplits);
      List<InputSplit> splits = format.getSplits(job);
      LOG.info("splitting: got =        " + splits.size());

      // we should have a single split as the length is comfortably smaller than
      // the block size
      assertEquals("We got more than one splits!", 1, splits.size());
      InputSplit split = splits.get(0);
      assertEquals("It should be CombineFileSplit",
        CombineFileSplit.class, split.getClass());

      // check the split
      BitSet bits = new BitSet(length);
      LOG.debug("split= " + split);
      TaskAttemptContext context = MapReduceTestUtil.
        createDummyMapTaskAttemptContext(job.getConfiguration());
      RecordReader<LongWritable, Text> reader =
        format.createRecordReader(split, context);
      assertEquals("reader class is CombineFileRecordReader.",
        CombineFileRecordReader.class, reader.getClass());
      MapContext<LongWritable,Text,LongWritable,Text> mcontext =
        new MapContextImpl<LongWritable,Text,LongWritable,Text>(job.getConfiguration(),
        context.getTaskAttemptID(), reader, null, null,
        MapReduceTestUtil.createDummyReporter(), split);
      reader.initialize(split, mcontext);

      try {
        int count = 0;
        while (reader.nextKeyValue()) {
          LongWritable key = reader.getCurrentKey();
          assertNotNull("Key should not be null.", key);
          Text value = reader.getCurrentValue();
          final int v = Integer.parseInt(value.toString());
          LOG.debug("read " + v);
          assertFalse("Key in multiple partitions.", bits.get(v));
          bits.set(v);
          count++;
        }
        LOG.debug("split=" + split + " count=" + count);
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
      Path file = new Path(workDir, "test_" + i + ".txt");
      Writer writer = new OutputStreamWriter(localFs.create(file));
      Range range = ranges[i];
      try {
        for (int j = range.start; j < range.end; j++) {
          writer.write(Integer.toString(j));
          writer.write("\n");
        }
      } finally {
        writer.close();
      }
    }
  }

  private static void writeFile(FileSystem fs, Path name,
                                CompressionCodec codec,
                                String contents) throws IOException {
    OutputStream stm;
    if (codec == null) {
      stm = fs.create(name);
    } else {
      stm = codec.createOutputStream(fs.create(name));
    }
    stm.write(contents.getBytes());
    stm.close();
  }

  private static List<Text> readSplit(InputFormat<LongWritable,Text> format,
    InputSplit split, Job job) throws IOException, InterruptedException {
    List<Text> result = new ArrayList<Text>();
    Configuration conf = job.getConfiguration();
    TaskAttemptContext context = MapReduceTestUtil.
      createDummyMapTaskAttemptContext(conf);
    RecordReader<LongWritable, Text> reader = format.createRecordReader(split,
      MapReduceTestUtil.createDummyMapTaskAttemptContext(conf));
    MapContext<LongWritable,Text,LongWritable,Text> mcontext =
      new MapContextImpl<LongWritable,Text,LongWritable,Text>(conf,
      context.getTaskAttemptID(), reader, null, null,
      MapReduceTestUtil.createDummyReporter(),
      split);
    reader.initialize(split, mcontext);
    while (reader.nextKeyValue()) {
      result.add(new Text(reader.getCurrentValue()));
    }
    return result;
  }

  /**
   * Test using the gzip codec for reading
   */
  @Test(timeout=10000)
  public void testGzip() throws IOException, InterruptedException {
    Configuration conf = new Configuration(defaultConf);
    CompressionCodec gzip = new GzipCodec();
    ReflectionUtils.setConf(gzip, conf);
    localFs.delete(workDir, true);
    writeFile(localFs, new Path(workDir, "part1.txt.gz"), gzip,
              "the quick\nbrown\nfox jumped\nover\n the lazy\n dog\n");
    writeFile(localFs, new Path(workDir, "part2.txt.gz"), gzip,
              "this is a test\nof gzip\n");
    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, workDir);
    CombineTextInputFormat format = new CombineTextInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    assertEquals("compressed splits == 1", 1, splits.size());
    List<Text> results = readSplit(format, splits.get(0), job);
    assertEquals("splits[0] length", 8, results.size());

    final String[] firstList =
      {"the quick", "brown", "fox jumped", "over", " the lazy", " dog"};
    final String[] secondList = {"this is a test", "of gzip"};
    String first = results.get(0).toString();
    if (first.equals(firstList[0])) {
      testResults(results, firstList, secondList);
    } else if (first.equals(secondList[0])) {
      testResults(results, secondList, firstList);
    } else {
      fail("unexpected first token!");
    }
  }

  private static void testResults(List<Text> results, String[] first,
    String[] second) {
    for (int i = 0; i < first.length; i++) {
      assertEquals("splits[0]["+i+"]", first[i], results.get(i).toString());
    }
    for (int i = 0; i < second.length; i++) {
      int j = i + first.length;
      assertEquals("splits[0]["+j+"]", second[i], results.get(j).toString());
    }
  }
}
