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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.BitSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMRKeyValueTextInputFormat {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMRKeyValueTextInputFormat.class);

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
             "TestKeyValueTextInputFormat");
  
  @Test
  public void testFormat() throws Exception {
    Job job = Job.getInstance(new Configuration(defaultConf));
    Path file = new Path(workDir, "test.txt");

    int seed = new Random().nextInt();
    LOG.info("seed = " + seed);
    Random random = new Random(seed);

    localFs.delete(workDir, true);
    FileInputFormat.setInputPaths(job, workDir);

    final int MAX_LENGTH = 10000;
    // for a variety of lengths
    for (int length = 0; length < MAX_LENGTH;
         length += random.nextInt(MAX_LENGTH / 10) + 1) {

      LOG.debug("creating; entries = " + length);

      // create a file with length entries
      Writer writer = new OutputStreamWriter(localFs.create(file));
      try {
        for (int i = 0; i < length; i++) {
          writer.write(Integer.toString(i * 2));
          writer.write("\t");
          writer.write(Integer.toString(i));
          writer.write("\n");
        }
      } finally {
        writer.close();
      }

      // try splitting the file in a variety of sizes
      KeyValueTextInputFormat format = new KeyValueTextInputFormat();
      for (int i = 0; i < 3; i++) {
        int numSplits = random.nextInt(MAX_LENGTH / 20) + 1;
        LOG.debug("splitting: requesting = " + numSplits);
        List<InputSplit> splits = format.getSplits(job);
        LOG.debug("splitting: got =        " + splits.size());

        // check each split
        BitSet bits = new BitSet(length);
        for (int j = 0; j < splits.size(); j++) {
          LOG.debug("split["+j+"]= " + splits.get(j));
          TaskAttemptContext context = MapReduceTestUtil.
            createDummyMapTaskAttemptContext(job.getConfiguration());
          RecordReader<Text, Text> reader = format.createRecordReader(
            splits.get(j), context);
          Class<?> clazz = reader.getClass();
          assertEquals(KeyValueLineRecordReader.class, clazz,
              "reader class is KeyValueLineRecordReader.");
          MapContext<Text, Text, Text, Text> mcontext = 
            new MapContextImpl<Text, Text, Text, Text>(job.getConfiguration(), 
            context.getTaskAttemptID(), reader, null, null, 
            MapReduceTestUtil.createDummyReporter(), splits.get(j));
          reader.initialize(splits.get(j), mcontext);

          Text key = null;
          Text value = null;
          try {
            int count = 0;
            while (reader.nextKeyValue()) {
              key = reader.getCurrentKey();
              clazz = key.getClass();
              assertEquals(Text.class, clazz, "Key class is Text.");
              value = reader.getCurrentValue();
              clazz = value.getClass();
              assertEquals(Text.class, clazz, "Value class is Text.");
              final int k = Integer.parseInt(key.toString());
              final int v = Integer.parseInt(value.toString());
              assertEquals(0, k % 2, "Bad key");
              assertEquals(k / 2, v, "Mismatched key/value");
              LOG.debug("read " + v);
              assertFalse(bits.get(v), "Key in multiple partitions.");
              bits.set(v);
              count++;
            }
            LOG.debug("splits[" + j + "]=" + splits.get(j) +" count=" + count);
          } finally {
            reader.close();
          }
        }
        assertEquals(length, bits.cardinality(), "Some keys in no partition.");
      }

    }
  }

  @Test
  public void testSplitableCodecs() throws Exception {
    final Job job = Job.getInstance(defaultConf);
    final Configuration conf = job.getConfiguration();

    // Create the codec
    CompressionCodec codec = null;
    try {
      codec = (CompressionCodec)
      ReflectionUtils.newInstance(conf.getClassByName("org.apache.hadoop.io.compress.BZip2Codec"), conf);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Illegal codec!");
    }
    Path file = new Path(workDir, "test"+codec.getDefaultExtension());

    int seed = new Random().nextInt();
    LOG.info("seed = " + seed);
    Random random = new Random(seed);

    localFs.delete(workDir, true);
    FileInputFormat.setInputPaths(job, workDir);

    final int MAX_LENGTH = 500000;
    FileInputFormat.setMaxInputSplitSize(job, MAX_LENGTH / 20);
    // for a variety of lengths
    for (int length = 0; length < MAX_LENGTH;
         length += random.nextInt(MAX_LENGTH / 4) + 1) {

      LOG.info("creating; entries = " + length);

      // create a file with length entries
      Writer writer =
        new OutputStreamWriter(codec.createOutputStream(localFs.create(file)));
      try {
        for (int i = 0; i < length; i++) {
          writer.write(Integer.toString(i * 2));
          writer.write("\t");
          writer.write(Integer.toString(i));
          writer.write("\n");
        }
      } finally {
        writer.close();
      }

      // try splitting the file in a variety of sizes
      KeyValueTextInputFormat format = new KeyValueTextInputFormat();
      assertTrue(format.isSplitable(job, file), "KVTIF claims not splittable");
      for (int i = 0; i < 3; i++) {
        int numSplits = random.nextInt(MAX_LENGTH / 2000) + 1;
        LOG.info("splitting: requesting = " + numSplits);
        List<InputSplit> splits = format.getSplits(job);
        LOG.info("splitting: got =        " + splits.size());

        // check each split
        BitSet bits = new BitSet(length);
        for (int j = 0; j < splits.size(); j++) {
          LOG.debug("split["+j+"]= " + splits.get(j));
          TaskAttemptContext context = MapReduceTestUtil.
            createDummyMapTaskAttemptContext(job.getConfiguration());
          RecordReader<Text, Text> reader = format.createRecordReader(
            splits.get(j), context);
          Class<?> clazz = reader.getClass();
          MapContext<Text, Text, Text, Text> mcontext =
            new MapContextImpl<Text, Text, Text, Text>(job.getConfiguration(),
            context.getTaskAttemptID(), reader, null, null,
            MapReduceTestUtil.createDummyReporter(), splits.get(j));
          reader.initialize(splits.get(j), mcontext);

          Text key = null;
          Text value = null;
          try {
            int count = 0;
            while (reader.nextKeyValue()) {
              key = reader.getCurrentKey();
              value = reader.getCurrentValue();
              final int k = Integer.parseInt(key.toString());
              final int v = Integer.parseInt(value.toString());
              assertEquals(0, k % 2, "Bad key");
              assertEquals(k / 2, v, "Mismatched key/value");
              LOG.debug("read " + k + "," + v);
              assertFalse(bits.get(v), k + "," + v + " in multiple partitions.");
              bits.set(v);
              count++;
            }
            if (count > 0) {
              LOG.info("splits["+j+"]="+splits.get(j)+" count=" + count);
            } else {
              LOG.debug("splits["+j+"]="+splits.get(j)+" count=" + count);
            }
          } finally {
            reader.close();
          }
        }
        assertEquals(length, bits.cardinality(), "Some keys in no partition.");
      }

    }
  }

  private LineReader makeStream(String str) throws IOException {
    return new LineReader(new ByteArrayInputStream(str.getBytes(UTF_8)), defaultConf);
  }
  
  @Test
  public void testUTF8() throws Exception {
    LineReader in = makeStream("abcd\u20acbdcd\u20ac");
    Text line = new Text();
    in.readLine(line);
    assertEquals("abcd\u20acbdcd\u20ac", line.toString(),
        "readLine changed utf8 characters");
    in = makeStream("abc\u200axyz");
    in.readLine(line);
    assertEquals("abc\u200axyz", line.toString(),
        "split on fake newline");
  }

  @Test
  public void testNewLines() throws Exception {
    LineReader in = makeStream("a\nbb\n\nccc\rdddd\r\neeeee");
    Text out = new Text();
    in.readLine(out);
    assertEquals(1, out.getLength(), "line1 length");
    in.readLine(out);
    assertEquals(2, out.getLength(), "line2 length");
    in.readLine(out);
    assertEquals(0, out.getLength(), "line3 length");
    in.readLine(out);
    assertEquals(3, out.getLength(), "line4 length");
    in.readLine(out);
    assertEquals(4, out.getLength(), "line5 length");
    in.readLine(out);
    assertEquals(5, out.getLength(), "line5 length");
    assertEquals(0, in.readLine(out), "end of file");
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
  
  private static List<Text> readSplit(KeyValueTextInputFormat format, 
      InputSplit split, Job job) throws IOException, InterruptedException {
    List<Text> result = new ArrayList<Text>();
    Configuration conf = job.getConfiguration();
    TaskAttemptContext context = MapReduceTestUtil.
      createDummyMapTaskAttemptContext(conf);
    RecordReader<Text, Text> reader = format.createRecordReader(split, 
      MapReduceTestUtil.createDummyMapTaskAttemptContext(conf));
    MapContext<Text, Text, Text, Text> mcontext = 
      new MapContextImpl<Text, Text, Text, Text>(conf, 
      context.getTaskAttemptID(), reader, null, null,
      MapReduceTestUtil.createDummyReporter(), 
      split);
    reader.initialize(split, mcontext);
    while (reader.nextKeyValue()) {
      result.add(new Text(reader.getCurrentValue()));
    }
    reader.close();
    return result;
  }
  
  /**
   * Test using the gzip codec for reading
   */
  @Test
  public void testGzip() throws IOException, InterruptedException {
    Configuration conf = new Configuration(defaultConf);
    CompressionCodec gzip = new GzipCodec();
    ReflectionUtils.setConf(gzip, conf);
    localFs.delete(workDir, true);
    writeFile(localFs, new Path(workDir, "part1.txt.gz"), gzip, 
              "line-1\tthe quick\nline-2\tbrown\nline-3\t" +
              "fox jumped\nline-4\tover\nline-5\t the lazy\nline-6\t dog\n");
    writeFile(localFs, new Path(workDir, "part2.txt.gz"), gzip,
              "line-1\tthis is a test\nline-1\tof gzip\n");
    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, workDir);
    KeyValueTextInputFormat format = new KeyValueTextInputFormat();
    List<InputSplit> splits = format.getSplits(job);
    assertEquals(2, splits.size(), "compressed splits == 2");
    FileSplit tmp = (FileSplit) splits.get(0);
    if (tmp.getPath().getName().equals("part2.txt.gz")) {
      splits.set(0, splits.get(1));
      splits.set(1, tmp);
    }
    List<Text> results = readSplit(format, splits.get(0), job);
    assertEquals(6, results.size(), "splits[0] length");
    assertEquals("the quick", results.get(0).toString(), "splits[0][0]");
    assertEquals("brown", results.get(1).toString(), "splits[0][1]");
    assertEquals("fox jumped", results.get(2).toString(), "splits[0][2]");
    assertEquals("over", results.get(3).toString(), "splits[0][3]");
    assertEquals(" the lazy", results.get(4).toString(), "splits[0][4]");
    assertEquals(" dog", results.get(5).toString(), "splits[0][5]");
    results = readSplit(format, splits.get(1), job);
    assertEquals(2, results.size(), "splits[1] length");
    assertEquals("this is a test", results.get(0).toString(), "splits[1][0]");
    assertEquals("of gzip", results.get(1).toString(), "splits[1][1]");
  }
  
/*  public static void main(String[] args) throws Exception {
    new TestMRKeyValueTextInputFormat().testFormat();
  }*/
}
