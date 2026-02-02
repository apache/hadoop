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

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestKeyValueTextInputFormat {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestKeyValueTextInputFormat.class);

  private static int MAX_LENGTH = 10000;
  
  private static JobConf defaultConf = new JobConf();
  private static FileSystem localFs = null; 
  static {
    try {
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
    JobConf job = new JobConf();
    Path file = new Path(workDir, "test.txt");

    // A reporter that does nothing
    Reporter reporter = Reporter.NULL;
    
    int seed = new Random().nextInt();
    LOG.info("seed = "+seed);
    Random random = new Random(seed);

    localFs.delete(workDir, true);
    FileInputFormat.setInputPaths(job, workDir);

    // for a variety of lengths
    for (int length = 0; length < MAX_LENGTH;
         length+= random.nextInt(MAX_LENGTH/10)+1) {

      LOG.debug("creating; entries = " + length);

      // create a file with length entries
      Writer writer = new OutputStreamWriter(localFs.create(file));
      try {
        for (int i = 0; i < length; i++) {
          writer.write(Integer.toString(i*2));
          writer.write("\t");
          writer.write(Integer.toString(i));
          writer.write("\n");
        }
      } finally {
        writer.close();
      }

      // try splitting the file in a variety of sizes
      KeyValueTextInputFormat format = new KeyValueTextInputFormat();
      format.configure(job);
      for (int i = 0; i < 3; i++) {
        int numSplits = random.nextInt(MAX_LENGTH/20)+1;
        LOG.debug("splitting: requesting = " + numSplits);
        InputSplit[] splits = format.getSplits(job, numSplits);
        LOG.debug("splitting: got =        " + splits.length);

        // check each split
        BitSet bits = new BitSet(length);
        for (int j = 0; j < splits.length; j++) {
          LOG.debug("split["+j+"]= " + splits[j]);
          RecordReader<Text, Text> reader =
            format.getRecordReader(splits[j], job, reporter);
          Class readerClass = reader.getClass();
          assertEquals(KeyValueLineRecordReader.class, readerClass,
              "reader class is KeyValueLineRecordReader.");

          Text key = reader.createKey();
          Class keyClass = key.getClass();
          Text value = reader.createValue();
          Class valueClass = value.getClass();
          assertEquals(Text.class, keyClass, "Key class is Text.");
          assertEquals(Text.class, valueClass, "Value class is Text.");
          try {
            int count = 0;
            while (reader.next(key, value)) {
              int v = Integer.parseInt(value.toString());
              LOG.debug("read " + v);
              if (bits.get(v)) {
                LOG.warn("conflict with " + v + 
                         " in split " + j +
                         " at position "+reader.getPos());
              }
              assertFalse(bits.get(v), "Key in multiple partitions.");
              bits.set(v);
              count++;
            }
            LOG.debug("splits["+j+"]="+splits[j]+" count=" + count);
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
    LineReader in = null;

    try {
      in = makeStream("abcd\u20acbdcd\u20ac");
      Text line = new Text();
      in.readLine(line);
      assertEquals("abcd\u20acbdcd\u20ac", line.toString(),
          "readLine changed utf8 characters");
      in = makeStream("abc\u200axyz");
      in.readLine(line);
      assertEquals("abc\u200axyz", line.toString(),
          "split on fake newline");
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }
  @Test
  public void testNewLines() throws Exception {
    LineReader in = null;
    try {
      in = makeStream("a\nbb\n\nccc\rdddd\r\neeeee");
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
    } finally {
      if (in != null) {
        in.close();
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
  
  private static final Reporter voidReporter = Reporter.NULL;
  
  private static List<Text> readSplit(KeyValueTextInputFormat format, 
                                      InputSplit split, 
                                      JobConf job) throws IOException {
    List<Text> result = new ArrayList<Text>();
    RecordReader<Text, Text> reader = null;

    try {
      reader = format.getRecordReader(split, job, voidReporter);
      Text key = reader.createKey();
      Text value = reader.createValue();
      while (reader.next(key, value)) {
        result.add(value);
        value = (Text) reader.createValue();
      }   
    } finally {
      if (reader != null) {
        reader.close();
      }   
    }   
    return result;
  }
  
  /**
   * Test using the gzip codec for reading
   */
  @Test
  public void testGzip() throws IOException {
    JobConf job = new JobConf();
    CompressionCodec gzip = new GzipCodec();
    ReflectionUtils.setConf(gzip, job);
    localFs.delete(workDir, true);
    writeFile(localFs, new Path(workDir, "part1.txt.gz"), gzip, 
              "line-1\tthe quick\nline-2\tbrown\nline-3\tfox jumped\nline-4\tover\nline-5\t the lazy\nline-6\t dog\n");
    writeFile(localFs, new Path(workDir, "part2.txt.gz"), gzip,
              "line-1\tthis is a test\nline-1\tof gzip\n");
    FileInputFormat.setInputPaths(job, workDir);
    KeyValueTextInputFormat format = new KeyValueTextInputFormat();
    format.configure(job);
    InputSplit[] splits = format.getSplits(job, 100);
    assertEquals(2, splits.length, "compressed splits == 2");
    FileSplit tmp = (FileSplit) splits[0];
    if (tmp.getPath().getName().equals("part2.txt.gz")) {
      splits[0] = splits[1];
      splits[1] = tmp;
    }
    List<Text> results = readSplit(format, splits[0], job);
    assertEquals(6, results.size(), "splits[0] length");
    assertEquals(" dog", results.get(5).toString(), "splits[0][5]");
    results = readSplit(format, splits[1], job);
    assertEquals(2, results.size(), "splits[1] length");
    assertEquals("this is a test", results.get(0).toString(), "splits[1][0]");
    assertEquals("of gzip", results.get(1).toString(), "splits[1][1]");
  }
  
  public static void main(String[] args) throws Exception {
    new TestKeyValueTextInputFormat().testFormat();
  }
}
