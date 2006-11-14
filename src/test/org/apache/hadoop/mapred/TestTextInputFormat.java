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
import junit.framework.TestCase;

import org.apache.commons.logging.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;

public class TestTextInputFormat extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(TestTextInputFormat.class.getName());

  private static int MAX_LENGTH = 10000;
  
  private static JobConf defaultConf = new JobConf();
  private static FileSystem localFs = null; 
  static {
    try {
      localFs = FileSystem.getNamed("local", defaultConf);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }
  private static Path workDir = 
    new Path(new Path(System.getProperty("test.build.data", "."), "data"),
             "TestTextInputFormat");
  
  public void testFormat() throws Exception {
    JobConf job = new JobConf();
    Path file = new Path(workDir, "test.txt");

    Reporter reporter = new Reporter() {
        public void setStatus(String status) throws IOException {}
        public void progress() throws IOException {}
      };
    
    int seed = new Random().nextInt();
    LOG.info("seed = "+seed);
    Random random = new Random(seed);

    localFs.delete(workDir);
    job.setInputPath(workDir);

    // for a variety of lengths
    for (int length = 0; length < MAX_LENGTH;
         length+= random.nextInt(MAX_LENGTH/10)+1) {

      LOG.debug("creating; entries = " + length);

      // create a file with length entries
      Writer writer = new OutputStreamWriter(localFs.create(file));
      try {
        for (int i = 0; i < length; i++) {
          writer.write(Integer.toString(i));
          writer.write("\n");
        }
      } finally {
        writer.close();
      }

      // try splitting the file in a variety of sizes
      TextInputFormat format = new TextInputFormat();
      format.configure(job);
      LongWritable key = new LongWritable();
      Text value = new Text();
      for (int i = 0; i < 3; i++) {
        int numSplits = random.nextInt(MAX_LENGTH/20)+1;
        LOG.debug("splitting: requesting = " + numSplits);
        FileSplit[] splits = format.getSplits(localFs, job, numSplits);
        LOG.debug("splitting: got =        " + splits.length);

        // check each split
        BitSet bits = new BitSet(length);
        for (int j = 0; j < splits.length; j++) {
          LOG.debug("split["+j+"]= " + splits[j].getStart() + "+" +
                   splits[j].getLength());
          RecordReader reader =
            format.getRecordReader(localFs, splits[j], job, reporter);
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
              assertFalse("Key in multiple partitions.", bits.get(v));
              bits.set(v);
              count++;
            }
            LOG.debug("splits["+j+"]="+splits[j]+" count=" + count);
          } finally {
            reader.close();
          }
        }
        assertEquals("Some keys in no partition.", length, bits.cardinality());
      }

    }
  }

  private InputStream makeStream(String str) throws IOException {
    Text text = new Text(str);
    return new ByteArrayInputStream(text.getBytes(), 0, text.getLength());
  }
  
  public void testUTF8() throws Exception {
    InputStream in = makeStream("abcd\u20acbdcd\u20ac");
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    TextInputFormat.readLine(in, out);
    Text line = new Text();
    line.set(out.toByteArray());
    assertEquals("readLine changed utf8 characters", 
                 "abcd\u20acbdcd\u20ac", line.toString());
    in = makeStream("abc\u200axyz");
    out.reset();
    TextInputFormat.readLine(in, out);
    line.set(out.toByteArray());
    assertEquals("split on fake newline", "abc\u200axyz", line.toString());
  }

  public void testNewLines() throws Exception {
    InputStream in = makeStream("a\nbb\n\nccc\rdddd\r\neeeee");
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    TextInputFormat.readLine(in, out);
    assertEquals("line1 length", 1, out.size());
    out.reset();
    TextInputFormat.readLine(in, out);
    assertEquals("line2 length", 2, out.size());
    out.reset();
    TextInputFormat.readLine(in, out);
    assertEquals("line3 length", 0, out.size());
    out.reset();
    TextInputFormat.readLine(in, out);
    assertEquals("line4 length", 3, out.size());
    out.reset();
    TextInputFormat.readLine(in, out);
    assertEquals("line5 length", 4, out.size());
    out.reset();
    TextInputFormat.readLine(in, out);
    assertEquals("line5 length", 5, out.size());
    assertEquals("end of file", 0, TextInputFormat.readLine(in, out));
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
  
  private static class VoidReporter implements Reporter {
    public void progress() {}
    public void setStatus(String msg) {}
  }
  private static final Reporter voidReporter = new VoidReporter();
  
  private static List<Text> readSplit(InputFormat format, 
                                      FileSplit split, 
                                      JobConf job) throws IOException {
    List<Text> result = new ArrayList<Text>();
    RecordReader reader = format.getRecordReader(localFs, split, job,
                                                 voidReporter);
    LongWritable key = (LongWritable) reader.createKey();
    Text value = (Text) reader.createValue();
    while (reader.next(key, value)) {
      result.add(value);
      value = (Text) reader.createValue();
    }
    return result;
  }
  
  /**
   * Test using the gzip codec for reading
   */
  public static void testGzip() throws IOException {
    JobConf job = new JobConf();
    CompressionCodec gzip = new GzipCodec();
    ReflectionUtils.setConf(gzip, job);
    localFs.delete(workDir);
    writeFile(localFs, new Path(workDir, "part1.txt.gz"), gzip, 
              "the quick\nbrown\nfox jumped\nover\n the lazy\n dog\n");
    writeFile(localFs, new Path(workDir, "part2.txt.gz"), gzip,
              "this is a test\nof gzip\n");
    job.setInputPath(workDir);
    TextInputFormat format = new TextInputFormat();
    format.configure(job);
    FileSplit[] splits = format.getSplits(localFs, job, 100);
    assertEquals("compressed splits == 2", 2, splits.length);
    if (splits[0].getPath().getName().equals("part2.txt.gz")) {
      FileSplit tmp = splits[0];
      splits[0] = splits[1];
      splits[1] = tmp;
    }
    List<Text> results = readSplit(format, splits[0], job);
    assertEquals("splits[0] length", 6, results.size());
    assertEquals("splits[0][5]", " dog", results.get(5).toString());
    results = readSplit(format, splits[1], job);
    assertEquals("splits[1] length", 2, results.size());
    assertEquals("splits[1][0]", "this is a test", 
                 results.get(0).toString());    
    assertEquals("splits[1][1]", "of gzip", 
                 results.get(1).toString());    
  }
  
  public static void main(String[] args) throws Exception {
    new TestTextInputFormat().testFormat();
  }
}
