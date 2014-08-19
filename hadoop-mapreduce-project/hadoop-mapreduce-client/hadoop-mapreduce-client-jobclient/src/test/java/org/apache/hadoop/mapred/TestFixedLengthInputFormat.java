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

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestFixedLengthInputFormat {

  private static Log LOG;
  private static Configuration defaultConf;
  private static FileSystem localFs; 
  private static Path workDir;
  private static Reporter voidReporter;
  
  // some chars for the record data
  private static char[] chars;
  private static Random charRand;

  @BeforeClass
  public static void onlyOnce() {
    try {
      LOG = LogFactory.getLog(TestFixedLengthInputFormat.class.getName());
      defaultConf = new Configuration();
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
      voidReporter = Reporter.NULL;
      // our set of chars
      chars = ("abcdefghijklmnopqrstuvABCDEFGHIJKLMN OPQRSTUVWXYZ1234567890)"
          + "(*&^%$#@!-=><?:\"{}][';/.,']").toCharArray();
      workDir = 
          new Path(new Path(System.getProperty("test.build.data", "."), "data"),
          "TestKeyValueFixedLengthInputFormat");
      charRand = new Random();
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }

  /**
   * 20 random tests of various record, file, and split sizes.  All tests have
   * uncompressed file as input.
   */
  @Test (timeout=500000)
  public void testFormat() throws IOException {
    runRandomTests(null);
  }

  /**
   * 20 random tests of various record, file, and split sizes.  All tests have
   * compressed file as input.
   */
  @Test (timeout=500000)
  public void testFormatCompressedIn() throws IOException {
    runRandomTests(new GzipCodec());
  }

  /**
   * Test with no record length set.
   */
  @Test (timeout=5000)
  public void testNoRecordLength() throws IOException {
    localFs.delete(workDir, true);
    Path file = new Path(workDir, new String("testFormat.txt"));
    createFile(file, null, 10, 10);
    // Set the fixed length record length config property 
    JobConf job = new JobConf(defaultConf);
    FileInputFormat.setInputPaths(job, workDir);
    FixedLengthInputFormat format = new FixedLengthInputFormat();
    format.configure(job);
    InputSplit splits[] = format.getSplits(job, 1);
    boolean exceptionThrown = false;
    for (InputSplit split : splits) {
      try {
        RecordReader<LongWritable, BytesWritable> reader = 
            format.getRecordReader(split, job, voidReporter);
      } catch(IOException ioe) {
        exceptionThrown = true;
        LOG.info("Exception message:" + ioe.getMessage());
      }
    }
    assertTrue("Exception for not setting record length:", exceptionThrown);
  }

  /**
   * Test with record length set to 0
   */
  @Test (timeout=5000)
  public void testZeroRecordLength() throws IOException {
    localFs.delete(workDir, true);
    Path file = new Path(workDir, new String("testFormat.txt"));
    createFile(file, null, 10, 10);
    // Set the fixed length record length config property 
    JobConf job = new JobConf(defaultConf);
    FileInputFormat.setInputPaths(job, workDir);
    FixedLengthInputFormat format = new FixedLengthInputFormat();
    format.setRecordLength(job, 0);
    format.configure(job);
    InputSplit splits[] = format.getSplits(job, 1);
    boolean exceptionThrown = false;
    for (InputSplit split : splits) {
      try {
        RecordReader<LongWritable, BytesWritable> reader = 
                             format.getRecordReader(split, job, voidReporter);
      } catch(IOException ioe) {
        exceptionThrown = true;
        LOG.info("Exception message:" + ioe.getMessage());
      }
    }
    assertTrue("Exception for zero record length:", exceptionThrown);
  }

  /**
   * Test with record length set to a negative value
   */
  @Test (timeout=5000)
  public void testNegativeRecordLength() throws IOException {
    localFs.delete(workDir, true);
    Path file = new Path(workDir, new String("testFormat.txt"));
    createFile(file, null, 10, 10);
    // Set the fixed length record length config property 
    JobConf job = new JobConf(defaultConf);
    FileInputFormat.setInputPaths(job, workDir);
    FixedLengthInputFormat format = new FixedLengthInputFormat();
    format.setRecordLength(job, -10);
    format.configure(job);
    InputSplit splits[] = format.getSplits(job, 1);
    boolean exceptionThrown = false;
    for (InputSplit split : splits) {
      try {
        RecordReader<LongWritable, BytesWritable> reader = 
            format.getRecordReader(split, job, voidReporter);
      } catch(IOException ioe) {
        exceptionThrown = true;
        LOG.info("Exception message:" + ioe.getMessage());
      }
    }
    assertTrue("Exception for negative record length:", exceptionThrown);
  }

  /**
   * Test with partial record at the end of a compressed input file.
   */
  @Test (timeout=5000)
  public void testPartialRecordCompressedIn() throws IOException {
    CompressionCodec gzip = new GzipCodec();
    runPartialRecordTest(gzip);
  }

  /**
   * Test with partial record at the end of an uncompressed input file.
   */
  @Test (timeout=5000)
  public void testPartialRecordUncompressedIn() throws IOException {
    runPartialRecordTest(null);
  }

  /**
   * Test using the gzip codec with two input files.
   */
  @Test (timeout=5000)
  public void testGzipWithTwoInputs() throws IOException {
    CompressionCodec gzip = new GzipCodec();
    localFs.delete(workDir, true);
    FixedLengthInputFormat format = new FixedLengthInputFormat();
    JobConf job = new JobConf(defaultConf);
    format.setRecordLength(job, 5);
    FileInputFormat.setInputPaths(job, workDir);
    ReflectionUtils.setConf(gzip, job);
    format.configure(job);
    // Create files with fixed length records with 5 byte long records.
    writeFile(localFs, new Path(workDir, "part1.txt.gz"), gzip, 
        "one  two  threefour five six  seveneightnine ten  ");
    writeFile(localFs, new Path(workDir, "part2.txt.gz"), gzip,
        "ten  nine eightsevensix  five four threetwo  one  ");
    InputSplit[] splits = format.getSplits(job, 100);
    assertEquals("compressed splits == 2", 2, splits.length);
    FileSplit tmp = (FileSplit) splits[0];
    if (tmp.getPath().getName().equals("part2.txt.gz")) {
      splits[0] = splits[1];
      splits[1] = tmp;
    }
    List<String> results = readSplit(format, splits[0], job);
    assertEquals("splits[0] length", 10, results.size());
    assertEquals("splits[0][5]", "six  ", results.get(5));
    results = readSplit(format, splits[1], job);
    assertEquals("splits[1] length", 10, results.size());
    assertEquals("splits[1][0]", "ten  ", results.get(0));
    assertEquals("splits[1][1]", "nine ", results.get(1));
  }

  // Create a file containing fixed length records with random data
  private ArrayList<String> createFile(Path targetFile, CompressionCodec codec,
                                       int recordLen,
                                       int numRecords) throws IOException {
    ArrayList<String> recordList = new ArrayList<String>(numRecords);
    OutputStream ostream = localFs.create(targetFile);
    if (codec != null) {
      ostream = codec.createOutputStream(ostream);
    }
    Writer writer = new OutputStreamWriter(ostream);
    try {
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < numRecords; i++) {
        for (int j = 0; j < recordLen; j++) {
          sb.append(chars[charRand.nextInt(chars.length)]);
        }
        String recordData = sb.toString();
        recordList.add(recordData);
        writer.write(recordData);
        sb.setLength(0);
      }
    } finally {
      writer.close();
    }
    return recordList;
  }

  private void runRandomTests(CompressionCodec codec) throws IOException {
    StringBuilder fileName = new StringBuilder("testFormat.txt");
    if (codec != null) {
      fileName.append(".gz");
    }
    localFs.delete(workDir, true);
    Path file = new Path(workDir, fileName.toString());
    int seed = new Random().nextInt();
    LOG.info("Seed = " + seed);
    Random random = new Random(seed);
    int MAX_TESTS = 20;
    LongWritable key = new LongWritable();
    BytesWritable value = new BytesWritable();

    for (int i = 0; i < MAX_TESTS; i++) {
      LOG.info("----------------------------------------------------------");
      // Maximum total records of 999
      int totalRecords = random.nextInt(999)+1;
      // Test an empty file
      if (i == 8) {
         totalRecords = 0;
      }
      // Maximum bytes in a record of 100K
      int recordLength = random.nextInt(1024*100)+1;
      // For the 11th test, force a record length of 1
      if (i == 10) {
        recordLength = 1;
      }
      // The total bytes in the test file
      int fileSize = (totalRecords * recordLength);
      LOG.info("totalRecords=" + totalRecords + " recordLength="
          + recordLength);
      // Create the job 
      JobConf job = new JobConf(defaultConf);
      if (codec != null) {
        ReflectionUtils.setConf(codec, job);
      }
      // Create the test file
      ArrayList<String> recordList
          = createFile(file, codec, recordLength, totalRecords);
      assertTrue(localFs.exists(file));
      //set the fixed length record length config property for the job
      FixedLengthInputFormat.setRecordLength(job, recordLength);

      int numSplits = 1;
      // Arbitrarily set number of splits.
      if (i > 0) {
        if (i == (MAX_TESTS-1)) {
          // Test a split size that is less than record len
          numSplits = (int)(fileSize/Math.floor(recordLength/2));
        } else {
          if (MAX_TESTS % i == 0) {
            // Let us create a split size that is forced to be 
            // smaller than the end file itself, (ensures 1+ splits)
            numSplits = fileSize/(fileSize - random.nextInt(fileSize));
          } else {
            // Just pick a random split size with no upper bound 
            numSplits = Math.max(1, fileSize/random.nextInt(Integer.MAX_VALUE));
          }
        }
        LOG.info("Number of splits set to: " + numSplits);
      }

      // Setup the input path
      FileInputFormat.setInputPaths(job, workDir);
      // Try splitting the file in a variety of sizes
      FixedLengthInputFormat format = new FixedLengthInputFormat();
      format.configure(job);
      InputSplit splits[] = format.getSplits(job, numSplits);
      LOG.info("Actual number of splits = " + splits.length);
      // Test combined split lengths = total file size
      long recordOffset = 0;
      int recordNumber = 0;
      for (InputSplit split : splits) {
        RecordReader<LongWritable, BytesWritable> reader = 
            format.getRecordReader(split, job, voidReporter);
        Class<?> clazz = reader.getClass();
        assertEquals("RecordReader class should be FixedLengthRecordReader:", 
            FixedLengthRecordReader.class, clazz);
        // Plow through the records in this split
        while (reader.next(key, value)) {
          assertEquals("Checking key", (long)(recordNumber*recordLength),
              key.get());
          String valueString =
              new String(value.getBytes(), 0, value.getLength());
          assertEquals("Checking record length:", recordLength,
              value.getLength());
          assertTrue("Checking for more records than expected:",
              recordNumber < totalRecords);
          String origRecord = recordList.get(recordNumber);
          assertEquals("Checking record content:", origRecord, valueString);
          recordNumber++;
        }
        reader.close();
      }
      assertEquals("Total original records should be total read records:",
          recordList.size(), recordNumber);
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

  private static List<String> readSplit(FixedLengthInputFormat format, 
                                        InputSplit split, 
                                        JobConf job) throws IOException {
    List<String> result = new ArrayList<String>();
    RecordReader<LongWritable, BytesWritable> reader =
        format.getRecordReader(split, job, voidReporter);
    LongWritable key = reader.createKey();
    BytesWritable value = reader.createValue();
    try {
      while (reader.next(key, value)) {
        result.add(new String(value.getBytes(), 0, value.getLength()));
      }
    } finally {
      reader.close();
    }
    return result;
  }

  private void runPartialRecordTest(CompressionCodec codec) throws IOException {
    localFs.delete(workDir, true);
    // Create a file with fixed length records with 5 byte long
    // records with a partial record at the end.
    StringBuilder fileName = new StringBuilder("testFormat.txt");
    if (codec != null) {
      fileName.append(".gz");
    }
    FixedLengthInputFormat format = new FixedLengthInputFormat();
    JobConf job = new JobConf(defaultConf);
    format.setRecordLength(job, 5);
    FileInputFormat.setInputPaths(job, workDir);
    if (codec != null) {
      ReflectionUtils.setConf(codec, job);
    }
    format.configure(job);
    writeFile(localFs, new Path(workDir, fileName.toString()), codec,
            "one  two  threefour five six  seveneightnine ten");
    InputSplit[] splits = format.getSplits(job, 100);
    if (codec != null) {
      assertEquals("compressed splits == 1", 1, splits.length);
    }
    boolean exceptionThrown = false;
    for (InputSplit split : splits) {
      try {
        List<String> results = readSplit(format, split, job);
      } catch(IOException ioe) {
        exceptionThrown = true;
        LOG.info("Exception message:" + ioe.getMessage());
      }
    }
    assertTrue("Exception for partial record:", exceptionThrown);
  }

}
