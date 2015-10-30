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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Test;

public class TestLineRecordReader {
  private static Path workDir = new Path(new Path(System.getProperty(
      "test.build.data", "target"), "data"), "TestTextInputFormat");
  private static Path inputDir = new Path(workDir, "input");

  private void testSplitRecords(String testFileName, long firstSplitLength)
      throws IOException {
    URL testFileUrl = getClass().getClassLoader().getResource(testFileName);
    assertNotNull("Cannot find " + testFileName, testFileUrl);
    File testFile = new File(testFileUrl.getFile());
    long testFileSize = testFile.length();
    Path testFilePath = new Path(testFile.getAbsolutePath());
    Configuration conf = new Configuration();
    testSplitRecordsForFile(conf, firstSplitLength, testFileSize, testFilePath);
  }

  private void testSplitRecordsForFile(Configuration conf,
      long firstSplitLength, long testFileSize, Path testFilePath)
      throws IOException {
    conf.setInt(org.apache.hadoop.mapreduce.lib.input.
        LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);
    assertTrue("unexpected test data at " + testFilePath,
        testFileSize > firstSplitLength);

    String delimiter = conf.get("textinputformat.record.delimiter");
    byte[] recordDelimiterBytes = null;
    if (null != delimiter) {
      recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
    }
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());

    // read the data without splitting to count the records
    FileSplit split = new FileSplit(testFilePath, 0, testFileSize,
        (String[])null);
    LineRecordReader reader = new LineRecordReader(recordDelimiterBytes);
    reader.initialize(split, context);
    int numRecordsNoSplits = 0;
    while (reader.nextKeyValue()) {
      ++numRecordsNoSplits;
    }
    reader.close();

    // count the records in the first split
    split = new FileSplit(testFilePath, 0, firstSplitLength, (String[])null);
    reader = new LineRecordReader(recordDelimiterBytes);
    reader.initialize(split, context);
    int numRecordsFirstSplit = 0;
    while (reader.nextKeyValue()) {
      ++numRecordsFirstSplit;
    }
    reader.close();

    // count the records in the second split
    split = new FileSplit(testFilePath, firstSplitLength,
        testFileSize - firstSplitLength, (String[])null);
    reader = new LineRecordReader(recordDelimiterBytes);
    reader.initialize(split, context);
    int numRecordsRemainingSplits = 0;
    while (reader.nextKeyValue()) {
      ++numRecordsRemainingSplits;
    }
    reader.close();
    assertEquals("Unexpected number of records in split ", numRecordsNoSplits,
        numRecordsFirstSplit + numRecordsRemainingSplits);
  }

  @Test
  public void testBzip2SplitEndsAtCR() throws IOException {
    // the test data contains a carriage-return at the end of the first
    // split which ends at compressed offset 136498 and the next
    // character is not a linefeed
    testSplitRecords("blockEndingInCR.txt.bz2", 136498);
  }

  @Test
  public void testBzip2SplitEndsAtCRThenLF() throws IOException {
    // the test data contains a carriage-return at the end of the first
    // split which ends at compressed offset 136498 and the next
    // character is a linefeed
    testSplitRecords("blockEndingInCRThenLF.txt.bz2", 136498);
  }

  @Test(expected=IOException.class)
  public void testSafeguardSplittingUnsplittableFiles() throws IOException {
    // The LineRecordReader must fail when trying to read a file that
    // was compressed using an unsplittable file format
    testSplitRecords("TestSafeguardSplittingUnsplittableFiles.txt.gz", 2);
  }

  // Use the LineRecordReader to read records from the file
  public ArrayList<String> readRecords(URL testFileUrl, int splitSize)
      throws IOException {

    // Set up context
    File testFile = new File(testFileUrl.getFile());
    long testFileSize = testFile.length();
    Path testFilePath = new Path(testFile.getAbsolutePath());
    Configuration conf = new Configuration();
    conf.setInt("io.file.buffer.size", 1);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());

    // Gather the records returned by the record reader
    ArrayList<String> records = new ArrayList<String>();

    long offset = 0;
    while (offset < testFileSize) {
      FileSplit split = new FileSplit(testFilePath, offset, splitSize, null);
      LineRecordReader reader = new LineRecordReader();
      reader.initialize(split, context);

      while (reader.nextKeyValue()) {
        records.add(reader.getCurrentValue().toString());
      }
      offset += splitSize;
    }
    return records;
  }

  // Gather the records by just splitting on new lines
  public String[] readRecordsDirectly(URL testFileUrl, boolean bzip)
      throws IOException {
    int MAX_DATA_SIZE = 1024 * 1024;
    byte[] data = new byte[MAX_DATA_SIZE];
    FileInputStream fis = new FileInputStream(testFileUrl.getFile());
    int count;
    if (bzip) {
      BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(fis);
      count = bzIn.read(data);
      bzIn.close();
    } else {
      count = fis.read(data);
    }
    fis.close();
    assertTrue("Test file data too big for buffer", count < data.length);
    return new String(data, 0, count, "UTF-8").split("\n");
  }

  public void checkRecordSpanningMultipleSplits(String testFile,
                                                int splitSize,
                                                boolean bzip)
      throws IOException {
    URL testFileUrl = getClass().getClassLoader().getResource(testFile);
    ArrayList<String> records = readRecords(testFileUrl, splitSize);
    String[] actuals = readRecordsDirectly(testFileUrl, bzip);

    assertEquals("Wrong number of records", actuals.length, records.size());

    boolean hasLargeRecord = false;
    for (int i = 0; i < actuals.length; ++i) {
      assertEquals(actuals[i], records.get(i));
      if (actuals[i].length() > 2 * splitSize) {
        hasLargeRecord = true;
      }
    }

    assertTrue("Invalid test data. Doesn't have a large enough record",
               hasLargeRecord);
  }

  @Test
  public void testRecordSpanningMultipleSplits()
      throws IOException {
    checkRecordSpanningMultipleSplits("recordSpanningMultipleSplits.txt",
                                      10,
                                      false);
  }

  @Test
  public void testRecordSpanningMultipleSplitsCompressed()
      throws IOException {
    // The file is generated with bz2 block size of 100k. The split size
    // needs to be larger than that for the CompressedSplitLineReader to
    // work.
    checkRecordSpanningMultipleSplits("recordSpanningMultipleSplits.txt.bz2",
                                      200 * 1000,
                                      true);
  }

  @Test
  public void testStripBOM() throws IOException {
    // the test data contains a BOM at the start of the file
    // confirm the BOM is skipped by LineRecordReader
    String UTF8_BOM = "\uFEFF";
    URL testFileUrl = getClass().getClassLoader().getResource("testBOM.txt");
    assertNotNull("Cannot find testBOM.txt", testFileUrl);
    File testFile = new File(testFileUrl.getFile());
    Path testFilePath = new Path(testFile.getAbsolutePath());
    long testFileSize = testFile.length();
    Configuration conf = new Configuration();
    conf.setInt(org.apache.hadoop.mapreduce.lib.input.
        LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);

    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());

    // read the data and check whether BOM is skipped
    FileSplit split = new FileSplit(testFilePath, 0, testFileSize,
        (String[])null);
    LineRecordReader reader = new LineRecordReader();
    reader.initialize(split, context);
    int numRecords = 0;
    boolean firstLine = true;
    boolean skipBOM = true;
    while (reader.nextKeyValue()) {
      if (firstLine) {
        firstLine = false;
        if (reader.getCurrentValue().toString().startsWith(UTF8_BOM)) {
          skipBOM = false;
        }
      }
      ++numRecords;
    }
    reader.close();

    assertTrue("BOM is not skipped", skipBOM);
  }

  @Test
  public void testMultipleClose() throws IOException {
    URL testFileUrl = getClass().getClassLoader().
        getResource("recordSpanningMultipleSplits.txt.bz2");
    assertNotNull("Cannot find recordSpanningMultipleSplits.txt.bz2",
        testFileUrl);
    File testFile = new File(testFileUrl.getFile());
    Path testFilePath = new Path(testFile.getAbsolutePath());
    long testFileSize = testFile.length();
    Configuration conf = new Configuration();
    conf.setInt(org.apache.hadoop.mapreduce.lib.input.
        LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());

    // read the data and check whether BOM is skipped
    FileSplit split = new FileSplit(testFilePath, 0, testFileSize, null);
    LineRecordReader reader = new LineRecordReader();
    reader.initialize(split, context);

    //noinspection StatementWithEmptyBody
    while (reader.nextKeyValue()) ;
    reader.close();
    reader.close();

    BZip2Codec codec = new BZip2Codec();
    codec.setConf(conf);
    Set<Decompressor> decompressors = new HashSet<Decompressor>();
    for (int i = 0; i < 10; ++i) {
      decompressors.add(CodecPool.getDecompressor(codec));
    }
    assertEquals(10, decompressors.size());
  }

  /**
   * Writes the input test file
   *
   * @param conf
   * @return Path of the file created
   * @throws IOException
   */
  private Path createInputFile(Configuration conf, String data)
      throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    Path file = new Path(inputDir, "test.txt");
    Writer writer = new OutputStreamWriter(localFs.create(file));
    try {
      writer.write(data);
    } finally {
      writer.close();
    }
    return file;
  }

  @Test
  public void testUncompressedInput() throws Exception {
    Configuration conf = new Configuration();
    String inputData = "abc+++def+++ghi+++"
        + "jkl+++mno+++pqr+++stu+++vw +++xyz";
    Path inputFile = createInputFile(conf, inputData);
    conf.set("textinputformat.record.delimiter", "+++");
    for(int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for(int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        testSplitRecordsForFile(conf, splitSize, inputData.length(), inputFile);
      }
    }
  }

  @Test
  public void testUncompressedInputContainingCRLF() throws Exception {
    Configuration conf = new Configuration();
    String inputData = "a\r\nb\rc\nd\r\n";
    Path inputFile = createInputFile(conf, inputData);
    for(int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for(int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        testSplitRecordsForFile(conf, splitSize, inputData.length(), inputFile);
      }
    }
  }

  @Test
  public void testUncompressedInputCustomDelimiterPosValue()
      throws Exception {
    Configuration conf = new Configuration();
    String inputData = "1234567890ab12ab345";
    Path inputFile = createInputFile(conf, inputData);
    conf.setInt("io.file.buffer.size", 10);
    conf.setInt(org.apache.hadoop.mapreduce.lib.input.
        LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);
    String delimiter = "ab";
    byte[] recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
    FileSplit split = new FileSplit(inputFile, 0, 15, (String[])null);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf,
        new TaskAttemptID());
    LineRecordReader reader = new LineRecordReader(recordDelimiterBytes);
    reader.initialize(split, context);
    LongWritable key;
    Text value;
    reader.nextKeyValue();
    key = reader.getCurrentKey();
    value = reader.getCurrentValue();
    // Get first record:"1234567890"
    assertEquals(10, value.getLength());
    assertEquals(0, key.get());
    reader.nextKeyValue();
    // Get second record:"12"
    assertEquals(2, value.getLength());
    // Key should be 12 right after "1234567890ab"
    assertEquals(12, key.get());
    reader.nextKeyValue();
    // Get third record:"345"
    assertEquals(3, value.getLength());
    // Key should be 16 right after "1234567890ab12ab"
    assertEquals(16, key.get());
    assertFalse(reader.nextKeyValue());
    // Key should be 19 right after "1234567890ab12ab345"
    assertEquals(19, key.get());

    split = new FileSplit(inputFile, 15, 4, (String[])null);
    reader = new LineRecordReader(recordDelimiterBytes);
    reader.initialize(split, context);
    // No record is in the second split because the second split dropped
    // the first record, which was already reported by the first split.
    assertFalse(reader.nextKeyValue());

    inputData = "123456789aab";
    inputFile = createInputFile(conf, inputData);
    split = new FileSplit(inputFile, 0, 12, (String[])null);
    reader = new LineRecordReader(recordDelimiterBytes);
    reader.initialize(split, context);
    reader.nextKeyValue();
    key = reader.getCurrentKey();
    value = reader.getCurrentValue();
    // Get first record:"123456789a"
    assertEquals(10, value.getLength());
    assertEquals(0, key.get());
    assertFalse(reader.nextKeyValue());
    // Key should be 12 right after "123456789aab"
    assertEquals(12, key.get());

    inputData = "123456789a";
    inputFile = createInputFile(conf, inputData);
    split = new FileSplit(inputFile, 0, 10, (String[])null);
    reader = new LineRecordReader(recordDelimiterBytes);
    reader.initialize(split, context);
    reader.nextKeyValue();
    key = reader.getCurrentKey();
    value = reader.getCurrentValue();
    // Get first record:"123456789a"
    assertEquals(10, value.getLength());
    assertEquals(0, key.get());
    assertFalse(reader.nextKeyValue());
    // Key should be 10 right after "123456789a"
    assertEquals(10, key.get());

    inputData = "123456789ab";
    inputFile = createInputFile(conf, inputData);
    split = new FileSplit(inputFile, 0, 11, (String[])null);
    reader = new LineRecordReader(recordDelimiterBytes);
    reader.initialize(split, context);
    reader.nextKeyValue();
    key = reader.getCurrentKey();
    value = reader.getCurrentValue();
    // Get first record:"123456789"
    assertEquals(9, value.getLength());
    assertEquals(0, key.get());
    assertFalse(reader.nextKeyValue());
    // Key should be 11 right after "123456789ab"
    assertEquals(11, key.get());
  }

  @Test
  public void testUncompressedInputDefaultDelimiterPosValue()
      throws Exception {
    Configuration conf = new Configuration();
    String inputData = "1234567890\r\n12\r\n345";
    Path inputFile = createInputFile(conf, inputData);
    conf.setInt("io.file.buffer.size", 10);
    conf.setInt(org.apache.hadoop.mapreduce.lib.input.
        LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);
    FileSplit split = new FileSplit(inputFile, 0, 15, (String[])null);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf,
        new TaskAttemptID());
    LineRecordReader reader = new LineRecordReader(null);
    reader.initialize(split, context);
    LongWritable key;
    Text value;
    reader.nextKeyValue();
    key = reader.getCurrentKey();
    value = reader.getCurrentValue();
    // Get first record:"1234567890"
    assertEquals(10, value.getLength());
    assertEquals(0, key.get());
    reader.nextKeyValue();
    // Get second record:"12"
    assertEquals(2, value.getLength());
    // Key should be 12 right after "1234567890\r\n"
    assertEquals(12, key.get());
    assertFalse(reader.nextKeyValue());
    // Key should be 16 right after "1234567890\r\n12\r\n"
    assertEquals(16, key.get());

    split = new FileSplit(inputFile, 15, 4, (String[])null);
    reader = new LineRecordReader(null);
    reader.initialize(split, context);
    // The second split dropped the first record "\n"
    reader.nextKeyValue();
    key = reader.getCurrentKey();
    value = reader.getCurrentValue();
    // Get third record:"345"
    assertEquals(3, value.getLength());
    // Key should be 16 right after "1234567890\r\n12\r\n"
    assertEquals(16, key.get());
    assertFalse(reader.nextKeyValue());
    // Key should be 19 right after "1234567890\r\n12\r\n345"
    assertEquals(19, key.get());

    inputData = "123456789\r\r\n";
    inputFile = createInputFile(conf, inputData);
    split = new FileSplit(inputFile, 0, 12, (String[])null);
    reader = new LineRecordReader(null);
    reader.initialize(split, context);
    reader.nextKeyValue();
    key = reader.getCurrentKey();
    value = reader.getCurrentValue();
    // Get first record:"123456789"
    assertEquals(9, value.getLength());
    assertEquals(0, key.get());
    reader.nextKeyValue();
    // Get second record:""
    assertEquals(0, value.getLength());
    // Key should be 10 right after "123456789\r"
    assertEquals(10, key.get());
    assertFalse(reader.nextKeyValue());
    // Key should be 12 right after "123456789\r\r\n"
    assertEquals(12, key.get());
  }
}
