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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.Decompressor;
import org.junit.jupiter.api.Test;

public class TestLineRecordReader {
  private static Path workDir = new Path(new Path(System.getProperty(
      "test.build.data", "target"), "data"), "TestTextInputFormat");
  private static Path inputDir = new Path(workDir, "input");

  private void testSplitRecords(String testFileName, long firstSplitLength)
      throws IOException {
    URL testFileUrl = getClass().getClassLoader().getResource(testFileName);
    assertNotNull(testFileUrl, "Cannot find " + testFileName);
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
    assertTrue(testFileSize > firstSplitLength,
        "unexpected test data at " + testFilePath);

    String delimiter = conf.get("textinputformat.record.delimiter");
    byte[] recordDelimiterBytes = null;
    if (null != delimiter) {
      recordDelimiterBytes = delimiter.getBytes(StandardCharsets.UTF_8);
    }
    // read the data without splitting to count the records
    FileSplit split = new FileSplit(testFilePath, 0, testFileSize,
        (String[])null);
    LineRecordReader reader = new LineRecordReader(conf, split,
        recordDelimiterBytes);
    LongWritable key = new LongWritable();
    Text value = new Text();
    int numRecordsNoSplits = 0;
    while (reader.next(key, value)) {
      ++numRecordsNoSplits;
    }
    reader.close();

    // count the records in the first split
    split = new FileSplit(testFilePath, 0, firstSplitLength, (String[])null);
    reader = new LineRecordReader(conf, split, recordDelimiterBytes);
    int numRecordsFirstSplit = 0;
    while (reader.next(key,  value)) {
      ++numRecordsFirstSplit;
    }
    reader.close();

    // count the records in the second split
    split = new FileSplit(testFilePath, firstSplitLength,
        testFileSize - firstSplitLength, (String[])null);
    reader = new LineRecordReader(conf, split, recordDelimiterBytes);
    int numRecordsRemainingSplits = 0;
    while (reader.next(key,  value)) {
      ++numRecordsRemainingSplits;
    }
    reader.close();

    assertEquals(numRecordsNoSplits, numRecordsFirstSplit + numRecordsRemainingSplits,
        "Unexpected number of records in split");
  }

  private void testLargeSplitRecordForFile(Configuration conf,
      long firstSplitLength, long testFileSize, Path testFilePath)
      throws IOException {
    conf.setInt(org.apache.hadoop.mapreduce.lib.input.
        LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);
    assertTrue(testFileSize < firstSplitLength, "unexpected firstSplitLength:" +
        firstSplitLength);
    String delimiter = conf.get("textinputformat.record.delimiter");
    byte[] recordDelimiterBytes = null;
    if (null != delimiter) {
      recordDelimiterBytes = delimiter.getBytes(StandardCharsets.UTF_8);
    }
    // read the data without splitting to count the records
    FileSplit split = new FileSplit(testFilePath, 0, testFileSize,
        (String[])null);
    LineRecordReader reader = new LineRecordReader(conf, split,
        recordDelimiterBytes);
    LongWritable key = new LongWritable();
    Text value = new Text();
    int numRecordsNoSplits = 0;
    while (reader.next(key, value)) {
      ++numRecordsNoSplits;
    }
    reader.close();

    // count the records in the first split
    split = new FileSplit(testFilePath, 0, firstSplitLength, (String[])null);
    reader = new LineRecordReader(conf, split, recordDelimiterBytes);
    int numRecordsFirstSplit = 0;
    while (reader.next(key, value)) {
      ++numRecordsFirstSplit;
    }
    reader.close();
    assertEquals(numRecordsNoSplits, numRecordsFirstSplit,
        "Unexpected number of records in split");
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

  //This test ensures record reader doesn't lose records when it starts
  //exactly at the starting byte of a bz2 compressed block
  @Test
  public void testBzip2SplitStartAtBlockMarker() throws IOException {
    //136504 in blockEndingInCR.txt.bz2 is the byte at which the bz2 block ends
    //In the following test cases record readers should iterate over all the records
    //and should not miss any record.

    //Start next split at just the start of the block.
    testSplitRecords("blockEndingInCR.txt.bz2", 136504);

    //Start next split a byte forward in next block.
    testSplitRecords("blockEndingInCR.txt.bz2", 136505);

    //Start next split 3 bytes forward in next block.
    testSplitRecords("blockEndingInCR.txt.bz2", 136508);

    //Start next split 10 bytes from behind the end marker.
    testSplitRecords("blockEndingInCR.txt.bz2", 136494);
  }

  @Test
  public void testSafeguardSplittingUnSplittableFiles() throws IOException {
    // The LineRecordReader must fail when trying to read a file that
    // was compressed using an unsplittable file format
    assertThrows(IOException.class, () -> {
      testSplitRecords("TestSafeguardSplittingUnsplittableFiles.txt.gz", 2);
    });
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

    // Gather the records returned by the record reader
    ArrayList<String> records = new ArrayList<String>();

    long offset = 0;
    LongWritable key = new LongWritable();
    Text value = new Text();
    while (offset < testFileSize) {
      FileSplit split =
          new FileSplit(testFilePath, offset, splitSize, (String[]) null);
      LineRecordReader reader = new LineRecordReader(conf, split);

      while (reader.next(key, value)) {
        records.add(value.toString());
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
    assertTrue(count < data.length, "Test file data too big for buffer");
    return new String(data, 0, count, StandardCharsets.UTF_8).split("\n");
  }

  public void checkRecordSpanningMultipleSplits(String testFile,
                                                int splitSize,
                                                boolean bzip)
      throws IOException {
    URL testFileUrl = getClass().getClassLoader().getResource(testFile);
    ArrayList<String> records = readRecords(testFileUrl, splitSize);
    String[] actuals = readRecordsDirectly(testFileUrl, bzip);

    assertEquals(actuals.length, records.size(), "Wrong number of records");

    boolean hasLargeRecord = false;
    for (int i = 0; i < actuals.length; ++i) {
      assertEquals(actuals[i], records.get(i));
      if (actuals[i].length() > 2 * splitSize) {
        hasLargeRecord = true;
      }
    }

    assertTrue(hasLargeRecord,
        "Invalid test data. Doesn't have a large enough record");
  }

  @Test
  public void testRecordSpanningMultipleSplits()
      throws IOException {
    checkRecordSpanningMultipleSplits("recordSpanningMultipleSplits.txt",
        10, false);
  }

  @Test
  public void testRecordSpanningMultipleSplitsCompressed()
      throws IOException {
    // The file is generated with bz2 block size of 100k. The split size
    // needs to be larger than that for the CompressedSplitLineReader to
    // work.
    checkRecordSpanningMultipleSplits("recordSpanningMultipleSplits.txt.bz2",
        200 * 1000, true);
  }

  @Test
  public void testStripBOM() throws IOException {
    // the test data contains a BOM at the start of the file
    // confirm the BOM is skipped by LineRecordReader
    String UTF8_BOM = "\uFEFF";
    URL testFileUrl = getClass().getClassLoader().getResource("testBOM.txt");
    assertNotNull(testFileUrl, "Cannot find testBOM.txt");
    File testFile = new File(testFileUrl.getFile());
    Path testFilePath = new Path(testFile.getAbsolutePath());
    long testFileSize = testFile.length();
    Configuration conf = new Configuration();
    conf.setInt(org.apache.hadoop.mapreduce.lib.input.
        LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);

    // read the data and check whether BOM is skipped
    FileSplit split = new FileSplit(testFilePath, 0, testFileSize,
        (String[])null);
    LineRecordReader reader = new LineRecordReader(conf, split);
    LongWritable key = new LongWritable();
    Text value = new Text();
    int numRecords = 0;
    boolean firstLine = true;
    boolean skipBOM = true;
    while (reader.next(key, value)) {
      if (firstLine) {
        firstLine = false;
        if (value.toString().startsWith(UTF8_BOM)) {
          skipBOM = false;
        }
      }
      ++numRecords;
    }
    reader.close();

    assertTrue(skipBOM, "BOM is not skipped");
  }

  @Test
  public void testMultipleClose() throws IOException {
    URL testFileUrl = getClass().getClassLoader().
        getResource("recordSpanningMultipleSplits.txt.bz2");
    assertNotNull(testFileUrl,
        "Cannot find recordSpanningMultipleSplits.txt.bz2");
    File testFile = new File(testFileUrl.getFile());
    Path testFilePath = new Path(testFile.getAbsolutePath());
    long testFileSize = testFile.length();
    Configuration conf = new Configuration();
    conf.setInt(org.apache.hadoop.mapreduce.lib.input.
        LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);
    FileSplit split = new FileSplit(testFilePath, 0, testFileSize,
        (String[])null);

    LineRecordReader reader = new LineRecordReader(conf, split);
    LongWritable key = new LongWritable();
    Text value = new Text();
    //noinspection StatementWithEmptyBody
    while (reader.next(key, value)) ;
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
  public void testUncompressedInputWithLargeSplitSize() throws Exception {
    Configuration conf = new Configuration();
    // single char delimiter
    String inputData = "abcde +fghij+ klmno+pqrst+uvwxyz";
    Path inputFile = createInputFile(conf, inputData);
    conf.set("textinputformat.record.delimiter", "+");
    // split size over max value of integer
    long longSplitSize = (long)Integer.MAX_VALUE + 1;
    for (int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      conf.setInt("io.file.buffer.size", bufferSize);
      testLargeSplitRecordForFile(conf, longSplitSize, inputData.length(),
          inputFile);
    }
  }

  @Test
  public void testUncompressedInput() throws Exception {
    Configuration conf = new Configuration();
    // single char delimiter, best case
    String inputData = "abc+def+ghi+jkl+mno+pqr+stu+vw +xyz";
    Path inputFile = createInputFile(conf, inputData);
    conf.set("textinputformat.record.delimiter", "+");
    for (int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for (int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        testSplitRecordsForFile(conf, splitSize, inputData.length(), inputFile);
      }
    }
    // multi char delimiter, best case
    inputData = "abc|+|def|+|ghi|+|jkl|+|mno|+|pqr|+|stu|+|vw |+|xyz";
    inputFile = createInputFile(conf, inputData);
    conf.set("textinputformat.record.delimiter", "|+|");
    for (int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for (int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        testSplitRecordsForFile(conf, splitSize, inputData.length(), inputFile);
      }
    }
    // single char delimiter with empty records
    inputData = "abc+def++ghi+jkl++mno+pqr++stu+vw ++xyz";
    inputFile = createInputFile(conf, inputData);
    conf.set("textinputformat.record.delimiter", "+");
    for (int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for (int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        testSplitRecordsForFile(conf, splitSize, inputData.length(), inputFile);
      }
    }
    // multi char delimiter with empty records
    inputData = "abc|+||+|defghi|+|jkl|+||+|mno|+|pqr|+||+|stu|+|vw |+||+|xyz";
    inputFile = createInputFile(conf, inputData);
    conf.set("textinputformat.record.delimiter", "|+|");
    for (int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for (int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        testSplitRecordsForFile(conf, splitSize, inputData.length(), inputFile);
      }
    }
    // multi char delimiter with starting part of the delimiter in the data
    inputData = "abc+def+-ghi+jkl+-mno+pqr+-stu+vw +-xyz";
    inputFile = createInputFile(conf, inputData);
    conf.set("textinputformat.record.delimiter", "+-");
    for (int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for (int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        testSplitRecordsForFile(conf, splitSize, inputData.length(), inputFile);
      }
    }
    // multi char delimiter with newline as start of the delimiter
    inputData = "abc\n+def\n+ghi\n+jkl\n+mno";
    inputFile = createInputFile(conf, inputData);
    conf.set("textinputformat.record.delimiter", "\n+");
    for (int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for (int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        testSplitRecordsForFile(conf, splitSize, inputData.length(), inputFile);
      }
    }
    // multi char delimiter with newline in delimiter and in data
    inputData = "abc\ndef+\nghi+\njkl\nmno";
    inputFile = createInputFile(conf, inputData);
    conf.set("textinputformat.record.delimiter", "+\n");
    for (int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for (int splitSize = 1; splitSize < inputData.length(); splitSize++) {
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
    conf.setInt("io.file.buffer.size", 10);
    conf.setInt(org.apache.hadoop.mapreduce.lib.input.
        LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);
    String inputData = "abcdefghij++kl++mno";
    Path inputFile = createInputFile(conf, inputData);
    String delimiter = "++";
    byte[] recordDelimiterBytes = delimiter.getBytes(StandardCharsets.UTF_8);
    // the first split must contain two records to make sure that it also pulls
    // in the record from the 2nd split
    int splitLength = 15;
    FileSplit split = new FileSplit(inputFile, 0, splitLength, (String[]) null);
    LineRecordReader reader = new LineRecordReader(conf, split,
        recordDelimiterBytes);
    LongWritable key = new LongWritable();
    Text value = new Text();
    // Get first record: "abcdefghij"
    assertTrue(reader.next(key, value), "Expected record got nothing");
    assertEquals(10, value.getLength(), "Wrong length for record value");
    // Position should be 12 right after "abcdefghij++"
    assertEquals(12, reader.getPos(), "Wrong position after record read");
    // Get second record: "kl"
    assertTrue(reader.next(key, value), "Expected record got nothing");
    assertEquals(2, value.getLength(), "Wrong length for record value");
    // Position should be 16 right after "abcdefghij++kl++"
    assertEquals(16, reader.getPos(), "Wrong position after record read");
    // Get third record: "mno"
    assertTrue(reader.next(key, value), "Expected record got nothing");
    assertEquals(3, value.getLength(), "Wrong length for record value");
    // Position should be 19 right after "abcdefghij++kl++mno"
    assertEquals(19, reader.getPos(), "Wrong position after record read");
    assertFalse(reader.next(key, value));
    assertEquals(19, reader.getPos(), "Wrong position after record read");
    reader.close();
    // No record is in the second split because the second split will drop
    // the first record, which was already reported by the first split.
    split = new FileSplit(inputFile, splitLength,
        inputData.length() - splitLength, (String[]) null);
    reader = new LineRecordReader(conf, split, recordDelimiterBytes);
    // The position should be 19 right after "abcdefghij++kl++mno" and should
    // not change
    assertEquals(19, reader.getPos(), "Wrong position after record read");
    assertFalse(reader.next(key, value), "Unexpected record returned");
    assertEquals(19, reader.getPos(), "Wrong position after record read");
    reader.close();

    // multi char delimiter with starting part of the delimiter in the data
    inputData = "abcd+efgh++ijk++mno";
    inputFile = createInputFile(conf, inputData);
    splitLength = 5;
    split = new FileSplit(inputFile, 0, splitLength, (String[]) null);
    reader = new LineRecordReader(conf, split, recordDelimiterBytes);
    // Get first record: "abcd+efgh"
    assertTrue(reader.next(key, value), "Expected record got nothing");
    assertEquals(11, reader.getPos(), "Wrong position after record read");
    assertEquals(9, value.getLength(), "Wrong length for record value");
    // should have jumped over the delimiter, no record
    assertFalse(reader.next(key, value), "Unexpected record returned");
    assertEquals(11, reader.getPos(), "Wrong position after record read");
    reader.close();
    // next split: check for duplicate or dropped records
    split = new FileSplit(inputFile, splitLength,
        inputData.length() - splitLength, (String[]) null);
    reader = new LineRecordReader(conf, split, recordDelimiterBytes);
    // Get second record: "ijk" first in this split
    assertTrue(reader.next(key, value), "Expected record got nothing");
    assertEquals(16, reader.getPos(), "Wrong position after record read");
    assertEquals(3, value.getLength(), "Wrong length for record value");
    // Get third record: "mno" second in this split
    assertTrue(reader.next(key, value), "Expected record got nothing");
    assertEquals(19, reader.getPos(), "Wrong position after record read");
    assertEquals(3, value.getLength(), "Wrong length for record value");
    // should be at the end of the input
    assertFalse(reader.next(key, value));
    assertEquals(19, reader.getPos(), "Wrong position after record read");
    reader.close();

    inputData = "abcd|efgh|+|ij|kl|+|mno|pqr";
    inputFile = createInputFile(conf, inputData);
    delimiter = "|+|";
    recordDelimiterBytes = delimiter.getBytes(StandardCharsets.UTF_8);
    // walking over the buffer and split sizes checks for proper processing
    // of the ambiguous bytes of the delimiter
    for (int bufferSize = 1; bufferSize <= inputData.length(); bufferSize++) {
      for (int splitSize = 1; splitSize < inputData.length(); splitSize++) {
        conf.setInt("io.file.buffer.size", bufferSize);
        split = new FileSplit(inputFile, 0, bufferSize, (String[]) null);
        reader = new LineRecordReader(conf, split, recordDelimiterBytes);
        // Get first record: "abcd|efgh" always possible
        assertTrue(reader.next(key, value), "Expected record got nothing");
        assertThat(value.toString()).isEqualTo("abcd|efgh");
        assertEquals(9, value.getLength(), "Wrong position after record read");
        // Position should be 12 right after "|+|"
        int recordPos = 12;
        assertEquals(recordPos, reader.getPos(), "Wrong position after record read");
        // get the next record: "ij|kl" if the split/buffer allows it
        if (reader.next(key, value)) {
          // check the record info: "ij|kl"
          assertThat(value.toString()).isEqualTo("ij|kl");
          // Position should be 20 right after "|+|"
          recordPos = 20;
          assertEquals(recordPos, reader.getPos(), "Wrong position after record read");
        }
        // get the third record: "mno|pqr" if the split/buffer allows it
        if (reader.next(key, value)) {
          // check the record info: "mno|pqr"
          assertThat(value.toString()).isEqualTo("mno|pqr");
          // Position should be 27 at the end of the string now
          recordPos = inputData.length();
          assertEquals(recordPos, reader.getPos(), "Wrong position after record read");
        }
        // no more records can be read we should still be at the last position
        assertFalse(reader.next(key, value), "Unexpected record returned");
        assertEquals(recordPos, reader.getPos(), "Wrong position after record read");
        reader.close();
      }
    }
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
    LineRecordReader reader = new LineRecordReader(conf, split,
        null);
    LongWritable key = new LongWritable();
    Text value = new Text();
    reader.next(key, value);
    // Get first record:"1234567890"
    assertEquals(10, value.getLength());
    // Position should be 12 right after "1234567890\r\n"
    assertEquals(12, reader.getPos());
    reader.next(key, value);
    // Get second record:"12"
    assertEquals(2, value.getLength());
    // Position should be 16 right after "1234567890\r\n12\r\n"
    assertEquals(16, reader.getPos());
    assertFalse(reader.next(key, value));

    split = new FileSplit(inputFile, 15, 4, (String[])null);
    reader = new LineRecordReader(conf, split, null);
    // The second split dropped the first record "\n"
    // The position should be 16 right after "1234567890\r\n12\r\n"
    assertEquals(16, reader.getPos());
    reader.next(key, value);
    // Get third record:"345"
    assertEquals(3, value.getLength());
    // Position should be 19 right after "1234567890\r\n12\r\n345"
    assertEquals(19, reader.getPos());
    assertFalse(reader.next(key, value));
    assertEquals(19, reader.getPos());

    inputData = "123456789\r\r\n";
    inputFile = createInputFile(conf, inputData);
    split = new FileSplit(inputFile, 0, 12, (String[])null);
    reader = new LineRecordReader(conf, split, null);
    reader.next(key, value);
    // Get first record:"123456789"
    assertEquals(9, value.getLength());
    // Position should be 10 right after "123456789\r"
    assertEquals(10, reader.getPos());
    reader.next(key, value);
    // Get second record:""
    assertEquals(0, value.getLength());
    // Position should be 12 right after "123456789\r\r\n"
    assertEquals(12, reader.getPos());
    assertFalse(reader.next(key, value));
    assertEquals(12, reader.getPos());
  }

  @Test
  public void testBzipWithMultibyteDelimiter() throws IOException {
    String testFileName = "compressedMultibyteDelimiter.txt.bz2";
    // firstSplitLength < (headers + blockMarker) will pass always since no
    // records will be read (in the test file that is byte 0..9)
    // firstSplitlength > (compressed file length - one compressed block
    // size + 1) will also always pass since the second split will be empty
    // (833 bytes is the last block start in the used data file)
    int firstSplitLength = 100;
    URL testFileUrl = getClass().getClassLoader().getResource(testFileName);
    assertNotNull(testFileUrl, "Cannot find " + testFileName);
    File testFile = new File(testFileUrl.getFile());
    long testFileSize = testFile.length();
    Path testFilePath = new Path(testFile.getAbsolutePath());
    assertTrue(firstSplitLength > 9, "Split size is smaller than header length");
    assertTrue(testFileSize > firstSplitLength,
        "Split size is larger than compressed file size " +
        testFilePath);

    Configuration conf = new Configuration();
    conf.setInt(org.apache.hadoop.mapreduce.lib.input.
        LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);

    String delimiter = "<E-LINE>\r\r\n";
    conf.set("textinputformat.record.delimiter", delimiter);
    testSplitRecordsForFile(conf, firstSplitLength, testFileSize,
        testFilePath);
  }
}
