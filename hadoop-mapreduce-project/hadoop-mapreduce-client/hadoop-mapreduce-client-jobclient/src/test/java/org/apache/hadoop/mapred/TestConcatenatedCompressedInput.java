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

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.Inflater;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.ReflectionUtils;

import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
@Ignore
public class TestConcatenatedCompressedInput {
  private static final Log LOG =
    LogFactory.getLog(TestConcatenatedCompressedInput.class.getName());
  private static int MAX_LENGTH = 10000;
  private static JobConf defaultConf = new JobConf();
  private static FileSystem localFs = null;

  // from ~roelofs/ss30b-colors.hh
  final static String COLOR_RED        = "[0;31m";     // background doesn't matter...  "[0m"
  final static String COLOR_GREEN      = "[0;32m";     // background doesn't matter...  "[0m"
  final static String COLOR_YELLOW     = "[0;33;40m";  // DO force black background     "[0m"
  final static String COLOR_BLUE       = "[0;34m";     // do NOT force black background "[0m"
  final static String COLOR_MAGENTA    = "[0;35m";     // background doesn't matter...  "[0m"
  final static String COLOR_CYAN       = "[0;36m";     // background doesn't matter...  "[0m"
  final static String COLOR_WHITE      = "[0;37;40m";  // DO force black background     "[0m"
  final static String COLOR_BR_RED     = "[1;31m";     // background doesn't matter...  "[0m"
  final static String COLOR_BR_GREEN   = "[1;32m";     // background doesn't matter...  "[0m"
  final static String COLOR_BR_YELLOW  = "[1;33;40m";  // DO force black background     "[0m"
  final static String COLOR_BR_BLUE    = "[1;34m";     // do NOT force black background "[0m"
  final static String COLOR_BR_MAGENTA = "[1;35m";     // background doesn't matter...  "[0m"
  final static String COLOR_BR_CYAN    = "[1;36m";     // background doesn't matter...  "[0m"
  final static String COLOR_BR_WHITE   = "[1;37;40m";  // DO force black background     "[0m"
  final static String COLOR_NORMAL     = "[0m";

  static {
    try {
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }

  private static Path workDir =
    new Path(new Path(System.getProperty("test.build.data", "/tmp")),
             "TestConcatenatedCompressedInput").makeQualified(localFs);

  private static LineReader makeStream(String str) throws IOException {
    return new LineReader(new ByteArrayInputStream(str.getBytes("UTF-8")),
                          defaultConf);
  }

  private static void writeFile(FileSystem fs, Path name,
                                CompressionCodec codec, String contents)
  throws IOException {
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

  private static List<Text> readSplit(TextInputFormat format,
                                      InputSplit split, JobConf jobConf)
  throws IOException {
    List<Text> result = new ArrayList<Text>();
    RecordReader<LongWritable, Text> reader =
      format.getRecordReader(split, jobConf, voidReporter);
    LongWritable key = reader.createKey();
    Text value = reader.createValue();
    while (reader.next(key, value)) {
      result.add(value);
      value = reader.createValue();
    }
    reader.close();
    return result;
  }


  /**
   * Test using Hadoop's original, native-zlib gzip codec for reading.
   */
  @Test
  public void testGzip() throws IOException {
    JobConf jobConf = new JobConf(defaultConf);

    CompressionCodec gzip = new GzipCodec();
    ReflectionUtils.setConf(gzip, jobConf);
    localFs.delete(workDir, true);

    // preferred, but not compatible with Apache/trunk instance of Hudson:
/*
    assertFalse("[native (C/C++) codec]",
      (org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor.class ==
       gzip.getDecompressorType()) );
    System.out.println(COLOR_BR_RED +
      "testGzip() using native-zlib Decompressor (" +
      gzip.getDecompressorType() + ")" + COLOR_NORMAL);
 */

    // alternative:
    if (org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor.class ==
        gzip.getDecompressorType()) {
      System.out.println(COLOR_BR_RED +
        "testGzip() using native-zlib Decompressor (" +
        gzip.getDecompressorType() + ")" + COLOR_NORMAL);
    } else {
      LOG.warn("testGzip() skipped:  native (C/C++) libs not loaded");
      return;
    }

/*
 *      // THIS IS BUGGY: omits 2nd/3rd gzip headers; screws up 2nd/3rd CRCs--
 *      //                see https://issues.apache.org/jira/browse/HADOOP-6799
 *  Path fnHDFS = new Path(workDir, "concat" + gzip.getDefaultExtension());
 *  //OutputStream out = localFs.create(fnHDFS);
 *  //GzipCodec.GzipOutputStream gzOStm = new GzipCodec.GzipOutputStream(out);
 *      // can just combine those two lines, probably
 *  //GzipCodec.GzipOutputStream gzOStm =
 *  //  new GzipCodec.GzipOutputStream(localFs.create(fnHDFS));
 *      // oops, no:  this is a protected helper class; need to access
 *      //   it via createOutputStream() instead:
 *  OutputStream out = localFs.create(fnHDFS);
 *  Compressor gzCmp = gzip.createCompressor();
 *  CompressionOutputStream gzOStm = gzip.createOutputStream(out, gzCmp);
 *      // this SHOULD be going to HDFS:  got out from localFs == HDFS
 *      //   ...yup, works
 *  gzOStm.write("first gzip concat\n member\nwith three lines\n".getBytes());
 *  gzOStm.finish();
 *  gzOStm.resetState();
 *  gzOStm.write("2nd gzip concat member\n".getBytes());
 *  gzOStm.finish();
 *  gzOStm.resetState();
 *  gzOStm.write("gzip concat\nmember #3\n".getBytes());
 *  gzOStm.close();
 *      //
 *  String fn = "hdfs-to-local-concat" + gzip.getDefaultExtension();
 *  Path fnLocal = new Path(System.getProperty("test.concat.data","/tmp"), fn);
 *  localFs.copyToLocalFile(fnHDFS, fnLocal);
 */

    // copy prebuilt (correct!) version of concat.gz to HDFS
    final String fn = "concat" + gzip.getDefaultExtension();
    Path fnLocal = new Path(System.getProperty("test.concat.data", "/tmp"), fn);
    Path fnHDFS  = new Path(workDir, fn);
    localFs.copyFromLocalFile(fnLocal, fnHDFS);

    writeFile(localFs, new Path(workDir, "part2.txt.gz"), gzip,
              "this is a test\nof gzip\n");
    FileInputFormat.setInputPaths(jobConf, workDir);
    TextInputFormat format = new TextInputFormat();
    format.configure(jobConf);

    InputSplit[] splits = format.getSplits(jobConf, 100);
    assertEquals("compressed splits == 2", 2, splits.length);
    FileSplit tmp = (FileSplit) splits[0];
    if (tmp.getPath().getName().equals("part2.txt.gz")) {
      splits[0] = splits[1];
      splits[1] = tmp;
    }

    List<Text> results = readSplit(format, splits[0], jobConf);
    assertEquals("splits[0] num lines", 6, results.size());
    assertEquals("splits[0][5]", "member #3",
                 results.get(5).toString());

    results = readSplit(format, splits[1], jobConf);
    assertEquals("splits[1] num lines", 2, results.size());
    assertEquals("splits[1][0]", "this is a test",
                 results.get(0).toString());
    assertEquals("splits[1][1]", "of gzip",
                 results.get(1).toString());
  }

  /**
   * Test using the raw Inflater codec for reading gzip files.
   */
  @Test
  public void testPrototypeInflaterGzip() throws IOException {
    CompressionCodec gzip = new GzipCodec();  // used only for file extension
    localFs.delete(workDir, true);            // localFs = FileSystem instance

    System.out.println(COLOR_BR_BLUE + "testPrototypeInflaterGzip() using " +
      "non-native/Java Inflater and manual gzip header/trailer parsing" +
      COLOR_NORMAL);

    // copy prebuilt (correct!) version of concat.gz to HDFS
    final String fn = "concat" + gzip.getDefaultExtension();
    Path fnLocal = new Path(System.getProperty("test.concat.data", "/tmp"), fn);
    Path fnHDFS  = new Path(workDir, fn);
    localFs.copyFromLocalFile(fnLocal, fnHDFS);

    final FileInputStream in = new FileInputStream(fnLocal.toString());
    assertEquals("concat bytes available", 148, in.available());

    // should wrap all of this header-reading stuff in a running-CRC wrapper
    // (did so in BuiltInGzipDecompressor; see below)

    byte[] compressedBuf = new byte[256];
    int numBytesRead = in.read(compressedBuf, 0, 10);
    assertEquals("header bytes read", 10, numBytesRead);
    assertEquals("1st byte", 0x1f, compressedBuf[0] & 0xff);
    assertEquals("2nd byte", 0x8b, compressedBuf[1] & 0xff);
    assertEquals("3rd byte (compression method)", 8, compressedBuf[2] & 0xff);

    byte flags = (byte)(compressedBuf[3] & 0xff);
    if ((flags & 0x04) != 0) {   // FEXTRA
      numBytesRead = in.read(compressedBuf, 0, 2);
      assertEquals("XLEN bytes read", 2, numBytesRead);
      int xlen = ((compressedBuf[1] << 8) | compressedBuf[0]) & 0xffff;
      in.skip(xlen);
    }
    if ((flags & 0x08) != 0) {   // FNAME
      while ((numBytesRead = in.read()) != 0) {
        assertFalse("unexpected end-of-file while reading filename",
                    numBytesRead == -1);
      }
    }
    if ((flags & 0x10) != 0) {   // FCOMMENT
      while ((numBytesRead = in.read()) != 0) {
        assertFalse("unexpected end-of-file while reading comment",
                    numBytesRead == -1);
      }
    }
    if ((flags & 0xe0) != 0) {   // reserved
      assertTrue("reserved bits are set??", (flags & 0xe0) == 0);
    }
    if ((flags & 0x02) != 0) {   // FHCRC
      numBytesRead = in.read(compressedBuf, 0, 2);
      assertEquals("CRC16 bytes read", 2, numBytesRead);
      int crc16 = ((compressedBuf[1] << 8) | compressedBuf[0]) & 0xffff;
    }

    // ready to go!  next bytes should be start of deflated stream, suitable
    // for Inflater
    numBytesRead = in.read(compressedBuf);

    // Inflater docs refer to a "dummy byte":  no clue what that's about;
    // appears to work fine without one
    byte[] uncompressedBuf = new byte[256];
    Inflater inflater = new Inflater(true);

    inflater.setInput(compressedBuf, 0, numBytesRead);
    try {
      int numBytesUncompressed = inflater.inflate(uncompressedBuf);
      String outString =
        new String(uncompressedBuf, 0, numBytesUncompressed, "UTF-8");
      System.out.println("uncompressed data of first gzip member = [" +
                         outString + "]");
    } catch (java.util.zip.DataFormatException ex) {
      throw new IOException(ex.getMessage());
    }

    in.close();
  }

  /**
   * Test using the new BuiltInGzipDecompressor codec for reading gzip files.
   */
  // NOTE:  This fails on RHEL4 with "java.io.IOException: header crc mismatch"
  //        due to buggy version of zlib (1.2.1.2) included.
  @Test
  public void testBuiltInGzipDecompressor() throws IOException {
    JobConf jobConf = new JobConf(defaultConf);
    jobConf.setBoolean("io.native.lib.available", false);

    CompressionCodec gzip = new GzipCodec();
    ReflectionUtils.setConf(gzip, jobConf);
    localFs.delete(workDir, true);

    assertEquals("[non-native (Java) codec]",
      org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor.class,
      gzip.getDecompressorType());
    System.out.println(COLOR_BR_YELLOW + "testBuiltInGzipDecompressor() using" +
      " non-native (Java Inflater) Decompressor (" + gzip.getDecompressorType()
      + ")" + COLOR_NORMAL);

    // copy single-member test file to HDFS
    String fn1 = "testConcatThenCompress.txt" + gzip.getDefaultExtension();
    Path fnLocal1 = new Path(System.getProperty("test.concat.data","/tmp"),fn1);
    Path fnHDFS1  = new Path(workDir, fn1);
    localFs.copyFromLocalFile(fnLocal1, fnHDFS1);

    // copy multiple-member test file to HDFS
    // (actually in "seekable gzip" format, a la JIRA PIG-42)
    String fn2 = "testCompressThenConcat.txt" + gzip.getDefaultExtension();
    Path fnLocal2 = new Path(System.getProperty("test.concat.data","/tmp"),fn2);
    Path fnHDFS2  = new Path(workDir, fn2);
    localFs.copyFromLocalFile(fnLocal2, fnHDFS2);

    FileInputFormat.setInputPaths(jobConf, workDir);

    // here's first pair of DecompressorStreams:
    final FileInputStream in1 = new FileInputStream(fnLocal1.toString());
    final FileInputStream in2 = new FileInputStream(fnLocal2.toString());
    assertEquals("concat bytes available", 2734, in1.available());
    assertEquals("concat bytes available", 3413, in2.available()); // w/hdr CRC

    CompressionInputStream cin2 = gzip.createInputStream(in2);
    LineReader in = new LineReader(cin2);
    Text out = new Text();

    int numBytes, totalBytes=0, lineNum=0;
    while ((numBytes = in.readLine(out)) > 0) {
      ++lineNum;
      totalBytes += numBytes;
    }
    in.close();
    assertEquals("total uncompressed bytes in concatenated test file",
                 5346, totalBytes);
    assertEquals("total uncompressed lines in concatenated test file",
                 84, lineNum);

    // test BuiltInGzipDecompressor with lots of different input-buffer sizes
    doMultipleGzipBufferSizes(jobConf, false);

    // test GzipZlibDecompressor (native), just to be sure
    // (FIXME?  could move this call to testGzip(), but would need filename
    // setup above) (alternatively, maybe just nuke testGzip() and extend this?)
    doMultipleGzipBufferSizes(jobConf, true);
  }

  // this tests either the native or the non-native gzip decoder with 43
  // input-buffer sizes in order to try to catch any parser/state-machine
  // errors at buffer boundaries
  private static void doMultipleGzipBufferSizes(JobConf jConf,
                                                boolean useNative)
  throws IOException {
    System.out.println(COLOR_YELLOW + "doMultipleGzipBufferSizes() using " +
      (useNative? "GzipZlibDecompressor" : "BuiltInGzipDecompressor") +
      COLOR_NORMAL);

    jConf.setBoolean("io.native.lib.available", useNative);

    int bufferSize;

    // ideally would add some offsets/shifts in here (e.g., via extra fields
    // of various sizes), but...significant work to hand-generate each header
    for (bufferSize = 1; bufferSize < 34; ++bufferSize) {
      jConf.setInt("io.file.buffer.size", bufferSize);
      doSingleGzipBufferSize(jConf);
    }

    bufferSize = 512;
    jConf.setInt("io.file.buffer.size", bufferSize);
    doSingleGzipBufferSize(jConf);

    bufferSize = 1024;
    jConf.setInt("io.file.buffer.size", bufferSize);
    doSingleGzipBufferSize(jConf);

    bufferSize = 2*1024;
    jConf.setInt("io.file.buffer.size", bufferSize);
    doSingleGzipBufferSize(jConf);

    bufferSize = 4*1024;
    jConf.setInt("io.file.buffer.size", bufferSize);
    doSingleGzipBufferSize(jConf);

    bufferSize = 63*1024;
    jConf.setInt("io.file.buffer.size", bufferSize);
    doSingleGzipBufferSize(jConf);

    bufferSize = 64*1024;
    jConf.setInt("io.file.buffer.size", bufferSize);
    doSingleGzipBufferSize(jConf);

    bufferSize = 65*1024;
    jConf.setInt("io.file.buffer.size", bufferSize);
    doSingleGzipBufferSize(jConf);

    bufferSize = 127*1024;
    jConf.setInt("io.file.buffer.size", bufferSize);
    doSingleGzipBufferSize(jConf);

    bufferSize = 128*1024;
    jConf.setInt("io.file.buffer.size", bufferSize);
    doSingleGzipBufferSize(jConf);

    bufferSize = 129*1024;
    jConf.setInt("io.file.buffer.size", bufferSize);
    doSingleGzipBufferSize(jConf);
  }

  // this tests both files (testCompressThenConcat, testConcatThenCompress);
  // all should work with either native zlib or new Inflater-based decoder
  private static void doSingleGzipBufferSize(JobConf jConf) throws IOException {

    TextInputFormat format = new TextInputFormat();
    format.configure(jConf);

    // here's Nth pair of DecompressorStreams:
    InputSplit[] splits = format.getSplits(jConf, 100);
    assertEquals("compressed splits == 2", 2, splits.length);
    FileSplit tmp = (FileSplit) splits[0];
    if (tmp.getPath().getName().equals("testCompressThenConcat.txt.gz")) {
      System.out.println("  (swapping)");
      splits[0] = splits[1];
      splits[1] = tmp;
    }

    List<Text> results = readSplit(format, splits[0], jConf);
    assertEquals("splits[0] length (num lines)", 84, results.size());
    assertEquals("splits[0][0]",
      "Call me Ishmael. Some years ago--never mind how long precisely--having",
      results.get(0).toString());
    assertEquals("splits[0][42]",
      "Tell me, does the magnetic virtue of the needles of the compasses of",
      results.get(42).toString());

    results = readSplit(format, splits[1], jConf);
    assertEquals("splits[1] length (num lines)", 84, results.size());
    assertEquals("splits[1][0]",
      "Call me Ishmael. Some years ago--never mind how long precisely--having",
      results.get(0).toString());
    assertEquals("splits[1][42]",
      "Tell me, does the magnetic virtue of the needles of the compasses of",
      results.get(42).toString());
  }

  /**
   * Test using the bzip2 codec for reading
   */
  @Test
  public void testBzip2() throws IOException {
    JobConf jobConf = new JobConf(defaultConf);

    CompressionCodec bzip2 = new BZip2Codec();
    ReflectionUtils.setConf(bzip2, jobConf);
    localFs.delete(workDir, true);

    System.out.println(COLOR_BR_CYAN +
      "testBzip2() using non-native CBZip2InputStream (presumably)" +
      COLOR_NORMAL);

    // copy prebuilt (correct!) version of concat.bz2 to HDFS
    final String fn = "concat" + bzip2.getDefaultExtension();
    Path fnLocal = new Path(System.getProperty("test.concat.data", "/tmp"), fn);
    Path fnHDFS  = new Path(workDir, fn);
    localFs.copyFromLocalFile(fnLocal, fnHDFS);

    writeFile(localFs, new Path(workDir, "part2.txt.bz2"), bzip2,
              "this is a test\nof bzip2\n");
    FileInputFormat.setInputPaths(jobConf, workDir);
    TextInputFormat format = new TextInputFormat();  // extends FileInputFormat
    format.configure(jobConf);
    format.setMinSplitSize(256);  // work around 2-byte splits issue
    // [135 splits for a 208-byte file and a 62-byte file(!)]

    InputSplit[] splits = format.getSplits(jobConf, 100);
    assertEquals("compressed splits == 2", 2, splits.length);
    FileSplit tmp = (FileSplit) splits[0];
    if (tmp.getPath().getName().equals("part2.txt.bz2")) {
      splits[0] = splits[1];
      splits[1] = tmp;
    }

    List<Text> results = readSplit(format, splits[0], jobConf);
    assertEquals("splits[0] num lines", 6, results.size());
    assertEquals("splits[0][5]", "member #3",
                 results.get(5).toString());

    results = readSplit(format, splits[1], jobConf);
    assertEquals("splits[1] num lines", 2, results.size());
    assertEquals("splits[1][0]", "this is a test",
                 results.get(0).toString());
    assertEquals("splits[1][1]", "of bzip2",
                 results.get(1).toString());
  }

  /**
   * Extended bzip2 test, similar to BuiltInGzipDecompressor test above.
   */
  @Test
  public void testMoreBzip2() throws IOException {
    JobConf jobConf = new JobConf(defaultConf);

    CompressionCodec bzip2 = new BZip2Codec();
    ReflectionUtils.setConf(bzip2, jobConf);
    localFs.delete(workDir, true);

    System.out.println(COLOR_BR_MAGENTA +
      "testMoreBzip2() using non-native CBZip2InputStream (presumably)" +
      COLOR_NORMAL);

    // copy single-member test file to HDFS
    String fn1 = "testConcatThenCompress.txt" + bzip2.getDefaultExtension();
    Path fnLocal1 = new Path(System.getProperty("test.concat.data","/tmp"),fn1);
    Path fnHDFS1  = new Path(workDir, fn1);
    localFs.copyFromLocalFile(fnLocal1, fnHDFS1);

    // copy multiple-member test file to HDFS
    String fn2 = "testCompressThenConcat.txt" + bzip2.getDefaultExtension();
    Path fnLocal2 = new Path(System.getProperty("test.concat.data","/tmp"),fn2);
    Path fnHDFS2  = new Path(workDir, fn2);
    localFs.copyFromLocalFile(fnLocal2, fnHDFS2);

    FileInputFormat.setInputPaths(jobConf, workDir);

    // here's first pair of BlockDecompressorStreams:
    final FileInputStream in1 = new FileInputStream(fnLocal1.toString());
    final FileInputStream in2 = new FileInputStream(fnLocal2.toString());
    assertEquals("concat bytes available", 2567, in1.available());
    assertEquals("concat bytes available", 3056, in2.available());

/*
    // FIXME
    // The while-loop below dies at the beginning of the 2nd concatenated
    // member (after 17 lines successfully read) with:
    //
    //   java.io.IOException: bad block header
    //   at org.apache.hadoop.io.compress.bzip2.CBZip2InputStream.initBlock(
    //   CBZip2InputStream.java:527)
    //
    // It is not critical to concatenated-gzip support, HADOOP-6835, so it's
    // simply commented out for now (and HADOOP-6852 filed).  If and when the
    // latter issue is resolved--perhaps by fixing an error here--this code
    // should be reenabled.  Note that the doMultipleBzip2BufferSizes() test
    // below uses the same testCompressThenConcat.txt.bz2 file but works fine.

    CompressionInputStream cin2 = bzip2.createInputStream(in2);
    LineReader in = new LineReader(cin2);
    Text out = new Text();

    int numBytes, totalBytes=0, lineNum=0;
    while ((numBytes = in.readLine(out)) > 0) {
      ++lineNum;
      totalBytes += numBytes;
    }
    in.close();
    assertEquals("total uncompressed bytes in concatenated test file",
                 5346, totalBytes);
    assertEquals("total uncompressed lines in concatenated test file",
                 84, lineNum);
 */

    // test CBZip2InputStream with lots of different input-buffer sizes
    doMultipleBzip2BufferSizes(jobConf, false);

    // no native version of bzip2 codec (yet?)
    //doMultipleBzip2BufferSizes(jobConf, true);
  }

  // this tests either the native or the non-native gzip decoder with more than
  // three dozen input-buffer sizes in order to try to catch any parser/state-
  // machine errors at buffer boundaries
  private static void doMultipleBzip2BufferSizes(JobConf jConf,
                                                boolean useNative)
  throws IOException {
    System.out.println(COLOR_MAGENTA + "doMultipleBzip2BufferSizes() using " +
      "default bzip2 decompressor" + COLOR_NORMAL);

    jConf.setBoolean("io.native.lib.available", useNative);

    int bufferSize;

    // ideally would add some offsets/shifts in here (e.g., via extra header
    // data?), but...significant work to hand-generate each header, and no
    // bzip2 spec for reference
    for (bufferSize = 1; bufferSize < 34; ++bufferSize) {
      jConf.setInt("io.file.buffer.size", bufferSize);
      doSingleBzip2BufferSize(jConf);
    }

    bufferSize = 512;
    jConf.setInt("io.file.buffer.size", bufferSize);
    doSingleBzip2BufferSize(jConf);

    bufferSize = 1024;
    jConf.setInt("io.file.buffer.size", bufferSize);
    doSingleBzip2BufferSize(jConf);

    bufferSize = 2*1024;
    jConf.setInt("io.file.buffer.size", bufferSize);
    doSingleBzip2BufferSize(jConf);

    bufferSize = 4*1024;
    jConf.setInt("io.file.buffer.size", bufferSize);
    doSingleBzip2BufferSize(jConf);

    bufferSize = 63*1024;
    jConf.setInt("io.file.buffer.size", bufferSize);
    doSingleBzip2BufferSize(jConf);

    bufferSize = 64*1024;
    jConf.setInt("io.file.buffer.size", bufferSize);
    doSingleBzip2BufferSize(jConf);

    bufferSize = 65*1024;
    jConf.setInt("io.file.buffer.size", bufferSize);
    doSingleBzip2BufferSize(jConf);

    bufferSize = 127*1024;
    jConf.setInt("io.file.buffer.size", bufferSize);
    doSingleBzip2BufferSize(jConf);

    bufferSize = 128*1024;
    jConf.setInt("io.file.buffer.size", bufferSize);
    doSingleBzip2BufferSize(jConf);

    bufferSize = 129*1024;
    jConf.setInt("io.file.buffer.size", bufferSize);
    doSingleBzip2BufferSize(jConf);
  }

  // this tests both files (testCompressThenConcat, testConcatThenCompress); all
  // should work with existing Java bzip2 decoder and any future native version
  private static void doSingleBzip2BufferSize(JobConf jConf) throws IOException {
    TextInputFormat format = new TextInputFormat();
    format.configure(jConf);
    format.setMinSplitSize(5500);  // work around 256-byte/22-splits issue

    // here's Nth pair of DecompressorStreams:
    InputSplit[] splits = format.getSplits(jConf, 100);
    assertEquals("compressed splits == 2", 2, splits.length);
    FileSplit tmp = (FileSplit) splits[0];
    if (tmp.getPath().getName().equals("testCompressThenConcat.txt.gz")) {
      System.out.println("  (swapping)");
      splits[0] = splits[1];
      splits[1] = tmp;
    }

    // testConcatThenCompress (single)
    List<Text> results = readSplit(format, splits[0], jConf);
    assertEquals("splits[0] length (num lines)", 84, results.size());
    assertEquals("splits[0][0]",
      "Call me Ishmael. Some years ago--never mind how long precisely--having",
      results.get(0).toString());
    assertEquals("splits[0][42]",
      "Tell me, does the magnetic virtue of the needles of the compasses of",
      results.get(42).toString());

    // testCompressThenConcat (multi)
    results = readSplit(format, splits[1], jConf);
    assertEquals("splits[1] length (num lines)", 84, results.size());
    assertEquals("splits[1][0]",
      "Call me Ishmael. Some years ago--never mind how long precisely--having",
      results.get(0).toString());
    assertEquals("splits[1][42]",
      "Tell me, does the magnetic virtue of the needles of the compasses of",
      results.get(42).toString());
  }

  private static String unquote(String in) {
    StringBuffer result = new StringBuffer();
    for(int i=0; i < in.length(); ++i) {
      char ch = in.charAt(i);
      if (ch == '\\') {
        ch = in.charAt(++i);
        switch (ch) {
        case 'n':
          result.append('\n');
          break;
        case 'r':
          result.append('\r');
          break;
        default:
          result.append(ch);
          break;
        }
      } else {
        result.append(ch);
      }
    }
    return result.toString();
  }

  /**
   * Parse the command line arguments into lines and display the result.
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    for(String arg: args) {
      System.out.println("Working on " + arg);
      LineReader reader = makeStream(unquote(arg));
      Text line = new Text();
      int size = reader.readLine(line);
      while (size > 0) {
        System.out.println("Got: " + line.toString());
        size = reader.readLine(line);
      }
      reader.close();
    }
  }
}
