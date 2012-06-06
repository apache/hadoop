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
package org.apache.hadoop.mapred.gridmix;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapred.gridmix.CompressionEmulationUtil.RandomTextDataMapper;
import org.apache.hadoop.mapred.gridmix.GenerateData.GenSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static org.junit.Assert.*;
import org.junit.Test;

/**
 * Test {@link CompressionEmulationUtil}
 */
public class TestCompressionEmulationUtils {
  //TODO Remove this once LocalJobRunner can run Gridmix.
  static class CustomInputFormat extends GenerateData.GenDataFormat {
    @Override
    public List<InputSplit> getSplits(JobContext jobCtxt) throws IOException {
      // get the total data to be generated
      long toGen =
        jobCtxt.getConfiguration().getLong(GenerateData.GRIDMIX_GEN_BYTES, -1);
      if (toGen < 0) {
        throw new IOException("Invalid/missing generation bytes: " + toGen);
      }
      // get the total number of mappers configured
      int totalMappersConfigured =
        jobCtxt.getConfiguration().getInt(MRJobConfig.NUM_MAPS, -1);
      if (totalMappersConfigured < 0) {
        throw new IOException("Invalid/missing num mappers: " 
                              + totalMappersConfigured);
      }
      
      final long bytesPerTracker = toGen / totalMappersConfigured;
      final ArrayList<InputSplit> splits = 
        new ArrayList<InputSplit>(totalMappersConfigured);
      for (int i = 0; i < totalMappersConfigured; ++i) {
        splits.add(new GenSplit(bytesPerTracker, 
                   new String[] { "tracker_local" }));
      }
      return splits;
    }
  }
  
  /**
   * Test {@link RandomTextDataMapper} via {@link CompressionEmulationUtil}.
   */
  @Test
  public void testRandomCompressedTextDataGenerator() throws Exception {
    int wordSize = 10;
    int listSize = 20;
    long dataSize = 10*1024*1024;
    
    Configuration conf = new Configuration();
    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, true);
    CompressionEmulationUtil.setInputCompressionEmulationEnabled(conf, true);
    
    // configure the RandomTextDataGenerator to generate desired sized data
    conf.setInt(RandomTextDataGenerator.GRIDMIX_DATAGEN_RANDOMTEXT_LISTSIZE, 
                listSize);
    conf.setInt(RandomTextDataGenerator.GRIDMIX_DATAGEN_RANDOMTEXT_WORDSIZE, 
                wordSize);
    conf.setLong(GenerateData.GRIDMIX_GEN_BYTES, dataSize);
    conf.set("mapreduce.job.hdfs-servers", "");
    
    FileSystem lfs = FileSystem.getLocal(conf);
    
    // define the test's root temp directory
    Path rootTempDir =
        new Path(System.getProperty("test.build.data", "/tmp")).makeQualified(
            lfs.getUri(), lfs.getWorkingDirectory());

    Path tempDir = new Path(rootTempDir, "TestRandomCompressedTextDataGenr");
    lfs.delete(tempDir, true);
    
    runDataGenJob(conf, tempDir);
    
    // validate the output data
    FileStatus[] files = 
      lfs.listStatus(tempDir, new Utils.OutputFileUtils.OutputFilesFilter());
    long size = 0;
    long maxLineSize = 0;
    
    for (FileStatus status : files) {
      InputStream in = 
        CompressionEmulationUtil
          .getPossiblyDecompressedInputStream(status.getPath(), conf, 0);
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      String line = reader.readLine();
      if (line != null) {
        long lineSize = line.getBytes().length;
        if (lineSize > maxLineSize) {
          maxLineSize = lineSize;
        }
        while (line != null) {
          for (String word : line.split("\\s")) {
            size += word.getBytes().length;
          }
          line = reader.readLine();
        }
      }
      reader.close();
    }

    assertTrue(size >= dataSize);
    assertTrue(size <= dataSize + maxLineSize);
  }
  
  /**
   * Runs a GridMix data-generation job.
   */
  private static void runDataGenJob(Configuration conf, Path tempDir) 
  throws IOException, ClassNotFoundException, InterruptedException {
    JobClient client = new JobClient(conf);
    
    // get the local job runner
    conf.setInt(MRJobConfig.NUM_MAPS, 1);
    
    Job job = new Job(conf);
    
    CompressionEmulationUtil.configure(job);
    job.setInputFormatClass(CustomInputFormat.class);
    
    // set the output path
    FileOutputFormat.setOutputPath(job, tempDir);
    
    // submit and wait for completion
    job.submit();
    int ret = job.waitForCompletion(true) ? 0 : 1;

    assertEquals("Job Failed", 0, ret);
  }
  
  /**
   * Test if {@link RandomTextDataGenerator} can generate random text data 
   * with the desired compression ratio. This involves
   *   - using {@link CompressionEmulationUtil} to configure the MR job for 
   *     generating the random text data with the desired compression ratio
   *   - running the MR job
   *   - test {@link RandomTextDataGenerator}'s output and match the output size
   *     (compressed) with the expected compression ratio.
   */
  private void testCompressionRatioConfigure(float ratio)
  throws Exception {
    long dataSize = 10*1024*1024;
    
    Configuration conf = new Configuration();
    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, true);
    CompressionEmulationUtil.setInputCompressionEmulationEnabled(conf, true);
    
    conf.setLong(GenerateData.GRIDMIX_GEN_BYTES, dataSize);
    conf.set("mapreduce.job.hdfs-servers", "");
    
    float expectedRatio = CompressionEmulationUtil.DEFAULT_COMPRESSION_RATIO;
    if (ratio > 0) {
      // set the compression ratio in the conf
      CompressionEmulationUtil.setMapInputCompressionEmulationRatio(conf, ratio);
      expectedRatio = 
        CompressionEmulationUtil.standardizeCompressionRatio(ratio);
    }
    
    // invoke the utility to map from ratio to word-size
    CompressionEmulationUtil.setupDataGeneratorConfig(conf);
    
    FileSystem lfs = FileSystem.getLocal(conf);
    
    // define the test's root temp directory
    Path rootTempDir =
        new Path(System.getProperty("test.build.data", "/tmp")).makeQualified(
            lfs.getUri(), lfs.getWorkingDirectory());

    Path tempDir = 
      new Path(rootTempDir, "TestCustomRandomCompressedTextDataGenr");
    lfs.delete(tempDir, true);
    
    runDataGenJob(conf, tempDir);
    
    // validate the output data
    FileStatus[] files = 
      lfs.listStatus(tempDir, new Utils.OutputFileUtils.OutputFilesFilter());
    long size = 0;
    
    for (FileStatus status : files) {
      size += status.getLen();
    }

    float compressionRatio = ((float)size)/dataSize;
    float stdRatio = 
      CompressionEmulationUtil.standardizeCompressionRatio(compressionRatio);
    
    assertEquals(expectedRatio, stdRatio, 0.0D);
  }
  
  /**
   * Test compression ratio with multiple compression ratios.
   */
  @Test
  public void testCompressionRatios() throws Exception {
    // test default compression ratio i.e 0.5
    testCompressionRatioConfigure(0F);
    // test for a sample compression ratio of 0.2
    testCompressionRatioConfigure(0.2F);
    // test for a sample compression ratio of 0.4
    testCompressionRatioConfigure(0.4F);
    // test for a sample compression ratio of 0.65
    testCompressionRatioConfigure(0.65F);
    // test for a compression ratio of 0.682 which should be standardized
    // to round(0.682) i.e 0.68
    testCompressionRatioConfigure(0.682F);
    // test for a compression ratio of 0.567 which should be standardized
    // to round(0.567) i.e 0.57
    testCompressionRatioConfigure(0.567F);
    
    // test with a compression ratio of 0.01 which less than the min supported
    // value of 0.07
    boolean failed = false;
    try {
      testCompressionRatioConfigure(0.01F);
    } catch (RuntimeException re) {
      failed = true;
    }
    assertTrue("Compression ratio min value (0.07) check failed!", failed);
    
    // test with a compression ratio of 0.01 which less than the max supported
    // value of 0.68
    failed = false;
    try {
      testCompressionRatioConfigure(0.7F);
    } catch (RuntimeException re) {
      failed = true;
    }
    assertTrue("Compression ratio max value (0.68) check failed!", failed);
  }
  
  /**
   * Test compression ratio standardization.
   */
  @Test
  public void testCompressionRatioStandardization() throws Exception {
    assertEquals(0.55F, 
        CompressionEmulationUtil.standardizeCompressionRatio(0.55F), 0.0D);
    assertEquals(0.65F, 
        CompressionEmulationUtil.standardizeCompressionRatio(0.652F), 0.0D);
    assertEquals(0.78F, 
        CompressionEmulationUtil.standardizeCompressionRatio(0.777F), 0.0D);
    assertEquals(0.86F, 
        CompressionEmulationUtil.standardizeCompressionRatio(0.855F), 0.0D);
  }
  
  /**
   * Test map input compression ratio configuration utilities.
   */
  @Test
  public void testInputCompressionRatioConfiguration() throws Exception {
    Configuration conf = new Configuration();
    float ratio = 0.567F;
    CompressionEmulationUtil.setMapInputCompressionEmulationRatio(conf, ratio);
    assertEquals(ratio, 
        CompressionEmulationUtil.getMapInputCompressionEmulationRatio(conf), 
        0.0D);
  }
  
  /**
   * Test map output compression ratio configuration utilities.
   */
  @Test
  public void testIntermediateCompressionRatioConfiguration() 
  throws Exception {
    Configuration conf = new Configuration();
    float ratio = 0.567F;
    CompressionEmulationUtil.setMapOutputCompressionEmulationRatio(conf, ratio);
    assertEquals(ratio, 
        CompressionEmulationUtil.getMapOutputCompressionEmulationRatio(conf), 
        0.0D);
  }
  
  /**
   * Test reduce output compression ratio configuration utilities.
   */
  @Test
  public void testOutputCompressionRatioConfiguration() throws Exception {
    Configuration conf = new Configuration();
    float ratio = 0.567F;
    CompressionEmulationUtil.setJobOutputCompressionEmulationRatio(conf, ratio);
    assertEquals(ratio, 
        CompressionEmulationUtil.getJobOutputCompressionEmulationRatio(conf),
        0.0D);
  }
  
  /**
   * Test compressible {@link GridmixRecord}.
   */
  @Test
  public void testCompressibleGridmixRecord() throws IOException {
    JobConf conf = new JobConf();
    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, true);
    CompressionEmulationUtil.setInputCompressionEmulationEnabled(conf, true);
    
    FileSystem lfs = FileSystem.getLocal(conf);
    int dataSize = 1024 * 1024 * 10; // 10 MB
    float ratio = 0.357F;
    
    // define the test's root temp directory
    Path rootTempDir =
        new Path(System.getProperty("test.build.data", "/tmp")).makeQualified(
            lfs.getUri(), lfs.getWorkingDirectory());

    Path tempDir = new Path(rootTempDir, 
                            "TestPossiblyCompressibleGridmixRecord");
    lfs.delete(tempDir, true);
    
    // define a compressible GridmixRecord
    GridmixRecord record = new GridmixRecord(dataSize, 0);
    record.setCompressibility(true, ratio); // enable compression
    
    conf.setClass(FileOutputFormat.COMPRESS_CODEC, GzipCodec.class, 
                  CompressionCodec.class);
    org.apache.hadoop.mapred.FileOutputFormat.setCompressOutput(conf, true);
    
    // write the record to a file
    Path recordFile = new Path(tempDir, "record");
    OutputStream outStream = CompressionEmulationUtil
                               .getPossiblyCompressedOutputStream(recordFile, 
                                                                  conf);    
    DataOutputStream out = new DataOutputStream(outStream);
    record.write(out);
    out.close();
    outStream.close();
    
    // open the compressed stream for reading
    Path actualRecordFile = recordFile.suffix(".gz");
    InputStream in = 
      CompressionEmulationUtil
        .getPossiblyDecompressedInputStream(actualRecordFile, conf, 0);
    
    // get the compressed file size
    long compressedFileSize = lfs.listStatus(actualRecordFile)[0].getLen();
    
    GridmixRecord recordRead = new GridmixRecord();
    recordRead.readFields(new DataInputStream(in));
    
    assertEquals("Record size mismatch in a compressible GridmixRecord",
                 dataSize, recordRead.getSize());
    assertTrue("Failed to generate a compressible GridmixRecord",
               recordRead.getSize() > compressedFileSize);
    
    // check if the record can generate data with the desired compression ratio
    float seenRatio = ((float)compressedFileSize)/dataSize;
    assertEquals(CompressionEmulationUtil.standardizeCompressionRatio(ratio), 
        CompressionEmulationUtil.standardizeCompressionRatio(seenRatio), 1.0D);
  }
  
  /**
   * Test 
   * {@link CompressionEmulationUtil#isCompressionEmulationEnabled(
   *          org.apache.hadoop.conf.Configuration)}.
   */
  @Test
  public void testIsCompressionEmulationEnabled() {
    Configuration conf = new Configuration();
    // Check default values
    assertTrue(CompressionEmulationUtil.isCompressionEmulationEnabled(conf));
    
    // Check disabled
    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, false);
    assertFalse(CompressionEmulationUtil.isCompressionEmulationEnabled(conf));
    
    // Check enabled
    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, true);
    assertTrue(CompressionEmulationUtil.isCompressionEmulationEnabled(conf));
  }
  
  /**
   * Test 
   * {@link CompressionEmulationUtil#getPossiblyDecompressedInputStream(Path, 
   *                                   Configuration, long)}
   *  and
   *  {@link CompressionEmulationUtil#getPossiblyCompressedOutputStream(Path, 
   *                                    Configuration)}.
   */
  @Test
  public void testPossiblyCompressedDecompressedStreams() throws IOException {
    JobConf conf = new JobConf();
    FileSystem lfs = FileSystem.getLocal(conf);
    String inputLine = "Hi Hello!";

    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, true);
    CompressionEmulationUtil.setInputCompressionEmulationEnabled(conf, true);
    conf.setBoolean(FileOutputFormat.COMPRESS, true);
    conf.setClass(FileOutputFormat.COMPRESS_CODEC, GzipCodec.class, 
                  CompressionCodec.class);

    // define the test's root temp directory
    Path rootTempDir =
        new Path(System.getProperty("test.build.data", "/tmp")).makeQualified(
            lfs.getUri(), lfs.getWorkingDirectory());

    Path tempDir =
      new Path(rootTempDir, "TestPossiblyCompressedDecompressedStreams");
    lfs.delete(tempDir, true);

    // create a compressed file
    Path compressedFile = new Path(tempDir, "test");
    OutputStream out = 
      CompressionEmulationUtil.getPossiblyCompressedOutputStream(compressedFile, 
                                                                 conf);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
    writer.write(inputLine);
    writer.close();
    
    // now read back the data from the compressed stream
    compressedFile = compressedFile.suffix(".gz");
    InputStream in = 
      CompressionEmulationUtil
        .getPossiblyDecompressedInputStream(compressedFile, conf, 0);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    String readLine = reader.readLine();
    assertEquals("Compression/Decompression error", inputLine, readLine);
    reader.close();
  }
  
  /**
   * Test if 
   * {@link CompressionEmulationUtil#configureCompressionEmulation(
   *        org.apache.hadoop.mapred.JobConf, org.apache.hadoop.mapred.JobConf)}
   *  can extract compression related configuration parameters.
   */
  @Test
  public void testExtractCompressionConfigs() {
    JobConf source = new JobConf();
    JobConf target = new JobConf();
    
    // set the default values
    source.setBoolean(FileOutputFormat.COMPRESS, false);
    source.set(FileOutputFormat.COMPRESS_CODEC, "MyDefaultCodec");
    source.set(FileOutputFormat.COMPRESS_TYPE, "MyDefaultType");
    source.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, false); 
    source.set(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, "MyDefaultCodec2");
    
    CompressionEmulationUtil.configureCompressionEmulation(source, target);
    
    // check default values
    assertFalse(target.getBoolean(FileOutputFormat.COMPRESS, true));
    assertEquals("MyDefaultCodec", target.get(FileOutputFormat.COMPRESS_CODEC));
    assertEquals("MyDefaultType", target.get(FileOutputFormat.COMPRESS_TYPE));
    assertFalse(target.getBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true));
    assertEquals("MyDefaultCodec2", 
                 target.get(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC));
    assertFalse(CompressionEmulationUtil
                .isInputCompressionEmulationEnabled(target));
    
    // set new values
    source.setBoolean(FileOutputFormat.COMPRESS, true);
    source.set(FileOutputFormat.COMPRESS_CODEC, "MyCodec");
    source.set(FileOutputFormat.COMPRESS_TYPE, "MyType");
    source.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true); 
    source.set(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, "MyCodec2");
    org.apache.hadoop.mapred.FileInputFormat.setInputPaths(source, "file.gz");
    
    target = new JobConf(); // reset
    CompressionEmulationUtil.configureCompressionEmulation(source, target);
    
    // check new values
    assertTrue(target.getBoolean(FileOutputFormat.COMPRESS, false));
    assertEquals("MyCodec", target.get(FileOutputFormat.COMPRESS_CODEC));
    assertEquals("MyType", target.get(FileOutputFormat.COMPRESS_TYPE));
    assertTrue(target.getBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, false));
    assertEquals("MyCodec2", 
                 target.get(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC));
    assertTrue(CompressionEmulationUtil
               .isInputCompressionEmulationEnabled(target));
  }
  
  /**
   * Test of {@link FileQueue} can identify compressed file and provide
   * readers to extract uncompressed data only if input-compression is enabled.
   */
  @Test
  public void testFileQueueDecompression() throws IOException {
    JobConf conf = new JobConf();
    FileSystem lfs = FileSystem.getLocal(conf);
    String inputLine = "Hi Hello!";
    
    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, true);
    CompressionEmulationUtil.setInputCompressionEmulationEnabled(conf, true);
    org.apache.hadoop.mapred.FileOutputFormat.setCompressOutput(conf, true);
    org.apache.hadoop.mapred.FileOutputFormat.setOutputCompressorClass(conf, 
                                                GzipCodec.class);

    // define the test's root temp directory
    Path rootTempDir =
        new Path(System.getProperty("test.build.data", "/tmp")).makeQualified(
            lfs.getUri(), lfs.getWorkingDirectory());

    Path tempDir = new Path(rootTempDir, "TestFileQueueDecompression");
    lfs.delete(tempDir, true);

    // create a compressed file
    Path compressedFile = new Path(tempDir, "test");
    OutputStream out = 
      CompressionEmulationUtil.getPossiblyCompressedOutputStream(compressedFile, 
                                                                 conf);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
    writer.write(inputLine);
    writer.close();
    
    compressedFile = compressedFile.suffix(".gz");
    // now read back the data from the compressed stream using FileQueue
    long fileSize = lfs.listStatus(compressedFile)[0].getLen();
    CombineFileSplit split = 
      new CombineFileSplit(new Path[] {compressedFile}, new long[] {fileSize});
    FileQueue queue = new FileQueue(split, conf);
    byte[] bytes = new byte[inputLine.getBytes().length];
    queue.read(bytes);
    queue.close();
    String readLine = new String(bytes);
    assertEquals("Compression/Decompression error", inputLine, readLine);
  }

  /**
   * Tests the computation logic of uncompressed input bytes by
   * {@link LoadJob#getUncompressedInputBytes(long, Configuration)}
   */
  @Test
  public void testComputeUncompressedInputBytes() {
    long possiblyCompressedInputBytes = 100000;
    float compressionRatio = 0.45F;
    Configuration conf = new Configuration();
    CompressionEmulationUtil.setMapInputCompressionEmulationRatio(conf,
        compressionRatio);

    // By default, input compression emulation is diabled. Verify the
    // computation of uncompressed input bytes.
    long result = CompressionEmulationUtil.getUncompressedInputBytes(
        possiblyCompressedInputBytes, conf);
    assertEquals(possiblyCompressedInputBytes, result);

    // Enable input compression emulation and verify uncompressed
    // input bytes computation logic
    CompressionEmulationUtil.setInputCompressionEmulationEnabled(conf, true);
    result = CompressionEmulationUtil.getUncompressedInputBytes(
        possiblyCompressedInputBytes, conf);
    assertEquals((long)(possiblyCompressedInputBytes/compressionRatio), result);
  }
}
