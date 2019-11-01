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
package org.apache.hadoop.mapred.nativetask.compresstest;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.nativetask.NativeRuntime;
import org.apache.hadoop.mapred.nativetask.kvtest.TestInputFile;
import org.apache.hadoop.mapred.nativetask.testutil.ResultVerifier;
import org.apache.hadoop.mapred.nativetask.testutil.ScenarioConfiguration;
import org.apache.hadoop.mapred.nativetask.testutil.TestConstants;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.AfterClass;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class CompressTest {

  private static final Configuration nativeConf = ScenarioConfiguration.getNativeConfiguration();
  private static final Configuration hadoopConf = ScenarioConfiguration.getNormalConfiguration();

  static {
    nativeConf.addResource(TestConstants.COMPRESS_TEST_CONF_PATH);
    hadoopConf.addResource(TestConstants.COMPRESS_TEST_CONF_PATH);
  }

  @Test
  public void testSnappyCompress() throws Exception {
    final String snappyCodec = "org.apache.hadoop.io.compress.SnappyCodec";

    nativeConf.set(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, snappyCodec);
    final String nativeOutputPath =
      TestConstants.NATIVETASK_COMPRESS_TEST_NATIVE_OUTPUTDIR + "/snappy";
    final Job job = CompressMapper.getCompressJob("nativesnappy", nativeConf,
      TestConstants.NATIVETASK_COMPRESS_TEST_INPUTDIR, nativeOutputPath);
    assertThat(job.waitForCompletion(true)).isTrue();

    hadoopConf.set(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, snappyCodec);
    final String hadoopOutputPath =
      TestConstants.NATIVETASK_COMPRESS_TEST_NORMAL_OUTPUTDIR + "/snappy";
    final Job hadoopjob = CompressMapper.getCompressJob("hadoopsnappy", hadoopConf,
      TestConstants.NATIVETASK_COMPRESS_TEST_INPUTDIR, hadoopOutputPath);
    assertThat(hadoopjob.waitForCompletion(true)).isTrue();

    final boolean compareRet = ResultVerifier.verify(nativeOutputPath, hadoopOutputPath);
    assertThat(compareRet)
        .withFailMessage(
            "file compare result: if they are the same ,then return true")
        .isTrue();
    ResultVerifier.verifyCounters(hadoopjob, job);
  }

  @Test
  public void testGzipCompress() throws Exception {
    final String gzipCodec = "org.apache.hadoop.io.compress.GzipCodec";

    nativeConf.set(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, gzipCodec);
    final String nativeOutputPath =
      TestConstants.NATIVETASK_COMPRESS_TEST_NATIVE_OUTPUTDIR + "/gzip";
    final Job job = CompressMapper.getCompressJob("nativegzip", nativeConf,
      TestConstants.NATIVETASK_COMPRESS_TEST_INPUTDIR, nativeOutputPath);
    assertThat(job.waitForCompletion(true)).isTrue();

    hadoopConf.set(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, gzipCodec);
    final String hadoopOutputPath =
      TestConstants.NATIVETASK_COMPRESS_TEST_NORMAL_OUTPUTDIR + "/gzip";
    final Job hadoopjob = CompressMapper.getCompressJob("hadoopgzip", hadoopConf,
      TestConstants.NATIVETASK_COMPRESS_TEST_INPUTDIR, hadoopOutputPath);
    assertThat(hadoopjob.waitForCompletion(true)).isTrue();

    final boolean compareRet = ResultVerifier.verify(nativeOutputPath, hadoopOutputPath);
    assertThat(compareRet)
        .withFailMessage(
            "file compare result: if they are the same ,then return true")
        .isTrue();
    ResultVerifier.verifyCounters(hadoopjob, job);
  }

  @Test
  public void testLz4Compress() throws Exception {
    final String lz4Codec = "org.apache.hadoop.io.compress.Lz4Codec";

    nativeConf.set(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, lz4Codec);
    final String nativeOutputPath =
      TestConstants.NATIVETASK_COMPRESS_TEST_NATIVE_OUTPUTDIR + "/lz4";
    final Job nativeJob = CompressMapper.getCompressJob("nativelz4", nativeConf,
      TestConstants.NATIVETASK_COMPRESS_TEST_INPUTDIR, nativeOutputPath);
    assertThat(nativeJob.waitForCompletion(true)).isTrue();

    hadoopConf.set(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, lz4Codec);
    final String hadoopOutputPath =
      TestConstants.NATIVETASK_COMPRESS_TEST_NORMAL_OUTPUTDIR + "/lz4";
    final Job hadoopJob = CompressMapper.getCompressJob("hadooplz4", hadoopConf,
      TestConstants.NATIVETASK_COMPRESS_TEST_INPUTDIR, hadoopOutputPath);
    assertThat(hadoopJob.waitForCompletion(true)).isTrue();
    final boolean compareRet = ResultVerifier.verify(nativeOutputPath, hadoopOutputPath);
    assertThat(compareRet)
        .withFailMessage(
            "file compare result: if they are the same ,then return true")
        .isTrue();
    ResultVerifier.verifyCounters(hadoopJob, nativeJob);
  }

  @Before
  public void startUp() throws Exception {
    Assume.assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    Assume.assumeTrue(NativeRuntime.isNativeLibraryLoaded());
    final ScenarioConfiguration conf = new ScenarioConfiguration();
    final FileSystem fs = FileSystem.get(conf);
    final Path path = new Path(TestConstants.NATIVETASK_COMPRESS_TEST_INPUTDIR);
    fs.delete(path, true);
    if (!fs.exists(path)) {
      new TestInputFile(hadoopConf.getInt(
          TestConstants.NATIVETASK_COMPRESS_FILESIZE, 100000),
          Text.class.getName(), Text.class.getName(), conf)
      .createSequenceTestFile(TestConstants.NATIVETASK_COMPRESS_TEST_INPUTDIR);
    }
    fs.close();
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    final FileSystem fs = FileSystem.get(new ScenarioConfiguration());
    fs.delete(new Path(TestConstants.NATIVETASK_COMPRESS_TEST_DIR), true);
    fs.close();
  }

}
