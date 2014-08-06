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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.nativetask.kvtest.TestInputFile;
import org.apache.hadoop.mapred.nativetask.testutil.ResultVerifier;
import org.apache.hadoop.mapred.nativetask.testutil.ScenarioConfiguration;
import org.apache.hadoop.mapred.nativetask.testutil.TestConstants;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;

public class CompressTest {

  @Test
  public void testSnappyCompress() throws Exception {
    final Configuration conf = ScenarioConfiguration.getNativeConfiguration();
    conf.addResource(TestConstants.SNAPPY_COMPRESS_CONF_PATH);
    final Job job = CompressMapper.getCompressJob("nativesnappy", conf);
    job.waitForCompletion(true);

    final Configuration hadoopconf = ScenarioConfiguration.getNormalConfiguration();
    hadoopconf.addResource(TestConstants.SNAPPY_COMPRESS_CONF_PATH);
    final Job hadoopjob = CompressMapper.getCompressJob("hadoopsnappy", hadoopconf);
    hadoopjob.waitForCompletion(true);

    final boolean compareRet = ResultVerifier.verify(CompressMapper.outputFileDir + "nativesnappy",
        CompressMapper.outputFileDir + "hadoopsnappy");
    assertEquals("file compare result: if they are the same ,then return true", true, compareRet);
  }

  @Test
  public void testGzipCompress() throws Exception {
    final Configuration conf = ScenarioConfiguration.getNativeConfiguration();
    conf.addResource(TestConstants.GZIP_COMPRESS_CONF_PATH);
    final Job job = CompressMapper.getCompressJob("nativegzip", conf);
    job.waitForCompletion(true);

    final Configuration hadoopconf = ScenarioConfiguration.getNormalConfiguration();
    hadoopconf.addResource(TestConstants.GZIP_COMPRESS_CONF_PATH);
    final Job hadoopjob = CompressMapper.getCompressJob("hadoopgzip", hadoopconf);
    hadoopjob.waitForCompletion(true);

    final boolean compareRet = ResultVerifier.verify(CompressMapper.outputFileDir + "nativegzip",
        CompressMapper.outputFileDir + "hadoopgzip");
    assertEquals("file compare result: if they are the same ,then return true", true, compareRet);
  }

  @Test
  public void testLz4Compress() throws Exception {
    final Configuration nativeConf = ScenarioConfiguration.getNativeConfiguration();
    nativeConf.addResource(TestConstants.LZ4_COMPRESS_CONF_PATH);
    final Job nativeJob = CompressMapper.getCompressJob("nativelz4", nativeConf);
    nativeJob.waitForCompletion(true);

    final Configuration hadoopConf = ScenarioConfiguration.getNormalConfiguration();
    hadoopConf.addResource(TestConstants.LZ4_COMPRESS_CONF_PATH);
    final Job hadoopJob = CompressMapper.getCompressJob("hadooplz4", hadoopConf);
    hadoopJob.waitForCompletion(true);
    final boolean compareRet = ResultVerifier.verify(CompressMapper.outputFileDir + "nativelz4",
        CompressMapper.outputFileDir + "hadooplz4");
    assertEquals("file compare result: if they are the same ,then return true", true, compareRet);
  }

  @Before
  public void startUp() throws Exception {
    final ScenarioConfiguration conf = new ScenarioConfiguration();
    final FileSystem fs = FileSystem.get(conf);
    final Path path = new Path(CompressMapper.inputFile);
    fs.delete(path);
    if (!fs.exists(path)) {
      new TestInputFile(ScenarioConfiguration.getNormalConfiguration().getInt(
          TestConstants.NATIVETASK_COMPRESS_FILESIZE, 100000),
          Text.class.getName(), Text.class.getName(), conf)
      .createSequenceTestFile(CompressMapper.inputFile);

    }
    fs.close();
  }
}
