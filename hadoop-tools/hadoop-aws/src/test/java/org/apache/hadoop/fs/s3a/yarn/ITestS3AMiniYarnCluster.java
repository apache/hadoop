/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.fs.s3a.yarn;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.commit.files.SuccessData;
import org.apache.hadoop.fs.s3a.commit.staging.StagingCommitter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.server.MiniYARNCluster;

import org.junit.Test;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.FS_S3A_COMMITTER_NAME;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.FS_S3A_COMMITTER_STAGING_UNIQUE_FILENAMES;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants._SUCCESS;

/**
 * Tests that S3A is usable through a YARN application.
 */
public class ITestS3AMiniYarnCluster extends AbstractS3ATestBase {

  private MiniYARNCluster yarnCluster;
  private Path rootPath;

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    // and set up commit code
    conf.setBoolean(FS_S3A_COMMITTER_STAGING_UNIQUE_FILENAMES,
        false);
    conf.set(FS_S3A_COMMITTER_NAME, StagingCommitter.NAME);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    S3AFileSystem fs = getFileSystem();
    Configuration conf = getConfiguration();
    rootPath = path("MiniClusterWordCount");
    Path workingDir = path("working");
    fs.setWorkingDirectory(workingDir);
    fs.mkdirs(new Path(rootPath, "input/"));

    yarnCluster = new MiniYARNCluster("MiniClusterWordCount", // testName
            1, // number of node managers
            1, // number of local log dirs per node manager
            1); // number of hdfs dirs per node manager
    yarnCluster.init(conf);
    yarnCluster.start();
  }

  @Override
  public void teardown() throws Exception {
    if (yarnCluster != null) {
      yarnCluster.stop();
    }
    super.teardown();
  }

  @Test
  public void testWithMiniCluster() throws Exception {
    S3AFileSystem fs = getFileSystem();
    Configuration conf = getConfiguration();

    Path input = new Path(rootPath, "input/in.txt");
    input = input.makeQualified(fs.getUri(), fs.getWorkingDirectory());
    Path output = new Path(rootPath, "output/");
    output = output.makeQualified(fs.getUri(), fs.getWorkingDirectory());

    writeStringToFile(input, "first line\nsecond line\nthird line");

    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(WordCount.TokenizerMapper.class);
    job.setCombinerClass(WordCount.IntSumReducer.class);
    job.setReducerClass(WordCount.IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);

    int exitCode = (job.waitForCompletion(true) ? 0 : 1);
    assertEquals("Returned error code.", 0, exitCode);

    Path success = new Path(output, _SUCCESS);
    FileStatus status = fs.getFileStatus(success);
    assertTrue("0 byte success file - not a s3guard committer " + success,
        status.getLen() > 0);
    SuccessData successData = SuccessData.load(fs, success);
    String commitDetails = successData.toString();
    LOG.info("Committer details \n{}", commitDetails);
    String outputAsStr = readStringFromFile(new Path(output, "part-r-00000"));
    Map<String, Integer> resAsMap = getResultAsMap(outputAsStr);

    assertEquals(4, resAsMap.size());
    assertEquals(1, (int) resAsMap.get("first"));
    assertEquals(1, (int) resAsMap.get("second"));
    assertEquals(1, (int) resAsMap.get("third"));
    assertEquals(3, (int) resAsMap.get("line"));
  }

  /**
   * helper method.
   */
  private Map<String, Integer> getResultAsMap(String outputAsStr)
      throws IOException {
    Map<String, Integer> result = new HashMap<>();
    for (String line : outputAsStr.split("\n")) {
      String[] tokens = line.split("\t");
      assertTrue("Not enough tokens in in string \" "+ line
            + "\" from output \"" + outputAsStr + "\"",
          tokens.length > 1);
      result.put(tokens[0], Integer.parseInt(tokens[1]));
    }
    return result;
  }

  /**
   * helper method.
   */
  private void writeStringToFile(Path path, String string) throws IOException {
    Configuration conf = getConfiguration();

    FileContext fc = S3ATestUtils.createTestFileContext(conf);
    try (FSDataOutputStream file = fc.create(path,
            EnumSet.of(CreateFlag.CREATE))) {
      file.write(string.getBytes());
    }
  }

  /**
   * helper method.
   */
  private String readStringFromFile(Path path) throws IOException {
    S3AFileSystem fs = getFileSystem();

    try (FSDataInputStream in = fs.open(path)) {
      long bytesLen = fs.getFileStatus(path).getLen();
      byte[] buffer = new byte[(int) bytesLen];
      IOUtils.readFully(in, buffer, 0, buffer.length);
      return new String(buffer);
    }
  }

}
