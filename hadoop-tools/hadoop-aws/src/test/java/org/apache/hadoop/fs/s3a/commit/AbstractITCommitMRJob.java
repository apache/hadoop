/*
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

package org.apache.hadoop.fs.s3a.commit;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Sets;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.commit.files.SuccessData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants.FS_S3A_COMMITTER_STAGING_UUID;

/**
 * Test for an MR Job with all the different committers.
 */
public abstract class AbstractITCommitMRJob extends AbstractYarnClusterITest {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractITCommitMRJob.class);

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testMRJob() throws Exception {
    describe("Run a simple MR Job");

    S3AFileSystem fs = getFileSystem();
    // final dest is in S3A
    Path outputPath = path(getMethodName());

    String commitUUID = UUID.randomUUID().toString();
    String suffix = isUniqueFilenames() ? ("-" + commitUUID) : "";
    int numFiles = getTestFileCount();
    List<String> expectedFiles = new ArrayList<>(numFiles);
    Set<String> expectedKeys = Sets.newHashSet();
    for (int i = 0; i < numFiles; i += 1) {
      File file = temp.newFile(i + ".text");
      try (FileOutputStream out = new FileOutputStream(file)) {
        out.write(("file " + i).getBytes(StandardCharsets.UTF_8));
      }
      String filename = String.format("part-m-%05d%s", i, suffix);
      Path path = new Path(outputPath, filename);
      expectedFiles.add(path.toString());
      expectedKeys.add("/" + fs.pathToKey(path));
    }
    Collections.sort(expectedFiles);

    Job mrJob = createJob();
    JobConf jobConf = (JobConf) mrJob.getConfiguration();

    mrJob.setOutputFormatClass(LoggingTextOutputFormat.class);
    FileOutputFormat.setOutputPath(mrJob, outputPath);

    File mockResultsFile = temp.newFile("committer.bin");
    mockResultsFile.delete();
    String committerPath = "file:" + mockResultsFile;
    jobConf.set("mock-results-file", committerPath);
    jobConf.set(FS_S3A_COMMITTER_STAGING_UUID, commitUUID);

    mrJob.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(mrJob, new Path(temp.getRoot().toURI()));

    mrJob.setMapperClass(MapClass.class);
    mrJob.setNumReduceTasks(0);

    // an attempt to set up log4j properly, which clearly doesn't work
    URL log4j = getClass().getClassLoader().getResource("log4j.properties");
    if (log4j != null && log4j.getProtocol().equals("file")) {
      Path log4jPath = new Path(log4j.toURI());
      LOG.debug("Using log4j path {}", log4jPath);
      mrJob.addFileToClassPath(log4jPath);
      String sysprops = String.format("-Xmx256m -Dlog4j.configuration=%s",
          log4j);
      jobConf.set(JobConf.MAPRED_MAP_TASK_JAVA_OPTS, sysprops);
      jobConf.set("yarn.app.mapreduce.am.command-opts", sysprops);
    }

    applyCustomConfigOptions(jobConf);
    // fail fast if anything goes wrong
    mrJob.setMaxMapAttempts(1);

    mrJob.submit();
    try (DurationInfo ignore = new DurationInfo(LOG, "Job Execution")) {
      boolean succeeded = mrJob.waitForCompletion(true);
      assertTrue("MR job failed", succeeded);
    }

    waitForConsistency();
    assertIsDirectory(outputPath);
    FileStatus[] results = fs.listStatus(outputPath,
        S3AUtils.HIDDEN_FILE_FILTER);
    int fileCount = results.length;
    List<String> actualFiles = new ArrayList<>(fileCount);
    assertTrue("No files in output directory", fileCount != 0);
    LOG.info("Found {} files", fileCount);
    for (FileStatus result : results) {
      LOG.debug("result: {}", result);
      actualFiles.add(result.getPath().toString());
    }
    Collections.sort(actualFiles);

    SuccessData successData = validateSuccessFile(fs, outputPath,
        committerName());
    List<String> successFiles = successData.getFilenames();
    String commitData = successData.toString();
    assertTrue("No filenames in " + commitData,
        !successFiles.isEmpty());

    assertEquals("Should commit the expected files",
        expectedFiles, actualFiles);

    Set<String> summaryKeys = Sets.newHashSet();
    summaryKeys.addAll(successFiles);
    assertEquals("Summary keyset doesn't list the the expected paths "
        + commitData, expectedKeys, summaryKeys);
    assertPathDoesNotExist("temporary dir",
        new Path(outputPath, CommitConstants.TEMPORARY));
    customPostExecutionValidation(outputPath, successData);
  }

  /**
   *  Test Mapper.
   *  This is executed in separate process, and must not make any assumptions
   *  about external state.
   */
  public static class MapClass
      extends Mapper<LongWritable, Text, LongWritable, Text> {

    private int operations;
    private String id = "";
    private LongWritable l = new LongWritable();
    private Text t = new Text();

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      super.setup(context);
      // force in Log4J logging
      org.apache.log4j.BasicConfigurator.configure();
      boolean scaleMap = context.getConfiguration()
          .getBoolean(KEY_SCALE_TESTS_ENABLED, false);
      operations = scaleMap ? SCALE_TEST_KEYS : BASE_TEST_KEYS;
      id = context.getTaskAttemptID().toString();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      for (int i = 0; i < operations; i++) {
        l.set(i);
        t.set(String.format("%s:%05d", id, i));
        context.write(l, t);
      }
    }
  }

}
