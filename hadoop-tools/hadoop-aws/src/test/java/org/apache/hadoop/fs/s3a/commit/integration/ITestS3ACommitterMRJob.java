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

package org.apache.hadoop.fs.s3a.commit.integration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.commit.AbstractYarnClusterITest;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.LoggingTextOutputFormat;
import org.apache.hadoop.fs.s3a.commit.files.SuccessData;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter;
import org.apache.hadoop.fs.s3a.commit.staging.DirectoryStagingCommitter;
import org.apache.hadoop.fs.s3a.commit.staging.PartitionedStagingCommitter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.lsR;
import static org.apache.hadoop.fs.s3a.S3AUtils.applyLocatedFiles;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.FS_S3A_COMMITTER_STAGING_TMP_PATH;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.MAGIC;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants._SUCCESS;
import static org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants.FS_S3A_COMMITTER_STAGING_UUID;
import static org.apache.hadoop.fs.s3a.commit.staging.Paths.getMultipartUploadCommitsDirectory;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingCommitterConstants.JAVA_IO_TMPDIR;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingCommitterConstants.STAGING_UPLOADS;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test an MR Job with all the different committers.
 * This is a fairly complex parameterization: it is designed to
 * avoid the overhead of starting mini HDFS and yarn clusters for
 * individual committer types, so speed up operations.
 * This implicitly guarantees that there is never more than one committer MR\
 * job active at a time, so avoids overloading the test machine with too many
 * processes.
 */
@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ITestS3ACommitterMRJob extends AbstractYarnClusterITest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ACommitterMRJob.class);

  /**
   * Test array for parameterized test runs.
   * </ul>
   *
   * @return the committer binding for this run.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {new DirectoryCommitterTestBinding()},
        {new PartitionCommitterTestBinding()},
        {new MagicCommitterTestBinding()},
    });
  }

  /**
   * The static cluster binding with the lifecycle of this test; served
   * through instance-level methods for sharing across methods in the
   * suite.
   */
  @SuppressWarnings("StaticNonFinalField")
  private static ClusterBinding clusterBinding;

  @BeforeClass
  public static void setupClusters() throws IOException {
    clusterBinding = createCluster(new JobConf(), false);
  }

  @AfterClass
  public static void teardownClusters() throws IOException {
    terminateCluster(clusterBinding);
  }

  /**
   * The committer binding for this instance.
   */
  private final CommitterTestBinding committerTestBinding;

  public ITestS3ACommitterMRJob(
      final CommitterTestBinding committerTestBinding) {
    this.committerTestBinding = committerTestBinding;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    committerTestBinding.bind(getClusterBinding(),
        getFileSystem());
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    disableFilesystemCaching(conf);
    return conf;
  }

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  @Override
  protected String committerName() {
    return committerTestBinding.getCommitterName();
  }

  @Override
  protected ClusterBinding getClusterBinding() {
    return clusterBinding;
  }

  @Override
  public boolean useInconsistentClient() {
    return committerTestBinding.useInconsistentClient();
  }

  /**
   * Verify that the committer binding is happy.
   */
  @Test
  public void test_000() throws Throwable {
    committerTestBinding.validate();

  }
  @Test
  public void test_100() throws Throwable {
    committerTestBinding.test_100();
  }

  @Test
  public void test_200_execute() throws Exception {
    describe("Run an MR with committer %s", committerName());

    S3AFileSystem fs = getFileSystem();
    // final dest is in S3A
    // we can't use the method name as the template places square braces into that and
    // URI creation fails.

    Path outputPath = path("ITestS3ACommitterMRJob-execute-"+ committerName());
    // create and delete to force in a tombstone marker -see HADOOP-16207
    fs.mkdirs(outputPath);
    fs.delete(outputPath, true);

    String commitUUID = UUID.randomUUID().toString();
    String suffix = isUniqueFilenames() ? ("-" + commitUUID) : "";
    int numFiles = getTestFileCount();

    // create all the input files on the local FS.
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

    // setting up staging options is harmless for other committers
    jobConf.set(FS_S3A_COMMITTER_STAGING_UUID, commitUUID);
    File tmpDir = new File(System.getProperty(JAVA_IO_TMPDIR));
    String tmpDirStr = tmpDir.toURI().toString();
    LOG.info("Staging temp dir is {}", tmpDirStr);
    jobConf.set(FS_S3A_COMMITTER_STAGING_TMP_PATH, tmpDirStr);


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
      String sysprops = String.format("-Xmx128m -Dlog4j.configuration=%s",
          log4j);
      jobConf.set(JobConf.MAPRED_MAP_TASK_JAVA_OPTS, sysprops);
      jobConf.set(JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS, sysprops);
      jobConf.set("yarn.app.mapreduce.am.command-opts", sysprops);
    }

    applyCustomConfigOptions(jobConf);
    // fail fast if anything goes wrong
    mrJob.setMaxMapAttempts(1);

    try (DurationInfo ignore = new DurationInfo(LOG, "Job Submit")) {
      mrJob.submit();
    }
    try (DurationInfo ignore = new DurationInfo(LOG, "Job Execution")) {
      boolean succeeded = mrJob.waitForCompletion(true);
      assertTrue("MR job failed", succeeded);
    }

    waitForConsistency();
    Path successPath = new Path(outputPath, _SUCCESS);
    SuccessData successData = validateSuccessFile(outputPath, committerName(),
        fs, "MR job");
    String commitData = successData.toString();

    FileStatus[] results = fs.listStatus(outputPath,
        S3AUtils.HIDDEN_FILE_FILTER);
    int fileCount = results.length;
    Assertions.assertThat(fileCount)
        .describedAs("No files in output directory %s", outputPath)
        .isNotEqualTo(0);

    List<String> actualFiles = Arrays.stream(results)
        .map(s -> s.getPath().toString())
        .sorted()
        .collect(Collectors.toList());


    Assertions.assertThat(actualFiles)
        .describedAs("Files found in %s", outputPath)
        .isEqualTo(expectedFiles);

    Assertions.assertThat(successData.getFilenames())
        .describedAs("Success files listed in %s:%s",
            successPath, commitData)
        .isNotEmpty()
        .containsExactlyInAnyOrderElementsOf(expectedKeys);

    assertPathDoesNotExist("temporary dir should only be from"
            + " classic file committers",
        new Path(outputPath, CommitConstants.TEMPORARY));
    customPostExecutionValidation(outputPath, successData);
  }

  @Override
  protected void customPostExecutionValidation(final Path destPath,
      final SuccessData successData) throws Exception {
    committerTestBinding.validateResult(destPath, successData);
  }

  @Test
  public void test_500() throws Throwable {
    committerTestBinding.test_500();
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


  /**
   * A binding class for committer tests.
   * Subclasses of this will be instantiated and drive the parameterized
   * test suite.
   *
   * These classes will be instantiated in a static array of the suite, and
   * not bound to a cluster binding or filesystem.
   *
   * The per-method test setup method will call bind() to do the one-off binding.
   */

  private static abstract class CommitterTestBinding {

    private final String committerName;

    private ClusterBinding clusterBinding;

    private S3AFileSystem remoteFS;

    protected CommitterTestBinding(final String committerName) {
      this.committerName = committerName;
    }

    private void bind(
        ClusterBinding binding,
        final S3AFileSystem fs) {
      if (clusterBinding == null) {
        clusterBinding = binding;
      }
      remoteFS = fs;
    }

    protected String getCommitterName() {
      return committerName;
    }

    protected ClusterBinding getClusterBinding() {
      return clusterBinding;
    }

    protected S3AFileSystem getRemoteFS() {
      return remoteFS;
    }

    FileSystem getClusterFS() throws IOException {
      return getClusterBinding().getClusterFS();
    }

    @Override
    public String toString() {
      return committerName;
    }

    /**
     * Override point to let implementations tune the MR Job conf.
     * @param jobConf configuration
     */
    protected void applyCustomConfigOptions(JobConf jobConf)
        throws IOException {

    }


    /**
     * Should the inconsistent S3A client be used?
     * @return true for inconsistent listing
     */
    public abstract boolean useInconsistentClient();

    /**
     * Override point for any committer specific validation operations;
     * called after the base assertions have all passed.
     * @param destPath destination of work
     * @param successData loaded success data
     * @throws Exception failure
     */
    protected void validateResult(Path destPath,
        SuccessData successData)
        throws Exception {

    }

    void test_100() throws Throwable {

    }

    void test_500() throws Throwable {

    }

    public void validate() throws Throwable {
      assertNotNull("Not bound to a cluster", clusterBinding);
      assertNotNull("No cluster filesystem", getClusterFS());
      assertNotNull("No yarn cluster", clusterBinding.getYarn());

    }
  }

  /**
   * The directory staging committer.
   */
  private static final class DirectoryCommitterTestBinding
      extends CommitterTestBinding {

    private DirectoryCommitterTestBinding() {
      super(DirectoryStagingCommitter.NAME);
    }

    /**
     * @return true for inconsistent listing
     */
    public boolean useInconsistentClient() {
      return true;
    }

    /**
     * Verify that staging commit dirs are made absolute under the user's
     * home directory, so, in a secure cluster, private.
     */
    @Override
    void test_100() throws Throwable {
      FileSystem fs = getClusterFS();
      Configuration conf = fs.getConf();
      String pri = "private";
      conf.set(FS_S3A_COMMITTER_STAGING_TMP_PATH, pri);
      Path dir = getMultipartUploadCommitsDirectory(conf, "uuid");
      Assertions.assertThat(dir.isAbsolute())
          .describedAs("non-absolute path")
          .isTrue();
      String stagingTempDir = dir.toString().toLowerCase(Locale.ENGLISH);
      String self = UserGroupInformation.getCurrentUser()
          .getShortUserName().toLowerCase(Locale.ENGLISH);
      Assertions.assertThat(stagingTempDir)
          .describedAs("Staging committer temp path in cluster")
          .contains(pri + "/" + self)
          .endsWith("uuid/" + STAGING_UPLOADS);

    }
  }

  /**
   * The partition committer test binding.
   */
  private static final class PartitionCommitterTestBinding
      extends CommitterTestBinding {

    private PartitionCommitterTestBinding() {
      super(PartitionedStagingCommitter.NAME);
    }

    /**
     * @return true for inconsistent listing
     */
    public boolean useInconsistentClient() {
      return true;
    }
  }

  /**
   * The magic committer test binding.
   * This includes extra result validation.
   */
  private static final class MagicCommitterTestBinding
      extends CommitterTestBinding {

    private MagicCommitterTestBinding() {
      super(MagicS3GuardCommitter.NAME);
    }

    /**
     * @return we need a consistent store.
     */
    public boolean useInconsistentClient() {
      return false;
    }

    /**
     * The result validation here is that there isn't a __magic directory
     * any more.
     * @param destPath destination of work
     * @param successData loaded success data
     * @throws Exception failure
     */
    @Override
    protected void validateResult(final Path destPath,
        final SuccessData successData)
        throws Exception {
      Path magicDir = new Path(destPath, MAGIC);

      // if an FNFE isn't raised on getFileStatus, list out the directory
      // tree
      S3AFileSystem fs = getRemoteFS();
      // log the contents
      lsR(fs, destPath, true);
      intercept(FileNotFoundException.class, () -> {
        final FileStatus st = fs.getFileStatus(magicDir);
        StringBuilder result = new StringBuilder("Found magic dir which should"
            + " have been deleted at ").append(st).append('\n');
        result.append("[");
        applyLocatedFiles(fs.listFiles(magicDir, true),
            (status) -> result.append(status.getPath()).append('\n'));
        result.append("[");
        return result.toString();
      });
    }
  }

}
