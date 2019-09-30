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
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.files.SuccessData;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.deployService;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestPropertyBool;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.terminateService;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.FS_S3A_COMMITTER_STAGING_TMP_PATH;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.FS_S3A_COMMITTER_STAGING_UNIQUE_FILENAMES;
import static org.apache.hadoop.fs.s3a.commit.staging.StagingCommitterConstants.JAVA_IO_TMPDIR;

/**
 * Full integration test MR jobs.
 *
 * This is all done on shared static mini YARN and (optionally) HDFS clusters,
 * set up before any of the tests methods run.
 *
 * To isolate tests properly for parallel test runs, that static state
 * needs to be stored in the final classes implementing the tests, and
 * exposed to the base class, with the setup clusters in the
 * specific test suites creating the clusters with unique names.
 *
 * This is "hard" to do in Java, unlike, say, Scala.
 *
 * Note: this turns out not to be the root cause of ordering problems
 * with the Terasort tests (that is hard coded use of a file in the local FS),
 * but this design here does make it clear that the before and after class
 * operations are explicitly called in the subclasses.
 * If two subclasses of this class are instantiated in the same JVM, in order,
 * they are guaranteed to be isolated.
 *
 * History: this is a superclass extracted from
 * {@link AbstractITCommitMRJob} while adding support for testing terasorting.
 *
 */
public abstract class AbstractYarnClusterITest extends AbstractCommitITest {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractYarnClusterITest.class);

  private static final int TEST_FILE_COUNT = 1;
  private static final int SCALE_TEST_FILE_COUNT = 2;

  public static final int SCALE_TEST_KEYS = 100;
  public static final int BASE_TEST_KEYS = 10;

  private boolean scaleTest;

  private boolean uniqueFilenames = false;

  /**
   * This is the cluster binding which every subclass must create.
   */
  protected static final class ClusterBinding {

    private String clusterName;

    private final MiniDFSClusterService hdfs;

    private final MiniMRYarnCluster yarn;

    public ClusterBinding(
        final String clusterName,
        final MiniDFSClusterService hdfs,
        final MiniMRYarnCluster yarn) {
      this.clusterName = clusterName;
      this.hdfs = hdfs;
      this.yarn = checkNotNull(yarn);
    }

    public MiniDFSClusterService getHdfs() {
      return hdfs;
    }

    /**
     * Get the cluster FS, which will either be HDFS or the local FS.
     * @return a filesystem.
     * @throws IOException failure
     */
    public FileSystem getClusterFS() throws IOException {
      MiniDFSClusterService miniCluster = getHdfs();
      return miniCluster != null
          ? miniCluster.getClusterFS()
          : FileSystem.getLocal(yarn.getConfig());
    }

    public MiniMRYarnCluster getYarn() {
      return yarn;
    }

    public Configuration getConf() {
      return getYarn().getConfig();
    }

    public String getClusterName() {
      return clusterName;
    }

    public void terminate() {
      terminateService(getYarn());
      terminateService(getHdfs());
    }
  }

  /**
   * Create the cluster binding. This must be done in
   * class setup of the (final) subclass.
   * The local filesystem is used as the cluster FS.
   * @param conf configuration to start with.
   * @return the cluster binding.
   * @throws IOException failure.
   */
  protected static ClusterBinding createCluster(JobConf conf)
      throws IOException {
    return createCluster(conf, false);
  }

  /**
   * Create the cluster binding. This must be done in
   * class setup of the (final) subclass.
   * If an HDFS cluster is requested,
   * the HDFS and YARN clusters will share the same configuration, so
   * the HDFS cluster binding is implicitly propagated to YARN.
   * If one is not requested, the local filesystem is used as the cluster FS.
   * @param conf configuration to start with.
   * @param useHDFS should an HDFS cluster be instantiated.
   * @return the cluster binding.
   * @throws IOException failure.
   */
  protected static ClusterBinding createCluster(JobConf conf,
      final boolean useHDFS)
      throws IOException {

    conf.setBoolean(JHAdminConfig.MR_HISTORY_CLEANER_ENABLE, false);
    conf.setLong(CommonConfigurationKeys.FS_DU_INTERVAL_KEY, Long.MAX_VALUE);
    // minicluster tests overreact to not enough disk space.
    conf.setBoolean(YarnConfiguration.NM_DISK_HEALTH_CHECK_ENABLE, false);
    conf.setInt(YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE, 100);
    // create a unique cluster name based on the current time in millis.
    String timestamp = LocalDateTime.now().format(
        DateTimeFormatter.ofPattern("yyyy-MM-dd-HH.mm.ss.SS"));
    String clusterName = "yarn-" + timestamp;
    MiniDFSClusterService miniDFSClusterService =
        useHDFS
            ? deployService(conf, new MiniDFSClusterService())
            : null;
    MiniMRYarnCluster yarnCluster = deployService(conf,
        new MiniMRYarnCluster(clusterName, 2));
    return new ClusterBinding(clusterName, miniDFSClusterService, yarnCluster);
  }

  protected static void terminateCluster(ClusterBinding clusterBinding) {
    if (clusterBinding != null) {
      clusterBinding.terminate();
    }
  }

  /**
   * Get the cluster binding for this subclass.
   * @return the cluster binding
   */
  protected abstract ClusterBinding getClusterBinding();

  protected MiniDFSClusterService getHdfs() {
    return getClusterBinding().getHdfs();
  }


  protected MiniMRYarnCluster getYarn() {
    return getClusterBinding().getYarn();
  }

  protected FileSystem getDFS() throws IOException {
    MiniDFSClusterService hdfs = getHdfs();
    return hdfs != null
        ? hdfs.getClusterFS()
        : FileSystem.getLocal(getConfiguration());
  }

  /**
   * The name of the committer as returned by
   * {@link AbstractS3ACommitter#getName()}
   * and used for committer construction.
   */
  protected abstract String committerName();

  @Override
  public void setup() throws Exception {
    super.setup();
    assertNotNull("cluster is not bound",
        getClusterBinding());

    scaleTest = getTestPropertyBool(
        getConfiguration(),
        KEY_SCALE_TESTS_ENABLED,
        DEFAULT_SCALE_TESTS_ENABLED);
  }

  @Override
  protected int getTestTimeoutMillis() {
    return SCALE_TEST_TIMEOUT_SECONDS * 1000;
  }

  protected JobConf newJobConf() {
    return new JobConf(getYarn().getConfig());
  }


  protected Job createJob() throws IOException {
    Configuration jobConf = getClusterBinding().getConf();
    jobConf.addResource(getConfiguration());
    Job mrJob = Job.getInstance(jobConf, getMethodName());
    patchConfigurationForCommitter(mrJob.getConfiguration());
    return mrJob;
  }

  /**
   * Patch the (job) configuration for this committer.
   * @param jobConf configuration to patch
   * @return a configuration which will run this configuration.
   */
  protected Configuration patchConfigurationForCommitter(
      final Configuration jobConf) {
    jobConf.setBoolean(FS_S3A_COMMITTER_STAGING_UNIQUE_FILENAMES,
        uniqueFilenames);
    bindCommitter(jobConf,
        CommitConstants.S3A_COMMITTER_FACTORY,
        committerName());
    // pass down the scale test flag
    jobConf.setBoolean(KEY_SCALE_TESTS_ENABLED, scaleTest);
    // and fix the commit dir to the local FS across all workers.
    File tmpDir = new File(System.getProperty(JAVA_IO_TMPDIR));
    String tmpDirStr = tmpDir.toURI().toString();
    LOG.info("Staging temp dir is {}", tmpDirStr);
    jobConf.set(FS_S3A_COMMITTER_STAGING_TMP_PATH, tmpDirStr);
    return jobConf;
  }

  /**
   * Get the file count for the test.
   * @return the number of mappers to create.
   */
  public int getTestFileCount() {
    return scaleTest ? SCALE_TEST_FILE_COUNT : TEST_FILE_COUNT;
  }

  /**
   * Override point to let implementations tune the MR Job conf.
   * @param jobConf configuration
   */
  protected void applyCustomConfigOptions(JobConf jobConf) throws IOException {

  }

  /**
   * Override point for any committer specific validation operations;
   * called after the base assertions have all passed.
   * @param destPath destination of work
   * @param successData loaded success data
   * @throws Exception failure
   */
  protected void customPostExecutionValidation(Path destPath,
      SuccessData successData)
      throws Exception {

  }

  /**
   * Assume that scale tests are enabled.
   */
  protected void requireScaleTestsEnabled() {
    assume("Scale test disabled: to enable set property " +
            KEY_SCALE_TESTS_ENABLED,
        isScaleTest());
  }

  public boolean isScaleTest() {
    return scaleTest;
  }

  public boolean isUniqueFilenames() {
    return uniqueFilenames;
  }
}
