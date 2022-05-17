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

package org.apache.hadoop.fs.azurebfs.commit;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azure.integration.AzureTestConstants;
import org.apache.hadoop.fs.azurebfs.contract.ABFSContractTestBinding;
import org.apache.hadoop.fs.azurebfs.contract.AbfsFileSystemContract;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.AbstractManifestCommitterTest;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.util.DurationInfo;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.azure.integration.AzureTestUtils.assumeScaleTestsEnabled;
import static org.apache.hadoop.io.IOUtils.closeStream;

/**
 * Tests which create a yarn minicluster.
 * These are all considered scale tests; the probe for
 * scale tests being enabled is executed before the cluster
 * is set up to avoid wasting time on non-scale runs.
 */
public abstract class AbstractAbfsClusterITest extends
    AbstractManifestCommitterTest {

  public static final int NO_OF_NODEMANAGERS = 2;

  private final ABFSContractTestBinding binding;


  /**
   * The static cluster binding with the lifecycle of this test; served
   * through instance-level methods for sharing across methods in the
   * suite.
   */
  @SuppressWarnings("StaticNonFinalField")
  private static ClusterBinding clusterBinding;

  protected AbstractAbfsClusterITest() throws Exception {
    binding = new ABFSContractTestBinding();
  }

  @Override
  protected int getTestTimeoutMillis() {
    return AzureTestConstants.SCALE_TEST_TIMEOUT_MILLIS;
  }

  @Override
  public void setup() throws Exception {
    binding.setup();
    super.setup();
    requireScaleTestsEnabled();
    if (getClusterBinding() == null) {
      clusterBinding = demandCreateClusterBinding();
    }
    assertNotNull("cluster is not bound", getClusterBinding());
  }

  @AfterClass
  public static void teardownClusters() throws IOException {
    terminateCluster(clusterBinding);
    clusterBinding = null;
  }

  @Override
  protected AbstractFSContract createContract(final Configuration conf) {
    return new AbfsFileSystemContract(conf, binding.isSecureMode());
  }

  @Override
  protected Configuration createConfiguration() {
    return AbfsCommitTestHelper.prepareTestConfiguration(binding);
  }

  /**
   * This is the cluster binding which every subclass must create.
   */
  protected static final class ClusterBinding {

    private String clusterName;

    private final MiniMRYarnCluster yarn;

    public ClusterBinding(
        final String clusterName,
        final MiniMRYarnCluster yarn) {
      this.clusterName = clusterName;
      this.yarn = requireNonNull(yarn);
    }


    /**
     * Get the cluster FS, which will either be HDFS or the local FS.
     * @return a filesystem.
     * @throws IOException failure
     */
    public FileSystem getClusterFS() throws IOException {
      return FileSystem.getLocal(yarn.getConfig());
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
      closeStream(getYarn());
    }
  }

  /**
   * Create the cluster binding.
   * The configuration will be patched by propagating down options
   * from the maven build (S3Guard binding etc) and turning off unwanted
   * YARN features.
   *
   * If an HDFS cluster is requested,
   * the HDFS and YARN clusters will share the same configuration, so
   * the HDFS cluster binding is implicitly propagated to YARN.
   * If one is not requested, the local filesystem is used as the cluster FS.
   * @param conf configuration to start with.
   * @return the cluster binding.
   * @throws IOException failure.
   */
  protected static ClusterBinding createCluster(
      final JobConf conf) throws IOException {
    try (DurationInfo d = new DurationInfo(LOG, "Creating YARN MiniCluster")) {
      conf.setBoolean(JHAdminConfig.MR_HISTORY_CLEANER_ENABLE, false);
      // create a unique cluster name based on the current time in millis.
      String timestamp = LocalDateTime.now().format(
          DateTimeFormatter.ofPattern("yyyy-MM-dd-HH.mm.ss.SS"));
      String clusterName = "yarn-" + timestamp;
      MiniMRYarnCluster yarnCluster =
          new MiniMRYarnCluster(clusterName, NO_OF_NODEMANAGERS);
      yarnCluster.init(conf);
      yarnCluster.start();
      return new ClusterBinding(clusterName, yarnCluster);
    }
  }

  /**
   * Terminate the cluster if it is not null.
   * @param cluster the cluster
   */
  protected static void terminateCluster(ClusterBinding cluster) {
    if (cluster != null) {
      cluster.terminate();
    }
  }

  /**
   * Get the cluster binding for this subclass.
   * @return the cluster binding
   */
  protected ClusterBinding getClusterBinding() {
    return clusterBinding;
  }

  protected MiniMRYarnCluster getYarn() {
    return getClusterBinding().getYarn();
  }


  /**
   * We stage work into a temporary directory rather than directly under
   * the user's home directory, as that is often rejected by CI test
   * runners.
   */
  @Rule
  public final TemporaryFolder stagingFilesDir = new TemporaryFolder();


  /**
   * binding on demand rather than in a BeforeClass static method.
   * Subclasses can override this to change the binding options.
   * @return the cluster binding
   */
  protected ClusterBinding demandCreateClusterBinding() throws Exception {
    return createCluster(new JobConf());
  }

  /**
   * Create a job configuration.
   * This creates a new job conf from the yarn
   * cluster configuration then calls
   * {@link #applyCustomConfigOptions(JobConf)} to allow it to be customized.
   * @return the new job configuration.
   * @throws IOException failure
   */
  protected JobConf newJobConf() throws IOException {
    JobConf jobConf = new JobConf(getYarn().getConfig());
    jobConf.addResource(getConfiguration());
    applyCustomConfigOptions(jobConf);
    return jobConf;
  }

  /**
   * Patch the (job) configuration for this committer.
   * @param jobConf configuration to patch
   * @return a configuration which will run this configuration.
   */
  protected Configuration patchConfigurationForCommitter(
      final Configuration jobConf) {
    enableManifestCommitter(jobConf);
    return jobConf;
  }

  /**
   * Override point to let implementations tune the MR Job conf.
   * @param jobConf configuration
   */
  protected void applyCustomConfigOptions(JobConf jobConf) throws IOException {

  }


  /**
   * Assume that scale tests are enabled.
   */
  protected void requireScaleTestsEnabled() {
    assumeScaleTestsEnabled(getConfiguration());
  }

}
