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
package org.apache.hadoop.fs.s3a.auth.delegation;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MockJob;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_METASTORE_NULL;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assumeSessionTestsEnabled;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.deployService;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestPropertyInt;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.terminateService;
import static org.apache.hadoop.fs.s3a.auth.RoleTestUtils.probeForAssumedRoleARN;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.*;
import static org.apache.hadoop.fs.s3a.auth.delegation.MiniKerberizedHadoopCluster.assertSecurityEnabled;
import static org.apache.hadoop.fs.s3a.auth.delegation.MiniKerberizedHadoopCluster.closeUserFileSystems;

/**
 * Submit a job with S3 delegation tokens.
 *
 * YARN will not collect DTs unless it is running secure, and turning
 * security on complicates test setup "significantly".
 * Specifically: buts of MR refuse to work on a local FS unless the
 * native libraries are loaded and it can use lower level POSIX APIs
 * for creating files and directories with specific permissions.
 * In production, this is a good thing. In tests, this is not.
 *
 * To address this, Job to YARN communications are mocked.
 * The client-side job submission is as normal, but the implementation
 * of org.apache.hadoop.mapreduce.protocol.ClientProtocol is mock.
 *
 * It's still an ITest though, as it does use S3A as the source and
 * dest so as to collect delegation tokens.
 *
 * It also uses the open street map open bucket, so that there's an extra
 * S3 URL in job submission which can be added as a job resource.
 * This is needed to verify that job resources have their tokens extracted
 * too.
 */
@RunWith(Parameterized.class)
public class ITestDelegatedMRJob extends AbstractDelegationIT {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestDelegatedMRJob.class);

  /**
   * Created in static {@link #setupCluster()} call.
   */
  @SuppressWarnings("StaticNonFinalField")
  private static MiniKerberizedHadoopCluster cluster;

  private final String name;

  private final String tokenBinding;

  private final Text tokenKind;

  /**
   * Created in test setup.
   */
  private MiniMRYarnCluster yarn;

  private Path destPath;

  private static final Path EXTRA_JOB_RESOURCE_PATH
      = new Path("s3a://osm-pds/planet/planet-latest.orc");

  public static final URI jobResource = EXTRA_JOB_RESOURCE_PATH.toUri();

  /**
   * Test array for parameterized test runs.
   * @return a list of parameter tuples.
   */
  @Parameterized.Parameters
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {"session", DELEGATION_TOKEN_SESSION_BINDING, SESSION_TOKEN_KIND},
        {"full", DELEGATION_TOKEN_FULL_CREDENTIALS_BINDING, FULL_TOKEN_KIND},
        {"role", DELEGATION_TOKEN_ROLE_BINDING, ROLE_TOKEN_KIND},
    });
  }

  public ITestDelegatedMRJob(String name, String tokenBinding, Text tokenKind) {
    this.name = name;
    this.tokenBinding = tokenBinding;
    this.tokenKind = tokenKind;
  }

  /***
   * Set up the clusters.
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    JobConf conf = new JobConf();
    assumeSessionTestsEnabled(conf);
    disableFilesystemCaching(conf);
    cluster = deployService(conf, new MiniKerberizedHadoopCluster());
  }

  /**
   * Tear down the cluster.
   */
  @AfterClass
  public static void teardownCluster() throws Exception {
    cluster = terminateService(cluster);
  }

  @Override
  protected YarnConfiguration createConfiguration() {
    Configuration parent = super.createConfiguration();
    YarnConfiguration conf = new YarnConfiguration(parent);
    cluster.patchConfigWithYARNBindings(conf);

    // fail fairly fast
    conf.setInt(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS,
        100);
    conf.setInt(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,
        10_000);

    // turn off DDB for the job resource bucket
    String host = jobResource.getHost();
    conf.set(
        String.format("fs.s3a.bucket.%s.metadatastore.impl", host),
        S3GUARD_METASTORE_NULL);
    // and fix to the main endpoint if the caller has moved
    conf.set(
        String.format("fs.s3a.bucket.%s.endpoint", host), "");

    // set up DTs
    enableDelegationTokens(conf, tokenBinding);
    return conf;
  }

  @Override
  protected YarnConfiguration getConfiguration() {
    return (YarnConfiguration) super.getConfiguration();
  }

  @Override
  public void setup() throws Exception {
    cluster.loginPrincipal();
    super.setup();
    Configuration conf = getConfiguration();

    if (DELEGATION_TOKEN_ROLE_BINDING.equals(tokenBinding)) {
      // get the ARN or skip the test
      probeForAssumedRoleARN(getConfiguration());
    }

    // filesystems are cached across the test so that
    // instrumentation fields can be asserted on

    UserGroupInformation.setConfiguration(conf);
    assertSecurityEnabled();

    LOG.info("Starting MiniMRCluster");
    yarn = deployService(conf,
        new MiniMRYarnCluster("ITestDelegatedMRJob", 1));

  }

  @Override
  public void teardown() throws Exception {
    describe("Teardown operations");
    S3AFileSystem fs = getFileSystem();
    if (fs != null && destPath != null) {
      fs.delete(destPath, true);
    }
    yarn = terminateService(yarn);
    super.teardown();
    closeUserFileSystems(UserGroupInformation.getCurrentUser());
  }


  /**
   * Get the test timeout in seconds.
   * @return the test timeout as set in system properties or the default.
   */
  protected int getTestTimeoutSeconds() {
    return getTestPropertyInt(new Configuration(),
        KEY_TEST_TIMEOUT,
        SCALE_TEST_TIMEOUT_SECONDS);
  }

  @Override
  protected int getTestTimeoutMillis() {
    return getTestTimeoutSeconds() * 1000;
  }

  @Test
  public void testCommonCrawlLookup() throws Throwable {
    FileSystem resourceFS = EXTRA_JOB_RESOURCE_PATH.getFileSystem(
        getConfiguration());
    FileStatus status = resourceFS.getFileStatus(EXTRA_JOB_RESOURCE_PATH);
    LOG.info("Extra job resource is {}", status);
    assertTrue("Not encrypted: " + status, status.isEncrypted());
  }

  @Test
  public void testJobSubmissionCollectsTokens() throws Exception {
    describe("Mock Job test");
    JobConf conf = new JobConf(getConfiguration());

    // the input here is the landsat file; which lets
    // us differentiate source URI from dest URI
    Path input = new Path(DEFAULT_CSVTEST_FILE);
    final FileSystem sourceFS = input.getFileSystem(conf);


    // output is in the writable test FS.
    final S3AFileSystem fs = getFileSystem();

    destPath = path(getMethodName());
    fs.delete(destPath, true);
    fs.mkdirs(destPath);
    Path output = new Path(destPath, "output/");
    output = output.makeQualified(fs.getUri(), fs.getWorkingDirectory());

    MockJob job = new MockJob(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(WordCount.TokenizerMapper.class);
    job.setCombinerClass(WordCount.IntSumReducer.class);
    job.setReducerClass(WordCount.IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);
    job.setMaxMapAttempts(1);
    job.setMaxReduceAttempts(1);

    // and a file for a different store.
    // This is to actually stress the terasort code for which
    // the yarn ResourceLocalizationService was having problems with
    // fetching resources from.
    URI partitionUri = new URI(EXTRA_JOB_RESOURCE_PATH.toString() +
        "#_partition.lst");
    job.addCacheFile(partitionUri);

    describe("Executing Mock Job Submission to %s", output);

    job.submit();
    final JobStatus status = job.getStatus();
    assertEquals("not a mock job",
        MockJob.NAME, status.getSchedulingInfo());
    assertEquals("Job State",
        JobStatus.State.RUNNING, status.getState());

    final Credentials submittedCredentials =
        requireNonNull(job.getSubmittedCredentials(),
            "job submitted credentials");
    final Collection<Token<? extends TokenIdentifier>> tokens
        = submittedCredentials.getAllTokens();

    // log all the tokens for debugging failed test runs
    LOG.info("Token Count = {}", tokens.size());
    for (Token<? extends TokenIdentifier> token : tokens) {
      LOG.info("{}", token);
    }

    // verify the source token exists
    lookupToken(submittedCredentials, sourceFS.getUri(), tokenKind);
    // look up the destination token
    lookupToken(submittedCredentials, fs.getUri(), tokenKind);
    lookupToken(submittedCredentials,
        EXTRA_JOB_RESOURCE_PATH.getFileSystem(conf).getUri(), tokenKind);
  }

}
