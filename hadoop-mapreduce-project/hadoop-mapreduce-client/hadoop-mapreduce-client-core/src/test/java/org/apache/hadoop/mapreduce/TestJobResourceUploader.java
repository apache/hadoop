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

package org.apache.hadoop.mapreduce;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.verification.VerificationMode;


/**
 * A class for unit testing JobResourceUploader.
 */
public class TestJobResourceUploader {

  @Test
  public void testStringToPath() throws IOException {
    Configuration conf = new Configuration();
    JobResourceUploader uploader =
        new JobResourceUploader(FileSystem.getLocal(conf), false);

    Assert.assertEquals("Failed: absolute, no scheme, with fragment",
        "/testWithFragment.txt",
        uploader.stringToPath("/testWithFragment.txt#fragment.txt").toString());

    Assert.assertEquals("Failed: absolute, with scheme, with fragment",
        "file:/testWithFragment.txt",
        uploader.stringToPath("file:///testWithFragment.txt#fragment.txt")
            .toString());

    Assert.assertEquals("Failed: relative, no scheme, with fragment",
        "testWithFragment.txt",
        uploader.stringToPath("testWithFragment.txt#fragment.txt").toString());

    Assert.assertEquals("Failed: relative, no scheme, no fragment",
        "testWithFragment.txt",
        uploader.stringToPath("testWithFragment.txt").toString());

    Assert.assertEquals("Failed: absolute, with scheme, no fragment",
        "file:/testWithFragment.txt",
        uploader.stringToPath("file:///testWithFragment.txt").toString());
  }

  @Test
  public void testAllDefaults() throws IOException {
    ResourceConf.Builder b = new ResourceConf.Builder();
    runLimitsTest(b.build(), true, null);
  }

  @Test
  public void testNoLimitsWithResources() throws IOException {
    ResourceConf.Builder b = new ResourceConf.Builder();
    b.setNumOfDCArchives(1);
    b.setNumOfDCFiles(1);
    b.setNumOfTmpArchives(10);
    b.setNumOfTmpFiles(1);
    b.setNumOfTmpLibJars(1);
    b.setJobJar(true);
    b.setSizeOfResource(10);
    runLimitsTest(b.build(), true, null);
  }

  @Test
  public void testAtResourceLimit() throws IOException {
    ResourceConf.Builder b = new ResourceConf.Builder();
    b.setNumOfDCArchives(1);
    b.setNumOfDCFiles(1);
    b.setNumOfTmpArchives(1);
    b.setNumOfTmpFiles(1);
    b.setNumOfTmpLibJars(1);
    b.setJobJar(true);
    b.setMaxResources(6);
    runLimitsTest(b.build(), true, null);
  }

  @Test
  public void testOverResourceLimit() throws IOException {
    ResourceConf.Builder b = new ResourceConf.Builder();
    b.setNumOfDCArchives(1);
    b.setNumOfDCFiles(1);
    b.setNumOfTmpArchives(1);
    b.setNumOfTmpFiles(2);
    b.setNumOfTmpLibJars(1);
    b.setJobJar(true);
    b.setMaxResources(6);
    runLimitsTest(b.build(), false, ResourceViolation.NUMBER_OF_RESOURCES);
  }

  @Test
  public void testAtResourcesMBLimit() throws IOException {
    ResourceConf.Builder b = new ResourceConf.Builder();
    b.setNumOfDCArchives(1);
    b.setNumOfDCFiles(1);
    b.setNumOfTmpArchives(1);
    b.setNumOfTmpFiles(2);
    b.setNumOfTmpLibJars(1);
    b.setJobJar(true);
    b.setMaxResourcesMB(7);
    b.setSizeOfResource(1);
    runLimitsTest(b.build(), true, null);
  }

  @Test
  public void testOverResourcesMBLimit() throws IOException {
    ResourceConf.Builder b = new ResourceConf.Builder();
    b.setNumOfDCArchives(1);
    b.setNumOfDCFiles(2);
    b.setNumOfTmpArchives(1);
    b.setNumOfTmpFiles(2);
    b.setNumOfTmpLibJars(1);
    b.setJobJar(true);
    b.setMaxResourcesMB(7);
    b.setSizeOfResource(1);
    runLimitsTest(b.build(), false, ResourceViolation.TOTAL_RESOURCE_SIZE);
  }

  @Test
  public void testAtSingleResourceMBLimit() throws IOException {
    ResourceConf.Builder b = new ResourceConf.Builder();
    b.setNumOfDCArchives(1);
    b.setNumOfDCFiles(2);
    b.setNumOfTmpArchives(1);
    b.setNumOfTmpFiles(2);
    b.setNumOfTmpLibJars(1);
    b.setJobJar(true);
    b.setMaxSingleResourceMB(1);
    b.setSizeOfResource(1);
    runLimitsTest(b.build(), true, null);
  }

  @Test
  public void testOverSingleResourceMBLimit() throws IOException {
    ResourceConf.Builder b = new ResourceConf.Builder();
    b.setNumOfDCArchives(1);
    b.setNumOfDCFiles(2);
    b.setNumOfTmpArchives(1);
    b.setNumOfTmpFiles(2);
    b.setNumOfTmpLibJars(1);
    b.setJobJar(true);
    b.setMaxSingleResourceMB(1);
    b.setSizeOfResource(10);
    runLimitsTest(b.build(), false, ResourceViolation.SINGLE_RESOURCE_SIZE);
  }

  private String destinationPathPrefix = "hdfs:///destinationPath/";
  private String[] expectedFilesNoFrags =
      { destinationPathPrefix + "tmpFiles0.txt",
          destinationPathPrefix + "tmpFiles1.txt",
          destinationPathPrefix + "tmpFiles2.txt",
          destinationPathPrefix + "tmpFiles3.txt",
          destinationPathPrefix + "tmpFiles4.txt",
          destinationPathPrefix + "tmpjars0.jar",
          destinationPathPrefix + "tmpjars1.jar" };

  private String[] expectedFilesWithFrags =
      { destinationPathPrefix + "tmpFiles0.txt#tmpFilesfragment0.txt",
          destinationPathPrefix + "tmpFiles1.txt#tmpFilesfragment1.txt",
          destinationPathPrefix + "tmpFiles2.txt#tmpFilesfragment2.txt",
          destinationPathPrefix + "tmpFiles3.txt#tmpFilesfragment3.txt",
          destinationPathPrefix + "tmpFiles4.txt#tmpFilesfragment4.txt",
          destinationPathPrefix + "tmpjars0.jar#tmpjarsfragment0.jar",
          destinationPathPrefix + "tmpjars1.jar#tmpjarsfragment1.jar" };

  // We use the local fs for the submitFS in the StubedUploader, so libjars
  // should be replaced with a single path.
  private String[] expectedFilesWithWildcard =
      { destinationPathPrefix + "tmpFiles0.txt",
          destinationPathPrefix + "tmpFiles1.txt",
          destinationPathPrefix + "tmpFiles2.txt",
          destinationPathPrefix + "tmpFiles3.txt",
          destinationPathPrefix + "tmpFiles4.txt",
          "file:///libjars-submit-dir/libjars/*" };

  private String[] expectedArchivesNoFrags =
      { destinationPathPrefix + "tmpArchives0.tgz",
          destinationPathPrefix + "tmpArchives1.tgz" };

  private String[] expectedArchivesWithFrags =
      { destinationPathPrefix + "tmpArchives0.tgz#tmpArchivesfragment0.tgz",
          destinationPathPrefix + "tmpArchives1.tgz#tmpArchivesfragment1.tgz" };

  private String jobjarSubmitDir = "/jobjar-submit-dir";
  private String basicExpectedJobJar = jobjarSubmitDir + "/job.jar";

  @Test
  public void testPathsWithNoFragNoSchemeRelative() throws IOException {
    ResourceConf.Builder b = new ResourceConf.Builder();
    b.setNumOfTmpFiles(5);
    b.setNumOfTmpLibJars(2);
    b.setNumOfTmpArchives(2);
    b.setJobJar(true);
    b.setPathsWithScheme(false);
    b.setPathsWithFrags(false);
    ResourceConf rConf = b.build();
    JobConf jConf = new JobConf();
    JobResourceUploader uploader = new StubedUploader(jConf);

    runTmpResourcePathTest(uploader, rConf, jConf, expectedFilesNoFrags,
        expectedArchivesNoFrags, basicExpectedJobJar);
  }

  @Test
  public void testPathsWithNoFragNoSchemeAbsolute() throws IOException {
    ResourceConf.Builder b = new ResourceConf.Builder();
    b.setNumOfTmpFiles(5);
    b.setNumOfTmpLibJars(2);
    b.setNumOfTmpArchives(2);
    b.setJobJar(true);
    b.setPathsWithFrags(false);
    b.setPathsWithScheme(false);
    b.setAbsolutePaths(true);
    ResourceConf rConf = b.build();
    JobConf jConf = new JobConf();
    JobResourceUploader uploader = new StubedUploader(jConf);

    runTmpResourcePathTest(uploader, rConf, jConf, expectedFilesNoFrags,
        expectedArchivesNoFrags, basicExpectedJobJar);
  }

  @Test
  public void testPathsWithFragNoSchemeAbsolute() throws IOException {
    ResourceConf.Builder b = new ResourceConf.Builder();
    b.setNumOfTmpFiles(5);
    b.setNumOfTmpLibJars(2);
    b.setNumOfTmpArchives(2);
    b.setJobJar(true);
    b.setPathsWithFrags(true);
    b.setPathsWithScheme(false);
    b.setAbsolutePaths(true);
    ResourceConf rConf = b.build();
    JobConf jConf = new JobConf();
    JobResourceUploader uploader = new StubedUploader(jConf);

    runTmpResourcePathTest(uploader, rConf, jConf, expectedFilesWithFrags,
        expectedArchivesWithFrags, basicExpectedJobJar);
  }

  @Test
  public void testPathsWithFragNoSchemeRelative() throws IOException {
    ResourceConf.Builder b = new ResourceConf.Builder();
    b.setNumOfTmpFiles(5);
    b.setNumOfTmpLibJars(2);
    b.setNumOfTmpArchives(2);
    b.setJobJar(true);
    b.setPathsWithFrags(true);
    b.setAbsolutePaths(false);
    b.setPathsWithScheme(false);
    ResourceConf rConf = b.build();
    JobConf jConf = new JobConf();
    JobResourceUploader uploader = new StubedUploader(jConf);

    runTmpResourcePathTest(uploader, rConf, jConf, expectedFilesWithFrags,
        expectedArchivesWithFrags, basicExpectedJobJar);
  }

  @Test
  public void testPathsWithFragSchemeAbsolute() throws IOException {
    ResourceConf.Builder b = new ResourceConf.Builder();
    b.setNumOfTmpFiles(5);
    b.setNumOfTmpLibJars(2);
    b.setNumOfTmpArchives(2);
    b.setJobJar(true);
    b.setPathsWithFrags(true);
    b.setAbsolutePaths(true);
    b.setPathsWithScheme(true);
    ResourceConf rConf = b.build();
    JobConf jConf = new JobConf();
    JobResourceUploader uploader = new StubedUploader(jConf);

    runTmpResourcePathTest(uploader, rConf, jConf, expectedFilesWithFrags,
        expectedArchivesWithFrags, basicExpectedJobJar);
  }

  @Test
  public void testPathsWithNoFragWithSchemeAbsolute() throws IOException {
    ResourceConf.Builder b = new ResourceConf.Builder();
    b.setNumOfTmpFiles(5);
    b.setNumOfTmpLibJars(2);
    b.setNumOfTmpArchives(2);
    b.setJobJar(true);
    b.setPathsWithFrags(false);
    b.setPathsWithScheme(true);
    b.setAbsolutePaths(true);
    ResourceConf rConf = b.build();
    JobConf jConf = new JobConf();
    JobResourceUploader uploader = new StubedUploader(jConf);

    runTmpResourcePathTest(uploader, rConf, jConf, expectedFilesNoFrags,
        expectedArchivesNoFrags, basicExpectedJobJar);
  }

  @Test
  public void testPathsWithNoFragAndWildCard() throws IOException {
    ResourceConf.Builder b = new ResourceConf.Builder();
    b.setNumOfTmpFiles(5);
    b.setNumOfTmpLibJars(4);
    b.setNumOfTmpArchives(2);
    b.setJobJar(true);
    b.setPathsWithFrags(false);
    b.setPathsWithScheme(true);
    b.setAbsolutePaths(true);
    ResourceConf rConf = b.build();
    JobConf jConf = new JobConf();
    JobResourceUploader uploader = new StubedUploader(jConf, true);

    runTmpResourcePathTest(uploader, rConf, jConf, expectedFilesWithWildcard,
        expectedArchivesNoFrags, basicExpectedJobJar);
  }

  @Test
  public void testPathsWithFragsAndWildCard() throws IOException {
    ResourceConf.Builder b = new ResourceConf.Builder();
    b.setNumOfTmpFiles(5);
    b.setNumOfTmpLibJars(2);
    b.setNumOfTmpArchives(2);
    b.setJobJar(true);
    b.setPathsWithFrags(true);
    b.setPathsWithScheme(true);
    b.setAbsolutePaths(true);
    ResourceConf rConf = b.build();
    JobConf jConf = new JobConf();
    JobResourceUploader uploader = new StubedUploader(jConf, true);

    runTmpResourcePathTest(uploader, rConf, jConf, expectedFilesWithFrags,
        expectedArchivesWithFrags, basicExpectedJobJar);
  }

  @Test
  public void testErasureCodingDefault() throws IOException {
    testErasureCodingSetting(true);
  }

  @Test
  public void testErasureCodingDisabled() throws IOException {
    testErasureCodingSetting(false);
  }

  @Test
  public void testOriginalPathEndsInSlash()
      throws IOException, URISyntaxException {
    testOriginalPathWithTrailingSlash(
        new Path(new URI("file:/local/mapred/test/")),
        new Path("hdfs://localhost:1234/home/hadoop/test/"));
  }

  @Test
  public void testOriginalPathIsRoot() throws IOException, URISyntaxException {
    testOriginalPathWithTrailingSlash(
        new Path(new URI("file:/")),
        new Path("hdfs://localhost:1234/home/hadoop/"));
  }

  private void testOriginalPathWithTrailingSlash(Path path,
      Path expectedRemotePath) throws IOException, URISyntaxException {
    Path dstPath = new Path("hdfs://localhost:1234/home/hadoop/");
    DistributedFileSystem fs = mock(DistributedFileSystem.class);
    // make sure that FileUtils.copy() doesn't try to copy anything
    when(fs.mkdirs(any(Path.class))).thenReturn(false);
    when(fs.getUri()).thenReturn(dstPath.toUri());

    JobResourceUploader uploader = new StubedUploader(fs, true, true);
    JobConf jConf = new JobConf();
    Path originalPath = spy(path);
    FileSystem localFs = mock(FileSystem.class);
    FileStatus fileStatus = mock(FileStatus.class);
    when(localFs.getFileStatus(any(Path.class))).thenReturn(fileStatus);
    when(fileStatus.isDirectory()).thenReturn(true);
    when(fileStatus.getPath()).thenReturn(originalPath);

    doReturn(localFs).when(originalPath)
      .getFileSystem(any(Configuration.class));
    when(localFs.getUri()).thenReturn(path.toUri());

    uploader.copyRemoteFiles(dstPath,
        originalPath, jConf, (short) 1);

    ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);
    verify(fs).makeQualified(pathCaptor.capture());
    Assert.assertEquals("Path", expectedRemotePath, pathCaptor.getValue());
  }

  private void testErasureCodingSetting(boolean defaultBehavior)
      throws IOException {
    JobConf jConf = new JobConf();
    // don't set to false if EC remains disabled to check default setting
    if (!defaultBehavior) {
      jConf.setBoolean(MRJobConfig.MR_AM_STAGING_DIR_ERASURECODING_ENABLED,
          true);
    }

    DistributedFileSystem fs = mock(DistributedFileSystem.class);
    Path path = new Path("/");
    when(fs.makeQualified(any(Path.class))).thenReturn(path);
    JobResourceUploader uploader = new StubedUploader(fs, true, false);
    Job job = Job.getInstance(jConf);

    uploader.uploadResources(job, new Path("/test"));

    String replicationPolicyName = SystemErasureCodingPolicies
        .getReplicationPolicy().getName();
    VerificationMode mode = defaultBehavior ? times(1) : never();
    verify(fs, mode).setErasureCodingPolicy(eq(path),
        eq(replicationPolicyName));
  }

  private void runTmpResourcePathTest(JobResourceUploader uploader,
      ResourceConf rConf, JobConf jConf, String[] expectedFiles,
      String[] expectedArchives, String expectedJobJar) throws IOException {
    Job job = rConf.setupJobConf(jConf);
    uploadResources(uploader, job);
    validateResourcePaths(job, expectedFiles, expectedArchives, expectedJobJar);
  }

  private void uploadResources(JobResourceUploader uploader, Job job)
      throws IOException {
    Configuration conf = job.getConfiguration();
    Collection<String> files = conf.getStringCollection("tmpfiles");
    Collection<String> libjars = conf.getStringCollection("tmpjars");
    Collection<String> archives = conf.getStringCollection("tmparchives");
    Map<URI, FileStatus> statCache = new HashMap<>();
    Map<String, Boolean> fileSCUploadPolicies = new HashMap<>();
    String jobJar = job.getJar();
    uploader.uploadFiles(job, files, new Path("/files-submit-dir"), null,
        (short) 3, fileSCUploadPolicies, statCache);
    uploader.uploadArchives(job, archives, new Path("/archives-submit-dir"),
        null, (short) 3, fileSCUploadPolicies, statCache);
    uploader.uploadLibJars(job, libjars, new Path("/libjars-submit-dir"), null,
        (short) 3, fileSCUploadPolicies, statCache);
    uploader.uploadJobJar(job, jobJar, new Path(jobjarSubmitDir), (short) 3,
        statCache);
  }

  private void validateResourcePaths(Job job, String[] expectedFiles,
      String[] expectedArchives, String expectedJobJar)
      throws IOException {
    validateResourcePathsSub(job.getCacheFiles(), expectedFiles);
    validateResourcePathsSub(job.getCacheArchives(), expectedArchives);
    // We use a different job object here because the jobjar was set on a
    // different job object
    Assert.assertEquals("Job jar path is different than expected!",
        expectedJobJar, job.getJar());
  }

  private void validateResourcePathsSub(URI[] actualURIs,
      String[] expectedURIs) {
    List<URI> actualList = Arrays.asList(actualURIs);
    Set<String> expectedSet = new HashSet<>(Arrays.asList(expectedURIs));
    if (actualList.size() != expectedSet.size()) {
      Assert.fail("Expected list of resources (" + expectedSet.size()
          + ") and actual list of resources (" + actualList.size()
          + ") are different lengths!");
    }

    for (URI u : actualList) {
      if (!expectedSet.contains(u.toString())) {
        Assert.fail("Resource list contained unexpected path: " + u.toString());
      }
    }
  }

  private enum ResourceViolation {
    NUMBER_OF_RESOURCES, TOTAL_RESOURCE_SIZE, SINGLE_RESOURCE_SIZE;
  }

  private void runLimitsTest(ResourceConf rlConf, boolean checkShouldSucceed,
      ResourceViolation violation) throws IOException {

    if (!checkShouldSucceed && violation == null) {
      Assert.fail("Test is misconfigured. checkShouldSucceed is set to false"
          + " and a ResourceViolation is not specified.");
    }

    JobConf conf = new JobConf();
    rlConf.setupJobConf(conf);
    JobResourceUploader uploader = new StubedUploader(conf);
    long configuredSizeOfResourceBytes = rlConf.sizeOfResource * 1024 * 1024;
    when(mockedStatus.getLen()).thenReturn(configuredSizeOfResourceBytes);
    when(mockedStatus.isDirectory()).thenReturn(false);
    Map<URI, FileStatus> statCache = new HashMap<URI, FileStatus>();
    try {
      uploader.checkLocalizationLimits(conf,
          conf.getStringCollection("tmpfiles"),
          conf.getStringCollection("tmpjars"),
          conf.getStringCollection("tmparchives"),
          conf.getJar(), statCache);
      Assert.assertTrue("Limits check succeeded when it should have failed.",
          checkShouldSucceed);
    } catch (IOException e) {
      if (checkShouldSucceed) {
        Assert.fail("Limits check failed when it should have succeeded: " + e);
      }
      switch (violation) {
      case NUMBER_OF_RESOURCES:
        if (!e.getMessage().contains(
            JobResourceUploader.MAX_RESOURCE_ERR_MSG)) {
          Assert.fail("Test failed unexpectedly: " + e);
        }
        break;

      case TOTAL_RESOURCE_SIZE:
        if (!e.getMessage().contains(
            JobResourceUploader.MAX_TOTAL_RESOURCE_MB_ERR_MSG)) {
          Assert.fail("Test failed unexpectedly: " + e);
        }
        break;

      case SINGLE_RESOURCE_SIZE:
        if (!e.getMessage().contains(
            JobResourceUploader.MAX_SINGLE_RESOURCE_MB_ERR_MSG)) {
          Assert.fail("Test failed unexpectedly: " + e);
        }
        break;

      default:
        Assert.fail("Test failed unexpectedly: " + e);
        break;
      }
    }
  }

  private final FileStatus mockedStatus = mock(FileStatus.class);

  private static class ResourceConf {
    private final int maxResources;
    private final long maxResourcesMB;
    private final long maxSingleResourceMB;
    private final int numOfTmpFiles;
    private final int numOfTmpArchives;
    private final int numOfTmpLibJars;
    private final boolean jobJar;
    private final int numOfDCFiles;
    private final int numOfDCArchives;
    private final long sizeOfResource;
    private final boolean pathsWithFrags;
    private final boolean pathsWithScheme;
    private final boolean absolutePaths;

    private ResourceConf() {
      this(new Builder());
    }

    private ResourceConf(Builder builder) {
      this.maxResources = builder.maxResources;
      this.maxResourcesMB = builder.maxResourcesMB;
      this.maxSingleResourceMB = builder.maxSingleResourceMB;
      this.numOfTmpFiles = builder.numOfTmpFiles;
      this.numOfTmpArchives = builder.numOfTmpArchives;
      this.numOfTmpLibJars = builder.numOfTmpLibJars;
      this.jobJar = builder.jobJar;
      this.numOfDCFiles = builder.numOfDCFiles;
      this.numOfDCArchives = builder.numOfDCArchives;
      this.sizeOfResource = builder.sizeOfResource;
      this.pathsWithFrags = builder.pathsWithFrags;
      this.pathsWithScheme = builder.pathsWithScheme;
      this.absolutePaths = builder.absolutePaths;
    }

    static class Builder {
      // Defaults
      private int maxResources = 0;
      private long maxResourcesMB = 0;
      private long maxSingleResourceMB = 0;
      private int numOfTmpFiles = 0;
      private int numOfTmpArchives = 0;
      private int numOfTmpLibJars = 0;
      private boolean jobJar = false;
      private int numOfDCFiles = 0;
      private int numOfDCArchives = 0;
      private long sizeOfResource = 0;
      private boolean pathsWithFrags = false;
      private boolean pathsWithScheme = false;
      private boolean absolutePaths = true;

      private Builder() {
      }

      private Builder setMaxResources(int max) {
        this.maxResources = max;
        return this;
      }

      private Builder setMaxResourcesMB(long max) {
        this.maxResourcesMB = max;
        return this;
      }

      private Builder setMaxSingleResourceMB(long max) {
        this.maxSingleResourceMB = max;
        return this;
      }

      private Builder setNumOfTmpFiles(int num) {
        this.numOfTmpFiles = num;
        return this;
      }

      private Builder setNumOfTmpArchives(int num) {
        this.numOfTmpArchives = num;
        return this;
      }

      private Builder setNumOfTmpLibJars(int num) {
        this.numOfTmpLibJars = num;
        return this;
      }

      private Builder setJobJar(boolean jar) {
        this.jobJar = jar;
        return this;
      }

      private Builder setNumOfDCFiles(int num) {
        this.numOfDCFiles = num;
        return this;
      }

      private Builder setNumOfDCArchives(int num) {
        this.numOfDCArchives = num;
        return this;
      }

      private Builder setSizeOfResource(long sizeMB) {
        this.sizeOfResource = sizeMB;
        return this;
      }

      private Builder setPathsWithFrags(boolean fragments) {
        this.pathsWithFrags = fragments;
        return this;
      }

      private Builder setPathsWithScheme(boolean scheme) {
        this.pathsWithScheme = scheme;
        return this;
      }

      private Builder setAbsolutePaths(boolean absolute) {
        this.absolutePaths = absolute;
        return this;
      }

      ResourceConf build() {
        return new ResourceConf(this);
      }
    }

    private Job setupJobConf(JobConf conf) throws IOException {
      conf.set("tmpfiles",
          buildPathString("tmpFiles", this.numOfTmpFiles, ".txt"));
      conf.set("tmpjars",
          buildPathString("tmpjars", this.numOfTmpLibJars, ".jar"));
      conf.set("tmparchives",
          buildPathString("tmpArchives", this.numOfTmpArchives, ".tgz"));
      conf.set(MRJobConfig.CACHE_ARCHIVES, buildDistributedCachePathString(
          "cacheArchives", this.numOfDCArchives, ".tgz"));
      conf.set(MRJobConfig.CACHE_FILES, buildDistributedCachePathString(
          "cacheFiles", this.numOfDCFiles, ".txt"));
      if (this.jobJar) {
        String fragment = "";
        if (pathsWithFrags) {
          fragment = "#jobjarfrag.jar";
        }
        if (pathsWithScheme) {
          conf.setJar("file:///jobjar.jar" + fragment);
        } else {
          if (absolutePaths) {
            conf.setJar("/jobjar.jar" + fragment);
          } else {
            conf.setJar("jobjar.jar" + fragment);
          }
        }
      }
      conf.setInt(MRJobConfig.MAX_RESOURCES, this.maxResources);
      conf.setLong(MRJobConfig.MAX_RESOURCES_MB, this.maxResourcesMB);
      conf.setLong(MRJobConfig.MAX_SINGLE_RESOURCE_MB,
          this.maxSingleResourceMB);
      return new Job(conf);
    }

    // We always want absolute paths with a scheme in the DistributedCache, so
    // we use a separate method to construct the path string.
    private String buildDistributedCachePathString(String pathPrefix,
        int numOfPaths, String extension) {
      if (numOfPaths < 1) {
        return "";
      } else {
        StringBuilder b = new StringBuilder();
        b.append(buildPathStringSub(pathPrefix, "file:///" + pathPrefix,
            extension, 0));
        for (int i = 1; i < numOfPaths; i++) {
          b.append("," + buildPathStringSub(pathPrefix, "file:///" + pathPrefix,
              extension, i));
        }
        return b.toString();
      }
    }

    private String buildPathString(String pathPrefix, int numOfPaths,
        String extension) {
      if (numOfPaths < 1) {
        return "";
      } else {
        StringBuilder b = new StringBuilder();
        String processedPath;
        if (pathsWithScheme) {
          processedPath = "file:///" + pathPrefix;
        } else {
          if (absolutePaths) {
            processedPath = "/" + pathPrefix;
          } else {
            processedPath = pathPrefix;
          }
        }
        b.append(buildPathStringSub(pathPrefix, processedPath, extension, 0));
        for (int i = 1; i < numOfPaths; i++) {
          b.append(","
              + buildPathStringSub(pathPrefix, processedPath, extension, i));
        }
        return b.toString();
      }
    }

    private String buildPathStringSub(String pathPrefix, String processedPath,
        String extension, int num) {
      if (pathsWithFrags) {
        return processedPath + num + extension + "#" + pathPrefix + "fragment"
            + num + extension;
      } else {
        return processedPath + num + extension;
      }
    }
  }

  private class StubedUploader extends JobResourceUploader {
    private boolean callOriginalCopy = false;

    StubedUploader(JobConf conf) throws IOException {
      this(conf, false);
    }

    StubedUploader(JobConf conf, boolean useWildcard) throws IOException {
      super(FileSystem.getLocal(conf), useWildcard);
    }

    StubedUploader(FileSystem fs, boolean useWildcard,
        boolean callOriginalCopy) throws IOException {
      super(fs, useWildcard);
      this.callOriginalCopy = callOriginalCopy;
    }

    @Override
    FileStatus getFileStatus(Map<URI, FileStatus> statCache, Configuration job,
        Path p) throws IOException {
      return mockedStatus;
    }

    @Override
    boolean mkdirs(FileSystem fs, Path dir, FsPermission permission)
        throws IOException {
      // Do nothing. Stubbed out to avoid side effects. We don't actually need
      // to create submit dirs.
      return true;
    }

    @Override
    Path copyRemoteFiles(Path parentDir, Path originalPath, Configuration conf,
        short replication) throws IOException {
      if (callOriginalCopy) {
        return super.copyRemoteFiles(
            parentDir, originalPath, conf, replication);
      } else {
        return new Path(destinationPathPrefix + originalPath.getName());
      }
    }

    @Override
    void copyJar(Path originalJarPath, Path submitJarFile, short replication)
        throws IOException {
      // Do nothing. Stubbed out to avoid side effects. We don't actually need
      // to copy the jar to the remote fs.
    }
  }
}
