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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;
import org.junit.Test;

/**
 * A class for unit testing JobResourceUploader.
 */
public class TestJobResourceUploader {

  @Test
  public void testAllDefaults() throws IOException {
    ResourceLimitsConf.Builder b = new ResourceLimitsConf.Builder();
    runLimitsTest(b.build(), true, null);
  }

  @Test
  public void testNoLimitsWithResources() throws IOException {
    ResourceLimitsConf.Builder b = new ResourceLimitsConf.Builder();
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
    ResourceLimitsConf.Builder b = new ResourceLimitsConf.Builder();
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
    ResourceLimitsConf.Builder b = new ResourceLimitsConf.Builder();
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
    ResourceLimitsConf.Builder b = new ResourceLimitsConf.Builder();
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
    ResourceLimitsConf.Builder b = new ResourceLimitsConf.Builder();
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
    ResourceLimitsConf.Builder b = new ResourceLimitsConf.Builder();
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
    ResourceLimitsConf.Builder b = new ResourceLimitsConf.Builder();
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

  private enum ResourceViolation {
    NUMBER_OF_RESOURCES, TOTAL_RESOURCE_SIZE, SINGLE_RESOURCE_SIZE;
  }

  private void runLimitsTest(ResourceLimitsConf rlConf,
      boolean checkShouldSucceed, ResourceViolation violation)
      throws IOException {

    if (!checkShouldSucceed && violation == null) {
      Assert.fail("Test is misconfigured. checkShouldSucceed is set to false"
          + " and a ResourceViolation is not specified.");
    }

    JobConf conf = setupJobConf(rlConf);
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

  private JobConf setupJobConf(ResourceLimitsConf rlConf) {
    JobConf conf = new JobConf();
    conf.setInt(MRJobConfig.MAX_RESOURCES, rlConf.maxResources);
    conf.setLong(MRJobConfig.MAX_RESOURCES_MB, rlConf.maxResourcesMB);
    conf.setLong(MRJobConfig.MAX_SINGLE_RESOURCE_MB,
        rlConf.maxSingleResourceMB);

    conf.set("tmpfiles",
        buildPathString("file://tmpFiles", rlConf.numOfTmpFiles));
    conf.set("tmpjars",
        buildPathString("file://tmpjars", rlConf.numOfTmpLibJars));
    conf.set("tmparchives",
        buildPathString("file://tmpArchives", rlConf.numOfTmpArchives));
    conf.set(MRJobConfig.CACHE_ARCHIVES,
        buildPathString("file://cacheArchives", rlConf.numOfDCArchives));
    conf.set(MRJobConfig.CACHE_FILES,
        buildPathString("file://cacheFiles", rlConf.numOfDCFiles));
    if (rlConf.jobJar) {
      conf.setJar("file://jobjar.jar");
    }
    return conf;
  }

  private String buildPathString(String pathPrefix, int numOfPaths) {
    if (numOfPaths < 1) {
      return "";
    } else {
      StringBuilder b = new StringBuilder();
      b.append(pathPrefix + 0);
      for (int i = 1; i < numOfPaths; i++) {
        b.append("," + pathPrefix + i);
      }
      return b.toString();
    }
  }

  final static class ResourceLimitsConf {
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

    static final ResourceLimitsConf DEFAULT = new ResourceLimitsConf();

    private ResourceLimitsConf() {
      this(new Builder());
    }

    private ResourceLimitsConf(Builder builder) {
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

      Builder() {
      }

      Builder setMaxResources(int max) {
        this.maxResources = max;
        return this;
      }

      Builder setMaxResourcesMB(long max) {
        this.maxResourcesMB = max;
        return this;
      }

      Builder setMaxSingleResourceMB(long max) {
        this.maxSingleResourceMB = max;
        return this;
      }

      Builder setNumOfTmpFiles(int num) {
        this.numOfTmpFiles = num;
        return this;
      }

      Builder setNumOfTmpArchives(int num) {
        this.numOfTmpArchives = num;
        return this;
      }

      Builder setNumOfTmpLibJars(int num) {
        this.numOfTmpLibJars = num;
        return this;
      }

      Builder setJobJar(boolean jar) {
        this.jobJar = jar;
        return this;
      }

      Builder setNumOfDCFiles(int num) {
        this.numOfDCFiles = num;
        return this;
      }

      Builder setNumOfDCArchives(int num) {
        this.numOfDCArchives = num;
        return this;
      }

      Builder setSizeOfResource(long sizeMB) {
        this.sizeOfResource = sizeMB;
        return this;
      }

      ResourceLimitsConf build() {
        return new ResourceLimitsConf(this);
      }
    }
  }

  class StubedUploader extends JobResourceUploader {
    StubedUploader(JobConf conf) throws IOException {
      super(FileSystem.getLocal(conf), false);
    }

    @Override
    FileStatus getFileStatus(Map<URI, FileStatus> statCache, Configuration job,
        Path p) throws IOException {
      return mockedStatus;
    }
  }
}
