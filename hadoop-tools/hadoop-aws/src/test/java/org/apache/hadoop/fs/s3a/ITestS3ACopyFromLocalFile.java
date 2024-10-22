/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractCopyFromLocalTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.s3a.S3AContract;

import org.apache.hadoop.fs.Path;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.hadoop.fs.s3a.Constants.OPTIMIZED_COPY_FROM_LOCAL;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test copying files from the local filesystem to S3A.
 * Parameterized on whether or not the optimized
 * copyFromLocalFile is enabled.
 */
@RunWith(Parameterized.class)
public class ITestS3ACopyFromLocalFile extends
        AbstractContractCopyFromLocalTest {
  /**
   * Parameterization.
   */
  @Parameterized.Parameters(name = "enabled={0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {true},
        {false},
    });
  }
  private final boolean enabled;

  public ITestS3ACopyFromLocalFile(final boolean enabled) {
    this.enabled = enabled;
  }

  @Override
  protected Configuration createConfiguration() {
    final Configuration conf = super.createConfiguration();

    removeBaseAndBucketOverrides(getTestBucketName(conf), conf,
        OPTIMIZED_COPY_FROM_LOCAL);
    conf.setBoolean(OPTIMIZED_COPY_FROM_LOCAL, enabled);
    disableFilesystemCaching(conf);
    return conf;
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  @Test
  public void testOptionPropagation() throws Throwable {
    Assertions.assertThat(getFileSystem().hasPathCapability(new Path("/"),
        OPTIMIZED_COPY_FROM_LOCAL))
        .describedAs("path capability of %s", OPTIMIZED_COPY_FROM_LOCAL)
        .isEqualTo(enabled);

  }

  @Test
  public void testLocalFilesOnly() throws Throwable {
    describe("Copying into other file systems must fail");
    Path dest = fileToPath(createTempDirectory("someDir"));

    intercept(IllegalArgumentException.class,
        () -> getFileSystem().copyFromLocalFile(false, true, dest, dest));
  }

  @Test
  public void testOnlyFromLocal() throws Throwable {
    describe("Copying must be from a local file system");
    File source = createTempFile("someFile");
    Path dest = copyFromLocal(source, true);

    intercept(IllegalArgumentException.class,
        () -> getFileSystem().copyFromLocalFile(true, true, dest, dest));
  }

  @Test
  public void testCopyFromLocalWithNoFileScheme() throws IOException {
    describe("Copying from local file with no file scheme to remote s3 destination");
    File source = createTempFile("tempData");
    Path dest = path(getMethodName());

    Path sourcePathWithOutScheme = new Path(source.toURI().getPath());
    assertNull(sourcePathWithOutScheme.toUri().getScheme());
    getFileSystem().copyFromLocalFile(true, true, sourcePathWithOutScheme, dest);
  }
}
