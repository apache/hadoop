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

package org.apache.hadoop.fs.contract.s3a;

import java.io.FileNotFoundException;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractOpenTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.PREFETCH_OPTIONS;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.setPrefetchOption;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.prepareTestConfiguration;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * S3A contract tests opening files.
 */
@RunWith(Parameterized.class)
public class ITestS3AContractOpen extends AbstractContractOpenTest {

  public ITestS3AContractOpen(final boolean prefetch) {
    this.prefetch = prefetch;
  }

  /**
   * Parameterization.
   */
  @Parameterized.Parameters(name = "prefetch={0}")
  public static Collection<Object[]> params() {
    return PREFETCH_OPTIONS;
  }

  /**
   * Prefetch flag.
   */
  private final boolean prefetch;

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  /**
   * Create a configuration.
   * @return a configuration
   */
  @Override
  protected Configuration createConfiguration() {
    final Configuration conf = prepareTestConfiguration(super.createConfiguration());
    disableFilesystemCaching(conf);
    return setPrefetchOption(conf, prefetch);
  }

  /**
   * S3A always declares zero byte files as encrypted.
   * @return true, always.
   */
  @Override
  protected boolean areZeroByteFilesEncrypted() {
    return true;
  }

  @Test
  public void testOpenFileApplyReadBadName() throws Throwable {
    describe("use the apply sequence to read a whole file");
    Path path = methodPath();
    FileSystem fs = getFileSystem();
    touch(fs, path);
    FileStatus st = fs.getFileStatus(path);
    // The final element of the path is different, so
    // openFile must fail
    FileStatus st2 = new FileStatus(
        0, false,
        st.getReplication(),
        st.getBlockSize(),
        st.getModificationTime(),
        st.getAccessTime(),
        st.getPermission(),
        st.getOwner(),
        st.getGroup(),
        new Path("gopher:///localhost/something.txt"));
    intercept(IllegalArgumentException.class, () ->
        fs.openFile(path)
            .withFileStatus(st2)
            .build());
  }

  /**
   * Pass in a directory reference and expect the openFile call
   * to fail.
   */
  @Test
  public void testOpenFileDirectory() throws Throwable {
    describe("Change the status to a directory");
    Path path = methodPath();
    FileSystem fs = getFileSystem();
    int len = 4096;
    createFile(fs, path, true,
        dataset(len, 0x40, 0x80));
    FileStatus st = fs.getFileStatus(path);
    FileStatus st2 = new FileStatus(
        len, true,
        st.getReplication(),
        st.getBlockSize(),
        st.getModificationTime(),
        st.getAccessTime(),
        st.getPermission(),
        st.getOwner(),
        st.getGroup(),
        path);
    intercept(FileNotFoundException.class, () ->
        fs.openFile(path)
            .withFileStatus(st2)
            .build());
  }

}
