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
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.assertj.core.api.Assumptions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.fs.s3a.commit.impl.CommitContext;
import org.apache.hadoop.fs.s3a.commit.impl.CommitOperations;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;
import org.apache.hadoop.fs.s3a.test.SdkFaultInjector;

import static org.apache.hadoop.fs.contract.ContractTestUtils.verifyFileContents;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_MULTIPART_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_OPERATIONS_PURGE_UPLOADS;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BUFFER;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BUFFER_ARRAY;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BUFFER_DISK;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BYTEBUFFER;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_PERFORMANCE;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.AUDIT_EXECUTION_INTERCEPTORS;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.BASE;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.MAGIC_PATH_PREFIX;

/**
 * Test upload recovery by injecting failures into the response chain.
 * The tests are parameterized on upload buffering.
 * <p>
 * The test case {@link #testCommitOperations()} is independent of this option;
 * the test parameterization only runs this once.
 * A bit inelegant but as the fault injection code is shared and the problem "adjacent"
 * this isolates all forms of upload recovery into the same test class without
 * wasting time with duplicate uploads.
 * <p>
 * Fault injection is implemented in {@link SdkFaultInjector}.
 */
@RunWith(Parameterized.class)
public class ITestUploadRecovery extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestUploadRecovery.class);

  /**
   * Parameterization.
   */
  @Parameterized.Parameters(name = "{0}-commit-{1}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {FAST_UPLOAD_BUFFER_ARRAY, true},
        {FAST_UPLOAD_BUFFER_DISK, false},
        {FAST_UPLOAD_BYTEBUFFER, false},
    });
  }

  private static final String JOB_ID = UUID.randomUUID().toString();

  /**
   * Upload size for the committer test.
   */
  public static final int COMMIT_FILE_UPLOAD_SIZE = 1024 * 2;

  /**
   * should the commit test be included?
   */
  private final boolean includeCommitTest;

  /**
   * Buffer type for this test run.
   */
  private final String buffer;

  /**
   * Parameterized test suite.
   * @param buffer buffer type
   * @param includeCommitTest should the commit upload test be included?
   */
  public ITestUploadRecovery(final String buffer, final boolean includeCommitTest) {
    this.includeCommitTest = includeCommitTest;
    this.buffer = buffer;
  }


  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    SdkFaultInjector.addFaultInjection(conf);

    removeBaseAndBucketOverrides(conf,
        DIRECTORY_OPERATIONS_PURGE_UPLOADS,
        FAST_UPLOAD_BUFFER,
        FS_S3A_CREATE_PERFORMANCE);

    // select buffer location
    conf.set(FAST_UPLOAD_BUFFER, buffer);

    // save overhead on file creation
    conf.setBoolean(FS_S3A_CREATE_PERFORMANCE, true);

    // guarantees teardown will abort pending uploads.
    conf.setBoolean(DIRECTORY_OPERATIONS_PURGE_UPLOADS, true);
    // use the fault injector
    conf.set(AUDIT_EXECUTION_INTERCEPTORS, SdkFaultInjector.class.getName());
    return conf;
  }

  /**
   * Setup MUST set up the evaluator before the FS is created.
   */
  @Override
  public void setup() throws Exception {
    SdkFaultInjector.setRequestFailureCount(2);
    SdkFaultInjector.resetEvaluator();
    super.setup();
  }

  /**
   * Verify that failures of simple PUT requests can be recovered from.
   */
  @Test
  public void testPutRecovery() throws Throwable {
    describe("test put recovery");
    final S3AFileSystem fs = getFileSystem();
    final Path path = methodPath();
    SdkFaultInjector.setEvaluator(ITestUploadRecovery::isPutRequest);
    SdkFaultInjector.setRequestFailureCount(2);
    final FSDataOutputStream out = fs.create(path);
    out.writeUTF("utfstring");
    out.close();
  }

  /**
   * Validate recovery of multipart uploads within a magic write sequence.
   */
  @Test
  public void testMagicWriteRecovery() throws Throwable {
    describe("test magic write recovery with multipart uploads");
    final S3AFileSystem fs = getFileSystem();

    Assumptions.assumeThat(fs.isMultipartUploadEnabled())
        .describedAs("Multipart upload is disabled")
        .isTrue();

    final Path path = new Path(methodPath(),
        MAGIC_PATH_PREFIX + buffer + "/" + BASE + "/file.txt");

    SdkFaultInjector.setEvaluator(ITestUploadRecovery::isPartUpload);
    final FSDataOutputStream out = fs.create(path);

    // set the failure count again
    SdkFaultInjector.setRequestFailureCount(2);

    out.writeUTF("utfstring");
    out.close();
  }

  /**
   * Test the commit operations iff {@link #includeCommitTest} is true.
   */
  @Test
  public void testCommitOperations() throws Throwable {
    Assumptions.assumeThat(includeCommitTest)
        .describedAs("commit test excluded")
        .isTrue();
    describe("test staging upload");
    final S3AFileSystem fs = getFileSystem();
    final byte[] dataset = ContractTestUtils.dataset(COMMIT_FILE_UPLOAD_SIZE, '0', 36);
    File tempFile = File.createTempFile("commit", ".txt");
    FileUtils.writeByteArrayToFile(tempFile, dataset);
    CommitOperations actions = new CommitOperations(fs);
    Path dest = methodPath();
    SdkFaultInjector.setRequestFailureCount(2);
    SdkFaultInjector.setEvaluator(ITestUploadRecovery::isPartUpload);

    // upload from the local FS to the S3 store
    SinglePendingCommit commit =
        actions.uploadFileToPendingCommit(tempFile,
            dest,
            null,
            DEFAULT_MULTIPART_SIZE,
            () -> {});

    // commit
    SdkFaultInjector.setRequestFailureCount(2);
    SdkFaultInjector.setEvaluator(ITestUploadRecovery::isCommitCompletion);

    try (CommitContext commitContext
             = actions.createCommitContextForTesting(dest, JOB_ID, 0)) {
      commitContext.commitOrFail(commit);
    }
    // make sure the saved data is as expected
    verifyFileContents(fs, dest, dataset);
  }

  /**
   * Is the response being processed from a PUT request?
   * @param context request context.
   * @return true if the request is of the right type.
   */
  private static boolean isPutRequest(final Context.ModifyHttpResponse context) {
    return context.httpRequest().method().equals(SdkHttpMethod.PUT);
  }

  /**
   * Is the response being processed from any POST request?
   * @param context request context.
   * @return true if the request is of the right type.
   */
  private static boolean isPostRequest(final Context.ModifyHttpResponse context) {
    return context.httpRequest().method().equals(SdkHttpMethod.POST);
  }

  /**
   * Is the request a commit completion request?
   * @param context response
   * @return true if the predicate matches
   */
  private static boolean isCommitCompletion(final Context.ModifyHttpResponse context) {
    return context.request() instanceof CompleteMultipartUploadRequest;
  }

  /**
   * Is the request a part upload?
   * @param context response
   * @return true if the predicate matches
   */
  private static boolean isPartUpload(final Context.ModifyHttpResponse context) {
    return context.request() instanceof UploadPartRequest;
  }


}
