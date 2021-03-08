/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;

import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY_DELETE;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_METASTORE_NULL;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.submit;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.waitForCompletion;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;

/**
 * HADOOP-16721: race condition with delete and rename underneath the same destination
 * directory.
 * This test suite recreates the failure using semaphores to guarantee the failure
 * condition is encountered -then verifies that the rename operation is successful.
 */
public class ITestlRenameDeleteRace extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestlRenameDeleteRace.class);


  /** Many threads for scale performance: {@value}. */
  public static final int EXECUTOR_THREAD_COUNT = 2;

  /**
   * For submitting work.
   */
  private static final ListeningExecutorService EXECUTOR =
      BlockingThreadPoolExecutorService.newInstance(
          EXECUTOR_THREAD_COUNT,
          EXECUTOR_THREAD_COUNT * 2,
          30, TimeUnit.SECONDS,
          "test-operations");

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    String bucketName = getTestBucketName(conf);

    removeBaseAndBucketOverrides(bucketName, conf,
        S3_METADATA_STORE_IMPL,
        DIRECTORY_MARKER_POLICY);
    // use the keep policy to ensure that surplus markers exist
    // to complicate failures
    conf.set(DIRECTORY_MARKER_POLICY, DIRECTORY_MARKER_POLICY_DELETE);
    conf.set(S3_METADATA_STORE_IMPL, S3GUARD_METASTORE_NULL);

    return conf;
  }

  @Test
  public void testDeleteRenameRaceCondition() throws Throwable {
    describe("verify no race between delete and rename");
    final S3AFileSystem fs = getFileSystem();
    final Path path = path(getMethodName());
    Path srcDir = new Path(path, "src");

    // this dir must exist throughout the rename
    Path destDir = new Path(path, "dest");
    // this dir tree will be deleted in a thread which does not
    // complete before the rename exists
    Path destSubdir1 = new Path(destDir, "subdir1");
    Path subfile1 = new Path(destSubdir1, "subfile1");

    // this is the directory we want to copy over under the dest dir
    Path srcSubdir2 = new Path(srcDir, "subdir2");
    Path srcSubfile = new Path(srcSubdir2, "subfile2");
    Path destSubdir2 = new Path(destDir, "subdir2");

    // creates subfile1 and all parents
    ContractTestUtils.touch(fs, subfile1);
    assertIsDirectory(destDir);

    // source subfile
    ContractTestUtils.touch(fs, srcSubfile);

    final BlockingFakeDirMarkerFS blockingFS
        = new BlockingFakeDirMarkerFS();
    blockingFS.initialize(fs.getUri(), fs.getConf());
    // get the semaphore; this ensures that the next attempt to create
    // a fake marker blocks
    try {
      blockingFS.blockBeforCreatingMarker.acquire();
      final CompletableFuture<Path> future = submit(EXECUTOR, () -> {
        LOG.info("deleting {}", destSubdir1);
        blockingFS.delete(destSubdir1, true);
        return destSubdir1;
      });

      // wait for the delete to complete the deletion phase
      blockingFS.signalCreatingFakeParentDirectory.acquire();

      // there is now no destination directory
      assertPathDoesNotExist("should have been implicitly deleted", destDir);

      try {
        // Now attempt the rename in the normal FS.
        LOG.info("renaming {} to {}", srcSubdir2, destSubdir2);
        Assertions.assertThat(fs.rename(srcSubdir2, destSubdir2))
            .describedAs("rename(%s, %s)", srcSubdir2, destSubdir2)
            .isTrue();
      } finally {
        blockingFS.blockBeforCreatingMarker.release();
      }

      // now let the delete complete
      LOG.info("Waiting for delete {} to finish", destSubdir1);
      waitForCompletion(future);
      assertPathExists("must now exist", destDir);
      assertPathExists("must now exist", new Path(destSubdir2, "subfile2"));
      assertPathDoesNotExist("Src dir deleted", srcSubdir2);

    } finally {
      cleanupWithLogger(LOG, blockingFS);
    }

  }

  /**
   * Subclass of S3A FS whose execution of maybeCreateFakeParentDirectory
   * can be choreographed with another thread so as to reliably
   * create the delete/rename race condition.
   */
  private final class BlockingFakeDirMarkerFS extends S3AFileSystem {

    /**
     * Block for entry into maybeCreateFakeParentDirectory(); will be released
     * then.
     */
    private final Semaphore signalCreatingFakeParentDirectory = new Semaphore(
        1);

    /**
     * Semaphore to acquire before the marker can be listed/created.
     */
    private final Semaphore blockBeforCreatingMarker = new Semaphore(1);

    private BlockingFakeDirMarkerFS() {
      signalCreatingFakeParentDirectory.acquireUninterruptibly();
    }

    @Override
    protected void maybeCreateFakeParentDirectory(final Path path)
        throws IOException, AmazonClientException {
      LOG.info("waking anything blocked on the signal semaphore");
      // notify anything waiting
      signalCreatingFakeParentDirectory.release();
      // acquire the semaphore and then create any fake directory
      LOG.info("blocking for creation");
      blockBeforCreatingMarker.acquireUninterruptibly();
      try {
        LOG.info("probing for/creating markers");
        super.maybeCreateFakeParentDirectory(path);
      } finally {
        blockBeforCreatingMarker.release();
      }
    }
  }

}
