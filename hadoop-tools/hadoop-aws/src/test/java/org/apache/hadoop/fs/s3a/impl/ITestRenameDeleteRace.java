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
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableS3GuardInTestBucket;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.submit;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.waitForCompletion;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;

/**
 * HADOOP-16721: race condition with delete and rename underneath the
 * same destination directory.
 * This test suite recreates the failure using semaphores to
 * guarantee the failure condition is encountered
 * -then verifies that the rename operation is successful.
 */
public class ITestRenameDeleteRace extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestRenameDeleteRace.class);


  /** Many threads for scale performance: {@value}. */
  public static final int EXECUTOR_THREAD_COUNT = 2;

  /**
   * For submitting work.
   */
  private static final BlockingThreadPoolExecutorService EXECUTOR =
      BlockingThreadPoolExecutorService.newInstance(
          EXECUTOR_THREAD_COUNT,
          EXECUTOR_THREAD_COUNT * 2,
          30, TimeUnit.SECONDS,
          "test-operations");

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();

    // use the keep policy to ensure that surplus markers exist
    // to complicate failures
    conf.set(DIRECTORY_MARKER_POLICY, DIRECTORY_MARKER_POLICY_DELETE);
    removeBaseAndBucketOverrides(getTestBucketName(conf),
        conf,
        DIRECTORY_MARKER_POLICY);
    disableS3GuardInTestBucket(conf);
    return conf;
  }

  /**
   * This test uses a subclass of S3AFileSystem to recreate the race between
   * subdirectory delete and rename.
   * The JUnit thread performs the rename, while an executor-submitted
   * thread performs the delete.
   * Semaphores are used to
   * -block the JUnit thread from initiating the rename until the delete
   * has finished the delete phase, and has reached the
   * {@code maybeCreateFakeParentDirectory()} call.
   * A second semaphore is used to block the delete thread from
   * listing and recreating the deleted directory until after
   * the JUnit thread has completed.
   * Together, the two semaphores guarantee that the rename()
   * call will be made at exactly the moment when the destination
   * directory no longer exists.
   */
  @Test
  public void testDeleteRenameRaceCondition() throws Throwable {
    describe("verify no race between delete and rename");

    // the normal FS is used for path setup, verification
    // and the rename call.
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

    // creates subfile1 and all parents, so that
    // dest/subdir1/subfile1 exists as a file;
    // dest/subdir1 and dest are directories without markers
    ContractTestUtils.touch(fs, subfile1);
    assertIsDirectory(destDir);

    // source subfile
    ContractTestUtils.touch(fs, srcSubfile);

    // this is the FS used for delete()
    final BlockingFakeDirMarkerFS blockingFS
        = new BlockingFakeDirMarkerFS();
    blockingFS.initialize(fs.getUri(), fs.getConf());
    // get the semaphore; this ensures that the next attempt to create
    // a fake marker blocks
    blockingFS.blockFakeDirCreation();
    try {
      final CompletableFuture<Path> future = submit(EXECUTOR, () -> {
        LOG.info("deleting {}", destSubdir1);
        blockingFS.delete(destSubdir1, true);
        return destSubdir1;
      });

      // wait for the blocking FS to return from the DELETE call.
      blockingFS.awaitFakeDirCreation();

      try {
        // there is now no destination directory
        assertPathDoesNotExist("should have been implicitly deleted",
            destDir);

        // attempt the rename in the normal FS.
        LOG.info("renaming {} to {}", srcSubdir2, destSubdir2);
        Assertions.assertThat(fs.rename(srcSubdir2, destSubdir2))
            .describedAs("rename(%s, %s)", srcSubdir2, destSubdir2)
            .isTrue();
        // dest dir implicitly exists.
        assertPathExists("must now exist", destDir);
      } finally {
        // release the remaining semaphore so that the deletion thread exits.
        blockingFS.allowFakeDirCreationToProceed();
      }

      // now let the delete complete
      LOG.info("Waiting for delete {} to finish", destSubdir1);
      waitForCompletion(future);

      // everything still exists
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
   * This class is only intended for "single shot" API calls.
   */
  private final class BlockingFakeDirMarkerFS extends S3AFileSystem {

    /**
     * Block for entry into maybeCreateFakeParentDirectory(); will be released
     * then.
     */
    private final Semaphore signalCreatingFakeParentDirectory =
        new Semaphore(1);

    /**
     * Semaphore to acquire before the marker can be listed/created.
     */
    private final Semaphore blockBeforeCreatingMarker = new Semaphore(1);

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
      blockBeforeCreatingMarker.acquireUninterruptibly();
      try {
        LOG.info("probing for/creating markers");
        super.maybeCreateFakeParentDirectory(path);
      } finally {
        // and release the marker for completeness.
        blockBeforeCreatingMarker.release();
      }
    }

    /**
     * Block until fake dir creation is invoked.
     */
    public void blockFakeDirCreation() throws InterruptedException {
      blockBeforeCreatingMarker.acquire();
    }

    /**
     * wait for the blocking FS to return from the DELETE call.
     */
    public void awaitFakeDirCreation() throws InterruptedException {
      LOG.info("Blocking until maybeCreateFakeParentDirectory() is reached");
      signalCreatingFakeParentDirectory.acquire();
    }

    public void allowFakeDirCreationToProceed() {
      LOG.info("Allowing the fake directory LIST/PUT to proceed.");
      blockBeforeCreatingMarker.release();
    }
  }

}
