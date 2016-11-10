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

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;

import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;


/**
 * Renames a S3 directory by issuing parallel COPY requests.
 */
class ParallelDirectoryRenamer {

  private final S3AFileSystem fs;

  private static final DeleteObjectsRequest.KeyVersion END_OF_KEYS_TO_DELETE = new DeleteObjectsRequest.KeyVersion(null,
          null);

  ParallelDirectoryRenamer(S3AFileSystem fs) {
    this.fs = fs;
  }

  List<CopyContext> rename(String srcKey, String dstKey, S3AFileStatus dstStatus) throws IOException {

    List<DeleteObjectsRequest.KeyVersion> dirKeysToDelete = new ArrayList<>();

    List<CopyContext> copies = new ArrayList<>();

    // A blocking queue that tracks all objects that need to be deleted
    BlockingQueue<DeleteObjectsRequest.KeyVersion> deleteQueue = new LinkedBlockingQueue<>();

    // Used to track if the delete thread was gracefully shutdown
    boolean deleteFutureComplete = false;
    FutureTask<Void> deleteFuture = null;

    try {
      // Launch a thread that will read from the deleteQueue and batch delete any files that have already been copied
      deleteFuture = buildAndStartDeletionThread(deleteQueue);

      if (dstStatus != null && dstStatus.isEmptyDirectory() == Tristate.TRUE) {
        // delete unnecessary fake directory.
        deleteQueue.add(new DeleteObjectsRequest.KeyVersion(dstKey));
      }

      // Used to abort future copy tasks as soon as one copy task fails
      AtomicBoolean copyFailure = new AtomicBoolean(false);

      Path parentPath = fs.keyToPath(srcKey);
      RemoteIterator<LocatedFileStatus> iterator = fs.listFilesAndEmptyDirectories(parentPath, true);
      while (iterator.hasNext()) {

        LocatedFileStatus status = iterator.next();
        String key = fs.pathToKey(status.getPath());
        if (status.isDirectory() && !key.endsWith("/")) {
          key += "/";
        }
        String newDstKey = dstKey + key.substring(srcKey.length());

        DeleteObjectsRequest.KeyVersion deleteKeyVersion = null;
        if (status.isDirectory()) {
          dirKeysToDelete.add(new DeleteObjectsRequest.KeyVersion(key));
        } else {
          deleteKeyVersion = new DeleteObjectsRequest.KeyVersion(key);
        }

        // If no previous file hit a copy failure, copy this file
        if (!copyFailure.get()) {

          ProgressListener.ExceptionReporter progressListener = new ProgressListener.ExceptionReporter(
                  new RenameProgressListener(deleteKeyVersion, deleteQueue, copyFailure));

          copies.add(new CopyContext(fs.copyFileAsync(key, newDstKey, progressListener), key, newDstKey,
                  status.getLen(), progressListener));
        } else {
          // We got a copy failure, so don't bother going through the rest of the files
          break;
        }
      }

      for (CopyContext copyContext : copies) {
        try {
          copyContext.getCopy().waitForCopyResult();
          copyContext.getProgressListener().throwExceptionIfAny();
          fs.incrementWriteOperations();
          fs.getInstrumentation().filesCopied(1, copyContext.getLength());
        } catch (InterruptedException e) {
          throw new RenameFailedException(copyContext.getSrcKey(), copyContext.getDestKey(), e);
        }
      }

      if (copyFailure.get()) {
        throw new RenameFailedException(srcKey, dstKey,
                new IllegalStateException("Progress listener indicated a copy failure, but no exception was thrown"));
      }

      deleteQueue.addAll(dirKeysToDelete);
      deleteQueue.add(END_OF_KEYS_TO_DELETE);

      try {
        deleteFuture.get();
      } catch (ExecutionException | InterruptedException e) {
        throw new RenameFailedException(srcKey, dstKey, e);
      }
      deleteFutureComplete = true;
    } finally {
      if (!deleteFutureComplete) {
        if (deleteFuture != null && !deleteFuture.isDone() && !deleteFuture.isCancelled()) {
          deleteFuture.cancel(true);
        }
      }
    }
    return copies;
  }

  private FutureTask<Void> buildAndStartDeletionThread(BlockingQueue<DeleteObjectsRequest.KeyVersion> deleteQueue) {
    List<DeleteObjectsRequest.KeyVersion> keysToDelete = new ArrayList<>();
    FutureTask<Void> deleteFuture = new FutureTask<>(() -> {
      while (true) {
        while (keysToDelete.size() < fs.getMaxEntriesToDelete()) {
          DeleteObjectsRequest.KeyVersion key = deleteQueue.take();

          // The thread runs until it is given an a message that no more keys need to be deleted (END_OF_KEYS_TO_DELETE)
          if (key == END_OF_KEYS_TO_DELETE) {
            // Delete any remaining keys and exit
            fs.removeKeys(keysToDelete, true, false);
            return null;
          } else {
            keysToDelete.add(key);
          }
        }
        fs.removeKeys(keysToDelete, true, false);
      }
    });

    Thread deleteThread = new Thread(deleteFuture);
    deleteThread.setName("s3a-rename-delete-thread");
    deleteThread.start();
    return deleteFuture;
  }

  /**
   * A {@link ProgressListener} for renames. When the transfer completes, the listener will delete the source key and
   * update any relevant statistics.
   */
  private static class RenameProgressListener implements ProgressListener {

    private final DeleteObjectsRequest.KeyVersion keyVersion;
    private final Queue<DeleteObjectsRequest.KeyVersion> deleteQueue;
    private final AtomicBoolean copyFailure;

    RenameProgressListener(DeleteObjectsRequest.KeyVersion keyVersion,
                           Queue<DeleteObjectsRequest.KeyVersion> deleteQueue,
                           AtomicBoolean copyFailure) {
      this.keyVersion = keyVersion;
      this.deleteQueue = deleteQueue;
      this.copyFailure = copyFailure;
    }

    @Override
    public void progressChanged(ProgressEvent progressEvent) {
      switch (progressEvent.getEventType()) {
        case CLIENT_REQUEST_SUCCESS_EVENT:
          if (keyVersion != null) {
            deleteQueue.add(keyVersion);
          }
          break;
        case CLIENT_REQUEST_FAILED_EVENT:
          copyFailure.set(true);
          break;
        default:
          break;
      }
    }
  }
}
