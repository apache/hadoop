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

package org.apache.hadoop.fs.gs;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;

import org.apache.hadoop.thirdparty.com.google.common.base.Ascii;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.RateLimiter;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkState;
import static org.apache.hadoop.thirdparty.com.google.common.base.Strings.isNullOrEmpty;

class GoogleHadoopOutputStream extends OutputStream
    implements StreamCapabilities, Syncable {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleHadoopOutputStream.class);

  // Prefix used for all temporary files created by this stream.
  private static final String TMP_FILE_PREFIX = "_GHFS_SYNC_TMP_FILE_";

  // Temporary files don't need to contain the desired attributes of the final destination file
  // since metadata settings get clobbered on final compose() anyways; additionally, due to
  // the way we pick temp file names and already ensured directories for the destination file,
  // we can optimize tempfile creation by skipping various directory checks.
  private static final CreateFileOptions TMP_FILE_CREATE_OPTIONS =
      CreateFileOptions.builder().setEnsureNoDirectoryConflict(false).build();

  // Deletion of temporary files occurs asynchronously for performance reasons, but in-flight
  // deletions are awaited on close() so as long as all output streams are closed, there should
  // be no remaining in-flight work occurring inside this threadpool.
  private static final ExecutorService TMP_FILE_CLEANUP_THREADPOOL =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setNameFormat("ghfs-output-stream-sync-cleanup-%d")
              .setDaemon(true)
              .build());

  private final GoogleHadoopFileSystem ghfs;

  private final CreateObjectOptions composeObjectOptions;

  // Path of the file to write to.
  private final URI dstGcsPath;

  /**
   * The last known generationId of the {@link #dstGcsPath} file, or possibly {@link
   * StorageResourceId#UNKNOWN_GENERATION_ID} if unknown.
   */
  private long dstGenerationId;

  // GCS path pointing at the "tail" file which will be appended to the destination
  // on hflush()/hsync() call.
  private URI tmpGcsPath;

  /**
   * Stores the component index corresponding to {@link #tmpGcsPath}. If close() is called, the
   * total number of components in the {@link #dstGcsPath} will be {@code tmpIndex + 1}.
   */
  private int tmpIndex;

  // OutputStream pointing at the "tail" file which will be appended to the destination
  // on hflush()/hsync() call.
  private OutputStream tmpOut;

  private final RateLimiter syncRateLimiter;

  // List of temporary file-deletion futures accrued during the lifetime of this output stream.
  private final List<Future<Void>> tmpDeletionFutures = new ArrayList<>();

  // Statistics tracker provided by the parent GoogleHadoopFileSystem for recording
  // numbers of bytes written.
  private final FileSystem.Statistics statistics;

  /**
   * Constructs an instance of GoogleHadoopOutputStream object.
   *
   * @param ghfs              Instance of {@link GoogleHadoopFileSystem}.
   * @param dstGcsPath        Path of the file to write to.
   * @param statistics        File system statistics object.
   * @param createFileOptions options for file creation
   * @throws IOException if an IO error occurs.
   */
  GoogleHadoopOutputStream(GoogleHadoopFileSystem ghfs, URI dstGcsPath,
      CreateFileOptions createFileOptions, FileSystem.Statistics statistics) throws IOException {
    LOG.trace("GoogleHadoopOutputStream(gcsPath: {}, createFileOptions: {})", dstGcsPath,
        createFileOptions);
    this.ghfs = ghfs;
    this.dstGcsPath = dstGcsPath;
    this.statistics = statistics;

    Duration minSyncInterval = ghfs.getFileSystemConfiguration().getMinSyncInterval();

    this.syncRateLimiter =
        minSyncInterval.isNegative() || minSyncInterval.isZero()
            ? null
            : RateLimiter.create(/* permitsPerSecond= */ 1_000.0 / minSyncInterval.toMillis());
    this.composeObjectOptions =
        GoogleCloudStorageFileSystem.objectOptionsFromFileOptions(
            createFileOptions.toBuilder()
                // Set write mode to OVERWRITE because we use compose operation to append new data
                // to an existing object
                .setWriteMode(CreateFileOptions.WriteMode.OVERWRITE)
                .build());

    if (createFileOptions.getWriteMode() == CreateFileOptions.WriteMode.APPEND) {
      // When appending first component has to go to new temporary file.
      this.tmpGcsPath = getNextTmpPath();
      this.tmpIndex = 1;
    } else {
      // The first component of the stream will go straight to the destination filename to optimize
      // the case where no hsync() or a single hsync() is called during the lifetime of the stream;
      // committing the first component thus doesn't require any compose() call under the hood.
      this.tmpGcsPath = dstGcsPath;
      this.tmpIndex = 0;
    }

    this.tmpOut =
        createOutputStream(
            ghfs.getGcsFs(),
            tmpGcsPath,
            tmpIndex == 0 ? createFileOptions : TMP_FILE_CREATE_OPTIONS);
    this.dstGenerationId = StorageResourceId.UNKNOWN_GENERATION_ID;
  }

  private OutputStream createOutputStream(GoogleCloudStorageFileSystem gcsfs, URI gcsPath,
      CreateFileOptions options)
      throws IOException {
    WritableByteChannel channel;
    try {
      channel = gcsfs.create(gcsPath, options);
    } catch (java.nio.file.FileAlreadyExistsException e) {

      throw (FileAlreadyExistsException) new FileAlreadyExistsException(
          String.format("'%s' already exists", gcsPath)).initCause(e);
    }
    OutputStream outputStream = Channels.newOutputStream(channel);
    int bufferSize = gcsfs.getConfiguration().getOutStreamBufferSize();
    return bufferSize > 0 ? new BufferedOutputStream(outputStream, bufferSize) : outputStream;
  }

  @Override
  public void write(int b) throws IOException {
    throwIfNotOpen();
    tmpOut.write(b);
    statistics.incrementBytesWritten(1);
    statistics.incrementWriteOps(1);
  }

  @Override
  public void write(@Nonnull byte[] b, int offset, int len) throws IOException {
    throwIfNotOpen();
    tmpOut.write(b, offset, len);
    statistics.incrementBytesWritten(len);
    statistics.incrementWriteOps(1);
  }

  private void commitTempFile() throws IOException {
    // TODO: return early when 0 bytes have been written in the temp files
    tmpOut.close();

    long tmpGenerationId = StorageResourceId.UNKNOWN_GENERATION_ID;
    LOG.trace(
        "tmpOut is an instance of {}; expected generationId {}.",
        tmpOut.getClass(), tmpGenerationId);

    // On the first component, tmpGcsPath will equal finalGcsPath, and no compose() call is
    // necessary. Otherwise, we compose in-place into the destination object and then delete
    // the temporary object.
    if (dstGcsPath.equals(tmpGcsPath)) {
      // First commit was direct to the destination; the generationId of the object we just
      // committed will be used as the destination generation id for future compose calls.
      dstGenerationId = tmpGenerationId;
    } else {
      StorageResourceId dstId =
          StorageResourceId.fromUriPath(
              dstGcsPath, /* allowEmptyObjectName= */ false, dstGenerationId);
      StorageResourceId tmpId =
          StorageResourceId.fromUriPath(
              tmpGcsPath, /* allowEmptyObjectName= */ false, tmpGenerationId);
      checkState(
          dstId.getBucketName().equals(tmpId.getBucketName()),
          "Destination bucket in path '%s' doesn't match temp file bucket in path '%s'",
          dstGcsPath,
          tmpGcsPath);
      GoogleCloudStorageFileSystem gcs = ghfs.getGcsFs();
      GoogleCloudStorageItemInfo composedObject =
          gcs.composeObjects(ImmutableList.of(dstId, tmpId), dstId, composeObjectOptions);
      dstGenerationId = composedObject.getContentGeneration();
      tmpDeletionFutures.add(
          TMP_FILE_CLEANUP_THREADPOOL.submit(
              () -> {
                gcs.delete(ImmutableList.of(tmpId));
                return null;
              }));
    }
  }

  @Override
  public void close() throws IOException {
    LOG.trace(
        "close(): temp tail file: %s final destination: {}", tmpGcsPath, dstGcsPath);

    if (tmpOut == null) {
      LOG.trace("close(): Ignoring; stream already closed.");
      return;
    }

    commitTempFile();

    try {
      tmpOut.close();
    } finally {
      tmpOut = null;
    }
    tmpGcsPath = null;
    tmpIndex = -1;

    LOG.trace("close(): Awaiting {} deletionFutures", tmpDeletionFutures.size());
    for (Future<?> deletion : tmpDeletionFutures) {
      try {
        deletion.get();
      } catch (ExecutionException | InterruptedException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new IOException(
            String.format(
                "Failed to delete temporary files while closing stream: '%s'", dstGcsPath),
            e);
      }
    }
  }

  private void throwIfNotOpen() throws IOException {
    if (tmpOut == null) {
      throw new ClosedChannelException();
    }
  }

  @Override
  public boolean hasCapability(String capability) {
    checkArgument(!isNullOrEmpty(capability), "capability must not be null or empty string");
    switch (Ascii.toLowerCase(capability)) {
    case StreamCapabilities.HFLUSH:
    case StreamCapabilities.HSYNC:
      return syncRateLimiter != null;
    case StreamCapabilities.IOSTATISTICS:
      return false; // TODO: Add support
    default:
      return false;
    }
  }

  /**
   * There is no way to flush data to become available for readers without a full-fledged hsync(),
   * If the output stream is only syncable, this method is a no-op. If the output stream is also
   * flushable, this method will simply use the same implementation of hsync().
   *
   * <p>If it is rate limited, unlike hsync(), which will try to acquire the permits and block, it
   * will do nothing.
   */
  @Override
  public void hflush() throws IOException {
    LOG.trace("hflush(): {}", dstGcsPath);

    long startMs = System.currentTimeMillis();
    throwIfNotOpen();
    // If rate limit not set or permit acquired than use hsync()
    if (syncRateLimiter == null || syncRateLimiter.tryAcquire()) {
      LOG.trace("hflush() uses hsyncInternal() for {}", dstGcsPath);
      hsyncInternal(startMs);
      return;
    }
    LOG.trace(
        "hflush(): No-op due to rate limit ({}): readers will *not* yet see flushed data for {}",
        syncRateLimiter, dstGcsPath);

  }

  @Override
  public void hsync() throws IOException {
    LOG.trace("hsync(): {}", dstGcsPath);

    long startMs = System.currentTimeMillis();
    throwIfNotOpen();
    if (syncRateLimiter != null) {
      LOG.trace(
          "hsync(): Rate limited ({}) with blocking permit acquisition for {}",
          syncRateLimiter, dstGcsPath);
      syncRateLimiter.acquire();
    }
    hsyncInternal(startMs);

  }

  /** Internal implementation of hsync, can be reused by hflush() as well. */
  private void hsyncInternal(long startMs) throws IOException {
    LOG.trace(
        "hsyncInternal(): Committing tail file {} to final destination {}", tmpGcsPath, dstGcsPath);
    commitTempFile();

    // Use a different temporary path for each temporary component to reduce the possible avenues of
    // race conditions in the face of low-level retries, etc.
    ++tmpIndex;
    tmpGcsPath = getNextTmpPath();

    LOG.trace(
        "hsync(): Opening next temporary tail file {} at {} index", tmpGcsPath, tmpIndex);
    tmpOut = createOutputStream(ghfs.getGcsFs(), tmpGcsPath, TMP_FILE_CREATE_OPTIONS);

    long finishMs = System.currentTimeMillis();
    LOG.trace("Took {}ms to sync() for {}", finishMs - startMs, dstGcsPath);
  }
  /** Returns URI to be used for the next temp "tail" file in the series. */
  private URI getNextTmpPath() {
    Path basePath = ghfs.getHadoopPath(dstGcsPath);
    Path tempPath =
        new Path(
            basePath.getParent(),
            String.format(
                "%s%s.%d.%s", TMP_FILE_PREFIX, basePath.getName(), tmpIndex, UUID.randomUUID()));
    return ghfs.getGcsPath(tempPath);
  }
}
