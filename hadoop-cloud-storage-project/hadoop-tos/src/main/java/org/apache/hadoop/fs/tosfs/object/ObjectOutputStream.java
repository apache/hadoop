/*
 * ByteDance Volcengine EMR, Copyright 2022.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.tosfs.object;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.object.staging.FileStagingPart;
import org.apache.hadoop.fs.tosfs.object.staging.StagingPart;
import org.apache.hadoop.fs.tosfs.util.CommonUtils;
import org.apache.hadoop.fs.tosfs.util.UUIDUtils;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ObjectOutputStream extends OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(ObjectOutputStream.class);

  private final ObjectStorage storage;
  private final ExecutorService uploadPool;
  private long totalWroteSize;
  private final String destKey;
  private final String destScheme;
  private final long multiUploadThreshold;
  private final long byteSizePerPart;
  private final int stagingBufferSize;
  private final boolean allowPut;
  private final List<File> stagingDirs;
  private final List<StagingPart> stagingParts = Lists.newArrayList();

  // For multipart uploads.
  private final AtomicInteger partNumGetter = new AtomicInteger(0);
  private MultipartUpload multipartUpload = null;
  private final List<CompletableFuture<Part>> results = Lists.newArrayList();

  private StagingPart curPart;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public ObjectOutputStream(ObjectStorage storage, ExecutorService threadPool, Configuration conf,
      Path dest, boolean allowPut) {
    this.storage = storage;
    this.uploadPool = threadPool;
    this.destScheme = dest.toUri().getScheme();
    this.totalWroteSize = 0;
    this.destKey = createDestKey(dest);
    this.multiUploadThreshold = conf.getLong(ConfKeys.FS_MULTIPART_THRESHOLD.key(destScheme),
        ConfKeys.FS_MULTIPART_THRESHOLD_DEFAULT);
    this.byteSizePerPart = conf.getLong(ConfKeys.FS_MULTIPART_SIZE.key(destScheme),
        ConfKeys.FS_MULTIPART_SIZE_DEFAULT);
    this.stagingBufferSize = conf.getInt(ConfKeys.FS_MULTIPART_STAGING_BUFFER_SIZE.key(destScheme),
        ConfKeys.FS_MULTIPART_STAGING_BUFFER_SIZE_DEFAULT);
    this.allowPut = allowPut;
    this.stagingDirs = createStagingDirs(conf, destScheme);

    if (!allowPut) {
      this.multipartUpload = storage.createMultipartUpload(destKey);
    }
  }

  private static List<File> createStagingDirs(Configuration conf, String scheme) {
    String[] dirs = conf.getStrings(ConfKeys.FS_MULTIPART_STAGING_DIR.key(scheme),
        ConfKeys.FS_MULTIPART_STAGING_DIR_DEFAULT);
    Preconditions.checkArgument(dirs != null && dirs.length > 0, "'%s' cannot be an empty list",
        ConfKeys.FS_MULTIPART_STAGING_DIR.key(scheme));

    List<File> stagingDirs = new ArrayList<>();
    for (String dir : dirs) {
      // Create the directory if not exist.
      File stagingDir = new File(dir);
      if (!stagingDir.exists() && stagingDir.mkdirs()) {
        Preconditions.checkArgument(stagingDir.setWritable(true, false),
            "Failed to change staging dir permission to writable, please check %s with value %s",
            ConfKeys.FS_MULTIPART_STAGING_DIR.key(scheme), dir);
        Preconditions.checkArgument(stagingDir.setReadable(true, false),
            "Failed to change staging dir permission to readable, please check %s with value %s",
            ConfKeys.FS_MULTIPART_STAGING_DIR.key(scheme), dir);
      } else {
        Preconditions.checkArgument(stagingDir.exists(),
            "Failed to create staging dir, please check %s with value %s",
            ConfKeys.FS_MULTIPART_STAGING_DIR.key(scheme), dir);
        Preconditions.checkArgument(stagingDir.isDirectory(),
            "Staging dir should be a directory, please check %s with value %s",
            ConfKeys.FS_MULTIPART_STAGING_DIR.key(scheme), dir);
      }
      stagingDirs.add(stagingDir);
    }
    return stagingDirs;
  }

  private File chooseStagingDir() {
    // Choose a random directory from the staging dirs as the candidate staging dir.
    return stagingDirs.get(ThreadLocalRandom.current().nextInt(stagingDirs.size()));
  }

  @Override
  public void write(int b) throws IOException {
    write(new byte[]{(byte) b}, 0, 1);
  }

  protected String createDestKey(Path dest) {
    return ObjectUtils.pathToKey(dest);
  }

  @Override
  public synchronized void write(byte[] buf, int off, int len) throws IOException {
    if (len == 0) {
      return;
    }
    Preconditions.checkArgument(off >= 0 && off < buf.length,
        "Invalid offset - off: %s, len: %s, bufferSize: %s", off, len, buf.length);
    Preconditions.checkArgument(len >= 0 && off + len <= buf.length,
        "Invalid length - off: %s, len: %s, bufferSize: %s", off, len, buf.length);
    Preconditions.checkState(!closed.get(), "OutputStream is closed.");

    while (len > 0) {
      if (curPart == null) {
        curPart = newStagingPart();
      }

      Preconditions.checkArgument(curPart.size() <= byteSizePerPart,
          "Invalid staging size (%s) which is greater than part size (%s)", curPart.size(),
          byteSizePerPart);

      // size is the remaining length to fill a complete upload part.
      int size = (int) Math.min(byteSizePerPart - curPart.size(), len);
      curPart.write(buf, off, size);

      off += size;
      len -= size;
      totalWroteSize += size;

      // Switch to the next staging part if current staging part is full.
      if (curPart.size() >= byteSizePerPart) {
        curPart.complete();

        // Upload this part if multipart upload was triggered.
        if (multipartUpload != null) {
          CompletableFuture<Part> result =
              asyncUploadPart(curPart, partNumGetter.incrementAndGet());
          results.add(result);
        }

        // Reset the stagingOut
        curPart = null;
      }

      // Trigger the multipart upload when reach the configured threshold.
      if (multipartUpload == null && totalWroteSize >= multiUploadThreshold) {
        multipartUpload = storage.createMultipartUpload(destKey);
        Preconditions.checkState(byteSizePerPart >= multipartUpload.minPartSize(),
            "Configured upload part size %s must be greater than or equals to the minimal"
                + " part size %s, please check configure key %s.", byteSizePerPart,
            multipartUpload.minPartSize(), ConfKeys.FS_MULTIPART_THRESHOLD.key(destScheme));

        // Upload the accumulated staging files whose length >= byteSizePerPart.
        for (StagingPart stagingPart : stagingParts) {
          if (stagingPart.size() >= byteSizePerPart) {
            CompletableFuture<Part> result =
                asyncUploadPart(stagingPart, partNumGetter.incrementAndGet());
            results.add(result);
          }
        }
      }
    }
  }

  private CompletableFuture<Part> asyncUploadPart(final StagingPart stagingPart,
      final int partNum) {
    final MultipartUpload immutableUpload = multipartUpload;
    return CompletableFuture.supplyAsync(() -> uploadPart(stagingPart, partNum), uploadPool)
        .whenComplete((part, err) -> {
          stagingPart.cleanup();
          if (err != null) {
            LOG.error("Failed to upload part, multipartUpload: {}, partNum: {}, stagingPart: {}",
                immutableUpload, partNum, stagingPart, err);
          }
        });
  }

  private CompletableFuture<Part> asyncUploadEmptyPart(final int partNum) {
    final MultipartUpload immutableUpload = multipartUpload;
    return CompletableFuture.supplyAsync(
            () -> storage.uploadPart(
                immutableUpload.key(),
                immutableUpload.uploadId(),
                partNum,
                () -> new ByteArrayInputStream(new byte[0]),
                0),
            uploadPool)
        .whenComplete((part, err) -> {
          if (err != null) {
            LOG.error("Failed to upload empty part, multipartUpload: {}, partNum: {}",
                immutableUpload, partNum, err);
          }
        });
  }

  private Part uploadPart(StagingPart stagingPart, int partNum) {
    Preconditions.checkNotNull(storage, "Object storage cannot be null.");
    Preconditions.checkNotNull(multipartUpload, "Multipart upload is not initialized.");
    return storage.uploadPart(multipartUpload.key(), multipartUpload.uploadId(),
        partNum, stagingPart::newIn, stagingPart.size());
  }

  protected void finishUpload(String key, String uploadId, List<Part> parts) throws IOException {
    storage.completeUpload(key, uploadId, parts);
  }

  private void simplePut() throws IOException {
    if (curPart != null) {
      curPart.complete();
    }
    storage.put(
        destKey,
        () -> stagingParts()
            .stream()
            .map(StagingPart::newIn)
            .reduce(SequenceInputStream::new)
            .orElseGet(() -> new ByteArrayInputStream(new byte[0])),
        stagingParts().stream().mapToLong(StagingPart::size).sum());
    // Reset the staging output stream.
    curPart = null;
  }

  synchronized List<Part> waitForPartsUpload() {
    Preconditions.checkArgument(multipartUpload != null, "Multipart upload cannot be null");
    Preconditions.checkArgument(!results.isEmpty(), "Upload parts cannot be empty");
    // Waiting for all the upload parts to be finished.
    return results.stream()
        .map(CompletableFuture::join)
        .sorted(Comparator.comparing(Part::num))
        .collect(Collectors.toList());
  }

  @Override
  public synchronized void close() throws IOException {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    try {
      // Use the simple PUT API if wrote bytes is not reached the multipart threshold.
      if (multipartUpload == null && allowPut) {
        simplePut();
        return;
      }
      Preconditions.checkNotNull(multipartUpload,
          "MultipartUpload cannot be null since allowPut was disabled.");

      // Use multipart upload API to upload those parts.
      if (totalWroteSize <= 0) {
        // Write an empty part for this zero-byte file.
        CompletableFuture<Part> result = asyncUploadEmptyPart(partNumGetter.incrementAndGet());
        results.add(result);
      } else if (curPart != null) {
        curPart.complete();
        // Submit the last part to upload thread pool.
        CompletableFuture<Part> result = asyncUploadPart(curPart, partNumGetter.incrementAndGet());
        results.add(result);
        // Reset the staging output stream.
        curPart = null;
      }

      // Finish the multipart uploads.
      finishUpload(multipartUpload.key(), multipartUpload.uploadId(), waitForPartsUpload());

    } catch (Exception e) {
      LOG.error("Encountering error when closing output stream", e);
      if (multipartUpload != null) {
        CommonUtils.runQuietly(
            () -> storage.abortMultipartUpload(multipartUpload.key(), multipartUpload.uploadId()));
      }
      throw e;
    } finally {
      // Clear all the staging part.
      deleteStagingPart(stagingParts);
    }
  }

  public long totalWroteSize() {
    return totalWroteSize;
  }

  public ObjectStorage storage() {
    return storage;
  }

  public List<StagingPart> stagingParts() {
    return stagingParts;
  }

  public String destKey() {
    return destKey;
  }

  public MultipartUpload upload() {
    return multipartUpload;
  }

  private void deleteStagingPart(List<StagingPart> parts) {
    for (StagingPart part : parts) {
      part.cleanup();
    }
  }

  private StagingPart newStagingPart() {
    String stagingPath = String.format("%s/staging-%s.tmp", chooseStagingDir(),
        UUIDUtils.random());
    StagingPart part = new FileStagingPart(stagingPath, stagingBufferSize);
    stagingParts.add(part);
    return part;
  }
}
