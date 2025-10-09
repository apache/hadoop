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

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.StorageException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements WritableByteChannel to provide write access to GCS via java-storage client.
 */
class GoogleCloudStorageClientWriteChannel implements WritableByteChannel {
  private static final Logger LOG =
      LoggerFactory.getLogger(GoogleCloudStorageClientWriteChannel.class);

  private final StorageResourceId resourceId;
  private WritableByteChannel writableByteChannel;

  private GoogleCloudStorageItemInfo completedItemInfo = null;

  GoogleCloudStorageClientWriteChannel(
      final Storage storage,
      final StorageResourceId resourceId,
      final CreateFileOptions createOptions) throws IOException {
    this.resourceId = resourceId;
    BlobWriteSession blobWriteSession = getBlobWriteSession(storage, resourceId, createOptions);
    try {
      this.writableByteChannel = blobWriteSession.open();
    } catch (StorageException e) {
      throw new IOException(e);
    }
  }

  private static BlobInfo getBlobInfo(final StorageResourceId resourceId,
      final CreateFileOptions createOptions) {
    BlobInfo blobInfo = BlobInfo.newBuilder(
            BlobId.of(resourceId.getBucketName(), resourceId.getObjectName(),
                resourceId.getGenerationId())).setContentType(createOptions.getContentType())
        //                    .setMetadata(encodeMetadata(createOptions.getMetadata())) // TODO:
        .build();
    return blobInfo;
  }

  private static BlobWriteSession getBlobWriteSession(final Storage storage,
      final StorageResourceId resourceId, final CreateFileOptions createOptions) {
    return storage.blobWriteSession(getBlobInfo(resourceId, createOptions),
        generateWriteOptions(createOptions));
  }

  private static BlobWriteOption[] generateWriteOptions(final CreateFileOptions createOptions) {
    List<BlobWriteOption> blobWriteOptions = new ArrayList<>();

    blobWriteOptions.add(BlobWriteOption.disableGzipContent());
    blobWriteOptions.add(BlobWriteOption.generationMatch());

    //TODO: Enable KMS and checksum
    return blobWriteOptions.toArray(new BlobWriteOption[blobWriteOptions.size()]);
  }

  @Override
  public boolean isOpen() {
    return writableByteChannel != null && writableByteChannel.isOpen();
  }

  @Override
  public void close() throws IOException {
    try {
      if (!isOpen()) {
        return;
      }

      writableByteChannel.close();
    } catch (Exception e) {
      throw new IOException(
          String.format("Upload failed for '%s'. reason=%s", resourceId, e.getMessage()), e);
    } finally {
      writableByteChannel = null;
    }
  }

  private int writeInternal(final ByteBuffer byteBuffer) throws IOException {
    int bytesWritten = writableByteChannel.write(byteBuffer);
    LOG.trace("{} bytes were written out of provided buffer of capacity {}", bytesWritten,
        byteBuffer.limit());
    return bytesWritten;
  }

  @Override
  public int write(final ByteBuffer src) throws IOException {
    return writeInternal(src);
  }
}
