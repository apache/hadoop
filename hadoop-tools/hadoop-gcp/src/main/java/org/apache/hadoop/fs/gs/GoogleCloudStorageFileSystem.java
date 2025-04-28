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

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.*;
import static org.apache.hadoop.fs.gs.Constants.SCHEME;

import com.google.auth.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.WritableByteChannel;

/**
 * Provides FS semantics over GCS based on Objects API
 */
class GoogleCloudStorageFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(StorageResourceId.class);

  // URI of the root path.
  static URI GCS_ROOT = URI.create(SCHEME + ":/");

  // GCS access instance.
  private GoogleCloudStorage gcs;

  private static GoogleCloudStorage createCloudStorage(
      final GoogleHadoopFileSystemConfiguration configuration, final Credentials credentials)
      throws IOException {
    checkNotNull(configuration, "configuration must not be null");

    return new GoogleCloudStorage(configuration);
  }

  public GoogleCloudStorageFileSystem(final GoogleHadoopFileSystemConfiguration configuration,
      final Credentials credentials) throws IOException {
    gcs = createCloudStorage(configuration, credentials);
  }

  public WritableByteChannel create(final URI path, final CreateOptions createOptions)
      throws IOException {
    LOG.trace("create(path: {}, createOptions: {})", path, createOptions);
    checkNotNull(path, "path could not be null");
    StorageResourceId resourceId =
        StorageResourceId.fromUriPath(path, /* allowEmptyObjectName=*/ true);

    if (resourceId.isDirectory()) {
      throw new IOException(
          String.format("Cannot create a file whose name looks like a directory: '%s'",
              resourceId));
    }

    if (createOptions.getOverwriteGenerationId() != StorageResourceId.UNKNOWN_GENERATION_ID) {
      resourceId = new StorageResourceId(resourceId.getBucketName(), resourceId.getObjectName(),
          createOptions.getOverwriteGenerationId());
    }

    return gcs.create(resourceId, createOptions);
  }

  public void close() {
    if (gcs == null) {
      return;
    }
    LOG.trace("close()");
    try {
      gcs.close();
    } finally {
      gcs = null;
    }
  }
}
