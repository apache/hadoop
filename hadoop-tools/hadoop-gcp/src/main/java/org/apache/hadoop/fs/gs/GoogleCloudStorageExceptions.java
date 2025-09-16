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

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.thirdparty.com.google.common.base.Strings.isNullOrEmpty;
import static org.apache.hadoop.thirdparty.com.google.common.base.Strings.nullToEmpty;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * Miscellaneous helper methods for standardizing the types of exceptions thrown by the various
 * GCS-based FileSystems.
 */
final class GoogleCloudStorageExceptions {

  private GoogleCloudStorageExceptions() {}

  /** Creates FileNotFoundException with suitable message for a GCS bucket or object. */
  static FileNotFoundException createFileNotFoundException(
      String bucketName, String objectName, @Nullable IOException cause) {
    checkArgument(!isNullOrEmpty(bucketName), "bucketName must not be null or empty");
    FileNotFoundException fileNotFoundException =
        new FileNotFoundException(
            String.format(
                "Item not found: '%s'. Note, it is possible that the live version"
                    + " is still available but the requested generation is deleted.",
                StringPaths.fromComponents(bucketName, nullToEmpty(objectName))));
    if (cause != null) {
      fileNotFoundException.initCause(cause);
    }
    return fileNotFoundException;
  }

  static FileNotFoundException createFileNotFoundException(
      StorageResourceId resourceId, @Nullable IOException cause) {
    return createFileNotFoundException(
        resourceId.getBucketName(), resourceId.getObjectName(), cause);
  }

  public static IOException createCompositeException(Collection<IOException> innerExceptions) {
    Preconditions.checkArgument(
            innerExceptions != null && !innerExceptions.isEmpty(),
            "innerExceptions (%s) must be not null and contain at least one element",
            innerExceptions);

    Iterator<IOException> innerExceptionIterator = innerExceptions.iterator();

    if (innerExceptions.size() == 1) {
      return innerExceptionIterator.next();
    }

    IOException combined = new IOException("Multiple IOExceptions.");
    while (innerExceptionIterator.hasNext()) {
      combined.addSuppressed(innerExceptionIterator.next());
    }
    return combined;
  }
}
