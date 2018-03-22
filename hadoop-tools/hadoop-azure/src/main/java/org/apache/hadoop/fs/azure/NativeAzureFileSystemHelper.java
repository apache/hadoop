/**
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

package org.apache.hadoop.fs.azure;

import java.io.EOFException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.StorageErrorCode;
import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.microsoft.azure.storage.StorageException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FSExceptionMessages;

/**
 * Utility class that has helper methods.
 *
 */

@InterfaceAudience.Private
final class NativeAzureFileSystemHelper {

  private NativeAzureFileSystemHelper() {
    // Hiding the cosnstructor as this is a utility class.
  }

  private static final Logger LOG = LoggerFactory.getLogger(NativeAzureFileSystemHelper.class);

  public static void cleanup(Logger log, java.io.Closeable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch(IOException e) {
        if (log != null) {
          log.debug("Exception in closing {}", closeable, e);
        }
      }
    }
  }

  /*
   * Helper method to recursively check if the cause of the exception is
   * a Azure storage exception.
   */
  public static Throwable checkForAzureStorageException(Exception e) {

    Throwable innerException = e.getCause();

    while (innerException != null
            && !(innerException instanceof StorageException)) {

      innerException = innerException.getCause();
    }

    return innerException;
  }

  /*
   * Helper method to check if the AzureStorageException is
   * because backing blob was not found.
   */
  public static boolean isFileNotFoundException(StorageException e) {

    String errorCode = e.getErrorCode();
    if (errorCode != null
        && (errorCode.equals(StorageErrorCodeStrings.BLOB_NOT_FOUND)
            || errorCode.equals(StorageErrorCodeStrings.RESOURCE_NOT_FOUND)
            || errorCode.equals(StorageErrorCode.BLOB_NOT_FOUND.toString())
            || errorCode.equals(StorageErrorCode.RESOURCE_NOT_FOUND.toString()))) {

      return true;
    }

    return false;
  }

  /*
   * Determines if a conditional request failed because the blob already
   * exists.
   *
   * @param e - the storage exception thrown by the failed operation.
   *
   * @return true if a conditional request failed because the blob already
   * exists; otherwise, returns false.
   */
  static boolean isBlobAlreadyExistsConflict(StorageException e) {
    if (e.getHttpStatusCode() == HttpURLConnection.HTTP_CONFLICT
        && StorageErrorCodeStrings.BLOB_ALREADY_EXISTS.equals(e.getErrorCode())) {
      return true;
    }
    return false;
  }

  /*
   * Helper method that logs stack traces from all live threads.
   */
  public static void logAllLiveStackTraces() {

    for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
      LOG.debug("Thread " + entry.getKey().getName());
      StackTraceElement[] trace = entry.getValue();
      for (int j = 0; j < trace.length; j++) {
        LOG.debug("\tat " + trace[j]);
      }
    }
  }

  /**
   * Validation code, based on
   * {@code FSInputStream.validatePositionedReadArgs()}.
   * @param buffer destination buffer
   * @param offset offset within the buffer
   * @param length length of bytes to read
   * @throws EOFException if the position is negative
   * @throws IndexOutOfBoundsException if there isn't space for the amount of
   * data requested.
   * @throws IllegalArgumentException other arguments are invalid.
   */
  static void validateReadArgs(byte[] buffer, int offset, int length)
      throws EOFException {
    Preconditions.checkArgument(length >= 0, "length is negative");
    Preconditions.checkArgument(buffer != null, "Null buffer");
    if (buffer.length - offset < length) {
      throw new IndexOutOfBoundsException(
          FSExceptionMessages.TOO_MANY_BYTES_FOR_DEST_BUFFER
              + ": request length=" + length
              + ", with offset =" + offset
              + "; buffer capacity =" + (buffer.length - offset));
    }
  }
}
