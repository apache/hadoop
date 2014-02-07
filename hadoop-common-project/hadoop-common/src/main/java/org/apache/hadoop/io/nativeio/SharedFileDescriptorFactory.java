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
package org.apache.hadoop.io.nativeio;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileDescriptor;

import org.apache.commons.lang.SystemUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Preconditions;

/**
 * A factory for creating shared file descriptors inside a given directory.
 * Typically, the directory will be /dev/shm or /tmp.
 *
 * We will hand out file descriptors that correspond to unlinked files residing
 * in that directory.  These file descriptors are suitable for sharing across
 * multiple processes and are both readable and writable.
 *
 * Because we unlink the temporary files right after creating them, a JVM crash
 * usually does not leave behind any temporary files in the directory.  However,
 * it may happen that we crash right after creating the file and before
 * unlinking it.  In the constructor, we attempt to clean up after any such
 * remnants by trying to unlink any temporary files created by previous
 * SharedFileDescriptorFactory instances that also used our prefix.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SharedFileDescriptorFactory {
  private final String prefix;
  private final String path;

  /**
   * Create a SharedFileDescriptorFactory.
   *
   * @param prefix    Prefix to add to all file names we use.
   * @param path      Path to use.
   */
  public SharedFileDescriptorFactory(String prefix, String path)
      throws IOException {
    Preconditions.checkArgument(NativeIO.isAvailable());
    Preconditions.checkArgument(SystemUtils.IS_OS_UNIX);
    this.prefix = prefix;
    this.path = path;
    deleteStaleTemporaryFiles0(prefix, path);
  }

  /**
   * Create a shared file descriptor which will be both readable and writable.
   *
   * @param length         The starting file length.
   *
   * @return               The file descriptor, wrapped in a FileInputStream.
   * @throws IOException   If there was an I/O or configuration error creating
   *                       the descriptor.
   */
  public FileInputStream createDescriptor(int length) throws IOException {
    return new FileInputStream(createDescriptor0(prefix, path, length));
  }

  /**
   * Delete temporary files in the directory, NOT following symlinks.
   */
  private static native void deleteStaleTemporaryFiles0(String prefix,
      String path) throws IOException;

  /**
   * Create a file with O_EXCL, and then resize it to the desired size.
   */
  private static native FileDescriptor createDescriptor0(String prefix,
      String path, int length) throws IOException;
}
