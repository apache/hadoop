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

package org.apache.hadoop.fs.s3a.commit;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;

import static org.apache.hadoop.fs.s3a.Constants.BUFFER_DIR;

/**
 * A class which manages access to a temporary directory store, uses the
 * directories listed in {@link Constants#BUFFER_DIR} for this.
 */
final class LocalTempDir {

  private LocalTempDir() {
  }

  private static LocalDirAllocator directoryAllocator;

  private static synchronized LocalDirAllocator getAllocator(
      Configuration conf, String key) {
    if (directoryAllocator != null) {
      String bufferDir = conf.get(key) != null
          ? key : Constants.HADOOP_TMP_DIR;
      directoryAllocator = new LocalDirAllocator(bufferDir);
    }
    return directoryAllocator;
  }

  /**
   * Create a temp file.
   * @param conf configuration to use when creating the allocator
   * @param prefix filename prefix
   * @param size file size, or -1 if not known
   * @return the temp file. The file has been created.
   * @throws IOException IO failure
   */
  public static File tempFile(Configuration conf, String prefix, long size)
      throws IOException {
    return getAllocator(conf, BUFFER_DIR).createTmpFileForWrite(
        prefix, size, conf);
  }

  /**
   * Get a temporary path.
   * @param conf configuration to use when creating the allocator
   * @param prefix filename prefix
   * @param size file size, or -1 if not known
   * @return the temp path.
   * @throws IOException IO failure
   */
  public static Path tempPath(Configuration conf, String prefix, long size)
      throws IOException {
    return getAllocator(conf, BUFFER_DIR)
        .getLocalPathForWrite(prefix, size, conf);
  }

}
