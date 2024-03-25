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

package org.apache.hadoop.fs.s3a.scale;

import java.io.IOException;

import org.assertj.core.api.Assertions;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BYTEBUFFER;

/**
 * Use {@link Constants#FAST_UPLOAD_BYTEBUFFER} for buffering.
 * This also renames by parent directory, so validates parent
 * dir renaming of huge files.
 */
public class ITestS3AHugeFilesByteBufferBlocks
    extends AbstractSTestS3AHugeFiles {

  protected String getBlockOutputBufferName() {
    return FAST_UPLOAD_BYTEBUFFER;
  }

  /**
   * Rename the parent directory, rather than the file itself.
   * @param src source file
   * @param dest dest file
   * @throws IOException
   */
  @Override
  protected void renameFile(final Path src, final Path dest) throws IOException {

    final S3AFileSystem fs = getFileSystem();

    final Path srcDir = src.getParent();
    final Path destDir = dest.getParent();
    fs.delete(destDir, true);
    fs.mkdirs(destDir, null);
    final boolean renamed = fs.rename(srcDir, destDir);
    Assertions.assertThat(renamed)
        .describedAs("rename(%s, %s)", src, dest)
        .isTrue();
  }
}
