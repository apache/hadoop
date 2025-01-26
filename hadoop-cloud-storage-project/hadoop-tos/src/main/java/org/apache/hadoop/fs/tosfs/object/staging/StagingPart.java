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

package org.apache.hadoop.fs.tosfs.object.staging;

import java.io.IOException;
import java.io.InputStream;

public interface StagingPart {

  /**
   * Write bytes into the staging part.
   *
   * @param b the buffer to write.
   * @throws IOException if any IO error.
   */
  default void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  /**
   * Write the bytes into the staging part.
   *
   * @param b   the buffer to write.
   * @param off the start offset in buffer.
   * @param len the length.
   * @throws IOException if any IO error.
   */
  void write(byte[] b, int off, int len) throws IOException;

  /**
   * Complete the writing process and cannot write more bytes once we've completed this part.
   *
   * @throws IOException if any IO error.
   */
  void complete() throws IOException;

  /**
   * The wrote size of staging part.
   *
   * @return the staging part size.
   */
  long size();

  /**
   * Access the {@link State} of this part.
   *
   * @return the {@link State}.
   */
  State state();

  /**
   * Create a separate new {@link InputStream} to read the staging part data once we've completed
   * the writing by calling {@link StagingPart#complete()} . Call this method several times will
   * return many {@link InputStream}s, and remember to close the newly created stream.
   *
   * @return a totally new {@link InputStream}.
   */
  InputStream newIn();

  /**
   * Clean all the {@link  StagingPart}'s resources, such as removing temporary file, free the
   * buffered data etc. it should be idempotent and quiet (without throwing IO error).
   */
  void cleanup();
}
