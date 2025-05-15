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

package org.apache.hadoop.fs.tosfs.commit.ops;

import org.apache.hadoop.fs.tosfs.commit.Pending;

public interface PendingOps {
  /**
   * Revert the committed {@link Pending}, usually we need to remove or delete the committed files.
   *
   * @param commit to revert.
   */
  void revert(Pending commit);

  /**
   * Abort the uncommitted {@link Pending}, to prevent any further committing.
   *
   * @param commit to abort.
   */
  void abort(Pending commit);

  /**
   * Commit the {@link Pending} files to be visible. If we want to revert this completed result,
   * please just use {@link PendingOps#revert(Pending)} to revert this commit.
   *
   * @param commit to be visible.
   */
  void commit(Pending commit);
}
