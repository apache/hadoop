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

package org.apache.hadoop.fs.tosfs.commit;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.util.Lists;

import java.util.List;

public class CommitContext {
  private final List<FileStatus> pendingSets;
  // It will be accessed in multi-threads, please access it in a thread-safe context.
  private final List<String> destKeys;

  public CommitContext(List<FileStatus> pendingSets) {
    this.pendingSets = pendingSets;
    this.destKeys = Lists.newArrayList();
  }

  public List<FileStatus> pendingSets() {
    return pendingSets;
  }

  public synchronized void addDestKey(String destKey) {
    destKeys.add(destKey);
  }

  public synchronized List<String> destKeys() {
    return destKeys;
  }
}
