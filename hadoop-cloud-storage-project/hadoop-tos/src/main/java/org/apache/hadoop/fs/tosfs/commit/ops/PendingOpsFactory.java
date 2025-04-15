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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;

public final class PendingOpsFactory {
  public static final String PENDING_OPS_IMPL = "pending.ops.impl";
  public static final String DEFAULT_PENDING_OPS_IMPL = RawPendingOps.class.getName();

  private PendingOpsFactory() {
  }

  public static PendingOps create(FileSystem fs, ObjectStorage storage) {
    try {
      String opsImpl = fs.getConf().get(PENDING_OPS_IMPL, DEFAULT_PENDING_OPS_IMPL);
      Class<?> clazz = Class.forName(opsImpl);
      return (PendingOps) clazz
          .getDeclaredConstructor(FileSystem.class, ObjectStorage.class)
          .newInstance(fs, storage);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
