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

package org.apache.hadoop.fs.tosfs.util;

public final class FuseUtils {
  public static final String ENV_TOS_ENABLE_FUSE = "TOS_ENABLE_FUSE";

  private FuseUtils() {
  }

  public static boolean fuseEnabled() {
    return ParseUtils.envAsBoolean(ENV_TOS_ENABLE_FUSE, false);
  }
}
