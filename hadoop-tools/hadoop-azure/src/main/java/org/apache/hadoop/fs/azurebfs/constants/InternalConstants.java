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

package org.apache.hadoop.fs.azurebfs.constants;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Constants which are used internally and which don't fit into the other
 * classes.
 * For use within the {@code hadoop-azure} module only.
 */
@InterfaceAudience.Private
public final class InternalConstants {

  private InternalConstants() {
  }

  /**
   * Does this version of the store have safe readahead?
   * Possible combinations of this and the probe
   * {@code "fs.capability.etags.available"}.
   * <ol>
   *   <li>{@value}: store is safe</li>
   *   <li>no etags: store is safe</li>
   *   <li>etags and not {@value}: store is <i>UNSAFE</i></li>
   * </ol>
   */
  public static final String CAPABILITY_SAFE_READAHEAD =
      "fs.azure.capability.readahead.safe";
}
