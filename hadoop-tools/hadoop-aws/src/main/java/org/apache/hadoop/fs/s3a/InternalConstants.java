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

package org.apache.hadoop.fs.s3a;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Constants for internal use in the org.apache.hadoop.fs.s3a module itself.
 * Please don't refer to these outside of this module &amp; its tests.
 * If you find you need to then either the code is doing something it
 * should not, or these constants need to be uprated to being
 * public and stable entries.
 */
@InterfaceAudience.Private
public final class InternalConstants {

  private InternalConstants() {
  }

  /**
   * The known keys used in a standard openFile call.
   * if there's a select marker in there then the keyset
   * used becomes that of the select operation.
   */
  @InterfaceStability.Unstable
  public static final Set<String> STANDARD_OPENFILE_KEYS =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(Constants.INPUT_FADVISE,
                  Constants.READAHEAD_RANGE)));
}
