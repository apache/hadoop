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

package org.apache.hadoop.fs;

import java.io.IOException;

/**
 * Whether the given filesystem is in any status of safe mode.
 */
public interface SafeMode {

  /**
   * Enter, leave, or get safe mode.
   *
   * @param action One of {@link SafeModeAction} LEAVE, ENTER, GET, FORCE_EXIT.
   * @throws IOException if set safe mode fails to proceed.
   * @return true if the action is successfully accepted, otherwise false means rejected.
   */
  default boolean setSafeMode(SafeModeAction action) throws IOException {
    return setSafeMode(action, false);
  }

  /**
   * Enter, leave, or get safe mode.
   *
   * @param action    One of {@link SafeModeAction} LEAVE, ENTER, GET, FORCE_EXIT.
   * @param isChecked If true check only for Active metadata node / NameNode's status,
   *                  else check first metadata node / NameNode's status.
   * @throws IOException if set safe mode fails to proceed.
   * @return true if the action is successfully accepted, otherwise false means rejected.
   */
  boolean setSafeMode(SafeModeAction action, boolean isChecked) throws IOException;

}
