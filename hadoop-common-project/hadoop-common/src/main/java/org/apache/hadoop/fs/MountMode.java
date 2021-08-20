/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs;

/**
 * Mount mode for provided storage.
 */
public enum MountMode {
  READONLY("readOnly"),
  BACKUP("backup"),
  WRITEBACK("writeBack");

  private String modeFlag;

  MountMode(String modeFlag) {
    this.modeFlag = modeFlag;
  }

  @Override
  public String toString() {
    return this.modeFlag;
  }

  public static MountMode parseMountMode(String mountModeString) {
    if (READONLY.toString().equals(mountModeString)) {
      return READONLY;
    } else if (BACKUP.toString().equals(mountModeString)) {
      return BACKUP;
    } else if (WRITEBACK.toString().equals(mountModeString)) {
      return WRITEBACK;
    }
    throw new RuntimeException(
        "Unrecognized mount mode string: " + mountModeString);
  }
}