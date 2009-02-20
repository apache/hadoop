/**
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
package org.apache.hadoop.fs.permission;

/**
 * File system actions, e.g. read, write, etc.
 */
public enum FsAction {
  //POSIX style
  NONE(0, "---"),
  EXECUTE(1, "--x"),
  WRITE(2, "-w-"),
  WRITE_EXECUTE(3, "-wx"),
  READ(4, "r--"),
  READ_EXECUTE(5, "r-x"),
  READ_WRITE(6, "rw-"),
  ALL(7, "rwx");

  //constants
  /** Octal representation */
  public final int INDEX;
  /** Symbolic representation */
  public final String SYMBOL;

  private FsAction(int v, String s) {
    INDEX = v;
    SYMBOL = s;
  }

  /**
   * Return true if this action implies that action.
   * @param that
   */
  public boolean implies(FsAction that) {
    if (that != null) {
      return (this.INDEX & that.INDEX) == that.INDEX;
    }
    return false;
  }

  /** AND operation. */
  public FsAction and(FsAction that) {
    return values()[this.INDEX & that.INDEX];
  }
  /** OR operation. */
  public FsAction or(FsAction that) {
    return values()[this.INDEX | that.INDEX];
  }
  /** NOT operation. */
  public FsAction not() {
    return values()[7 - INDEX];
  }
}
