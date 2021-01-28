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

package org.apache.hadoop.fs.obs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;

/**
 * Exception to indicate a specific rename failure. The exit code defines the
 * value returned by {@link OBSFileSystem#rename(Path, Path)}.
 */
class RenameFailedException extends PathIOException {
  /**
   * Exit code to be returned.
   */
  private boolean exitCode = false;

  RenameFailedException(final Path src, final Path optionalDest,
      final String error) {
    super(src.toString(), error);
    setOperation("rename");
    if (optionalDest != null) {
      setTargetPath(optionalDest.toString());
    }
  }

  public boolean getExitCode() {
    return exitCode;
  }

  /**
   * Set the exit code.
   *
   * @param code exit code to raise
   * @return the exception
   */
  public RenameFailedException withExitCode(final boolean code) {
    this.exitCode = code;
    return this;
  }
}
