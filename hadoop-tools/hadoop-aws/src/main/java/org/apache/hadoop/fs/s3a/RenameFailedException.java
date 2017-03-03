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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;

/**
 * Error to indicate that a specific rename failed.
 * The exit code defines the exit code to be returned in the {@code rename()}
 * call.
 * Target path is set to destination.
 */
public class RenameFailedException extends PathIOException {

  /**
   * Exit code to be returned.
   */
  private boolean exitCode = false;

  public RenameFailedException(String src, String dest, Throwable cause) {
    super(src, cause);
    setOperation("rename");
    setTargetPath(dest);
  }

  public RenameFailedException(String src, String dest, String error) {
    super(src, error);
    setOperation("rename");
    setTargetPath(dest);
  }

  public RenameFailedException(Path src, Path optionalDest, String error) {
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
   * @param code exit code to raise
   * @return the exception
   */
  public RenameFailedException withExitCode(boolean code) {
    this.exitCode = code;
    return this;
  }
}
