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
package org.apache.hadoop.fs;

/**
 * Exception corresponding to path not found: ENOENT/ENOFILE
 */
public class PathNotFoundException extends PathIOException {
  static final long serialVersionUID = 0L;
  /** @param path for the exception */
  public PathNotFoundException(String path) {
    super(path, "No such file or directory");
  }

  public PathNotFoundException(String path, Throwable cause) {
    super(path, cause);
  }

  public PathNotFoundException(String path, String error) {
    super(path, error);
  }

  public PathNotFoundException(String path,
      String error,
      Throwable cause) {
    super(path, error, cause);
  }
}
