/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.swift.exceptions;

import org.apache.hadoop.fs.Path;

/**
 * Exception raised when an operation is meant to work on a directory, but
 * the target path is not a directory
 */
public class SwiftNotDirectoryException extends SwiftException {
  private final Path path;

  public SwiftNotDirectoryException(Path path) {
    this(path, "");
  }

  public SwiftNotDirectoryException(Path path,
                                    String message) {
    super(path.toString() + message);
    this.path = path;
  }

  public Path getPath() {
    return path;
  }
}
