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

import java.io.IOException;

/**
 * Exceptions based on standard posix/linux style exceptions for path related
 * errors. Returns an exception with the format "path: standard error string".
 * 
 * This exception corresponds to Error Input/ouput(EIO)
 */
public class PathIOException extends IOException {
  static final long serialVersionUID = 0L;
  private static final String EIO = "Input/output error";
  // NOTE: this really should be a Path, but a Path is buggy and won't
  // return the exact string used to construct the path, and it mangles
  // uris with no authority
  private String operation;
  private String path;
  private String fullyQualifiedPath;
  private String targetPath;

  /**
   * Constructor a generic I/O error exception
   *  @param path for the exception
   */
  public PathIOException(String path) {
    this(path, EIO);
  }

  /**
   * Appends the text of a Throwable to the default error message
   * @param path for the exception
   * @param cause a throwable to extract the error message
   */
  public PathIOException(String path, Throwable cause) {
    this(path, EIO, cause);
  }

  /**
   * Avoid using this method.  Use a subclass of PathIOException if
   * possible.
   * @param path for the exception
   * @param error custom string to use an the error text
   */
  public PathIOException(String path, String error) {
    super(error);
    this.path = path;
  }

  protected PathIOException(String path, String error, Throwable cause) {
    super(error, cause);
    this.path = path;
  }

  public PathIOException withFullyQualifiedPath(String fqPath) {
    fullyQualifiedPath = fqPath;
    return this;
  }

  /** Format:
   * cmd: {operation} `path' {to `target'}: error string
   */
  @Override
  public String getMessage() {
    StringBuilder message = new StringBuilder();
    if (operation != null) {
      message.append(operation + " ");
    }
    message.append(formatPath(path));
    if (targetPath != null) {
      message.append(" to " + formatPath(targetPath));
    }
    message.append(": " + super.getMessage());
    if (getCause() != null) {
      message.append(": " + getCause().getMessage());
    }
    if (fullyQualifiedPath != null && !fullyQualifiedPath.equals(path)) {
      message.append(": ").append(formatPath(fullyQualifiedPath));
    }
    return message.toString();
  }

  /** @return Path that generated the exception */
  public Path getPath()  { return new Path(path); }

  /** @return Path if the operation involved copying or moving, else null */
  public Path getTargetPath() {
    return (targetPath != null) ? new Path(targetPath) : null;
  }    
  
  /**
   * Optional operation that will preface the path
   * @param operation a string
   */
  public void setOperation(String operation) {
    this.operation = operation;
  }
  
  /**
   * Optional path if the exception involved two paths, ex. a copy operation
   * @param targetPath the of the operation
   */
  public void setTargetPath(String targetPath) {
    this.targetPath = targetPath;
  }
  
  private String formatPath(String path) {
    return "`" + path + "'";
  }
}