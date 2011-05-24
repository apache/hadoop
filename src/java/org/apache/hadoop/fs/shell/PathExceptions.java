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

package org.apache.hadoop.fs.shell;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;

/**
 * Standardized posix/linux style exceptions for path related errors.
 * Returns an IOException with the format "path: standard error string".
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

public class PathExceptions {

  /** EIO */
  public static class PathIOException extends IOException {
    static final long serialVersionUID = 0L;
    private static final String EIO = "Input/output error";
    // NOTE: this really should be a Path, but a Path is buggy and won't
    // return the exact string used to construct the path, and it mangles
    // uris with no authority
    private String path;

    /**
     * Constructor a generic I/O error exception
     *  @param path for the exception
     */
    public PathIOException(String path) {
      this(path, EIO, null);
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
      this(path, error, null);
    }

    protected PathIOException(String path, String error, Throwable cause) {
      super(error, cause);
      this.path = path;
    }

    @Override
    public String getMessage() {
      String message = "`" + path + "': " + super.getMessage();
      if (getCause() != null) {
        message += ": " + getCause().getMessage();
      }
      return message;
    }

    /** @return Path that generated the exception */
    public Path getPath()  { return new Path(path); }
  }

  /** ENOENT */
  public static class PathNotFoundException extends PathIOException {
    static final long serialVersionUID = 0L;
    /** @param path for the exception */
    public PathNotFoundException(String path) {
      super(path, "No such file or directory");
    }    
  }

  /** EEXISTS */
  public static class PathExistsException extends PathIOException {
    static final long serialVersionUID = 0L;
    /** @param path for the exception */
    public PathExistsException(String path) {
      super(path, "File exists");
    }
    
    protected PathExistsException(String path, String error) {
      super(path, error);
    }
  }

  /** EISDIR */
  public static class PathIsDirectoryException extends PathExistsException {
    static final long serialVersionUID = 0L;
    /** @param path for the exception */
    public PathIsDirectoryException(String path) {
      super(path, "Is a directory");
    }
  }

  /** ENOTDIR */
  public static class PathIsNotDirectoryException extends PathExistsException {
    static final long serialVersionUID = 0L;
    /** @param path for the exception */
    public PathIsNotDirectoryException(String path) {
      super(path, "Is not a directory");
    }
  }

  /** EACCES */
  public static class PathAccessDeniedException extends PathIOException {
    static final long serialVersionUID = 0L;
    /** @param path for the exception */
    public PathAccessDeniedException(String path) {
      super(path, "Permission denied");
    }
  }

  /** EPERM */
  public static class PathPermissionException extends PathIOException {
    static final long serialVersionUID = 0L;
    /** @param path for the exception */
    public PathPermissionException(String path) {
      super(path, "Operation not permitted");
    }
  }
}
