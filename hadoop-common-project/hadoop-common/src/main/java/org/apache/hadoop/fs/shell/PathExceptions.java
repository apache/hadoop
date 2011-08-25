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

@SuppressWarnings("serial")
public class PathExceptions {

  /** EIO */
  public static class PathIOException extends IOException {
    static final long serialVersionUID = 0L;
    private static final String EIO = "Input/output error";
    // NOTE: this really should be a Path, but a Path is buggy and won't
    // return the exact string used to construct the path, and it mangles
    // uris with no authority
    private String operation;
    private String path;
    private String targetPath;

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

  /** Generated by rm commands */
  public static class PathIsNotEmptyDirectoryException extends PathExistsException {
    /** @param path for the exception */
    public PathIsNotEmptyDirectoryException(String path) {
      super(path, "Directory is not empty");
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
  
  /** ENOTSUP */
  public static class PathOperationException extends PathExistsException {
    static final long serialVersionUID = 0L;
    /** @param path for the exception */
    public PathOperationException(String path) {
      super(path, "Operation not supported");
    }
  }
}