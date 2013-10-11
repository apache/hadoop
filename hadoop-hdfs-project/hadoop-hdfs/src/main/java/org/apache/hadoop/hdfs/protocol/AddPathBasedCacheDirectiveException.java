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
package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;

/**
 * An exception which occurred when trying to add a PathBasedCache directive.
 */
public abstract class AddPathBasedCacheDirectiveException extends IOException {
  private static final long serialVersionUID = 1L;

  public AddPathBasedCacheDirectiveException(String description) {
    super(description);
  }

  public static final class EmptyPathError
      extends AddPathBasedCacheDirectiveException {
    private static final long serialVersionUID = 1L;

    public EmptyPathError() {
      super("empty path in directive");
    }
  }

  public static class InvalidPathNameError
      extends AddPathBasedCacheDirectiveException {
    private static final long serialVersionUID = 1L;

    public InvalidPathNameError(String msg) {
      super(msg);
    }

    public InvalidPathNameError(PathBasedCacheDirective directive) {
      this("can't handle invalid path name " + directive.getPath());
    }
  }

  public static class InvalidPoolNameError
      extends AddPathBasedCacheDirectiveException {
    private static final long serialVersionUID = 1L;

    public InvalidPoolNameError(String msg) {
      super(msg);
    }

    public InvalidPoolNameError(PathBasedCacheDirective directive) {
      this("invalid pool name '" + directive.getPool() + "'");
    }
  }

  public static class PoolWritePermissionDeniedError
      extends AddPathBasedCacheDirectiveException {
    private static final long serialVersionUID = 1L;

    public PoolWritePermissionDeniedError(String msg) {
      super(msg);
    }

    public PoolWritePermissionDeniedError(PathBasedCacheDirective directive) {
      this("write permission denied for pool '" + directive.getPool() + "'");
    }
  }

  public static class PathAlreadyExistsInPoolError
      extends AddPathBasedCacheDirectiveException {
    private static final long serialVersionUID = 1L;

    public PathAlreadyExistsInPoolError(String msg) {
      super(msg);
    }

    public PathAlreadyExistsInPoolError(PathBasedCacheDirective directive) {
      this("path " + directive.getPath() + " already exists in pool " +
          directive.getPool());
    }
  }

  public static class UnexpectedAddPathBasedCacheDirectiveException
      extends AddPathBasedCacheDirectiveException {
    private static final long serialVersionUID = 1L;

    public UnexpectedAddPathBasedCacheDirectiveException(String msg) {
      super(msg);
    }

    public UnexpectedAddPathBasedCacheDirectiveException(
        PathBasedCacheDirective directive) {
      this("encountered an unexpected error when trying to " +
          "add PathBasedCache directive " + directive);
    }
  }
}
