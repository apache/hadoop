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
 * An exception which occurred when trying to add a path cache directive.
 */
public abstract class AddPathCacheDirectiveException extends IOException {
  private static final long serialVersionUID = 1L;

  private final PathCacheDirective directive;
  
  public AddPathCacheDirectiveException(String description,
      PathCacheDirective directive) {
    super(description);
    this.directive = directive;
  }

  public PathCacheDirective getDirective() {
    return directive;
  }

  public static final class EmptyPathError
      extends AddPathCacheDirectiveException {
    private static final long serialVersionUID = 1L;

    public EmptyPathError(PathCacheDirective directive) {
      super("empty path in directive " + directive, directive);
    }
  }

  public static class InvalidPathNameError
      extends AddPathCacheDirectiveException {
    private static final long serialVersionUID = 1L;

    public InvalidPathNameError(PathCacheDirective directive) {
      super("can't handle non-absolute path name " + directive.getPath(),
          directive);
    }
  }

  public static class InvalidPoolNameError
      extends AddPathCacheDirectiveException {
    private static final long serialVersionUID = 1L;

    public InvalidPoolNameError(PathCacheDirective directive) {
      super("invalid pool name '" + directive.getPool() + "'", directive);
    }
  }

  public static class UnexpectedAddPathCacheDirectiveException
      extends AddPathCacheDirectiveException {
    private static final long serialVersionUID = 1L;

    public UnexpectedAddPathCacheDirectiveException(
        PathCacheDirective directive) {
      super("encountered an unexpected error when trying to " +
          "add path cache directive " + directive, directive);
    }
  }
};
