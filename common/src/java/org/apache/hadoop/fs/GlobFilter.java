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

import java.util.regex.PatternSyntaxException;
import java.io.IOException;

 // A class that could decide if a string matches the glob or not
class GlobFilter implements PathFilter {
  private final static PathFilter DEFAULT_FILTER = new PathFilter() {
      public boolean accept(Path file) {
        return true;
      }
    };

  private PathFilter userFilter = DEFAULT_FILTER;
  private GlobPattern pattern;

  GlobFilter(String filePattern) throws IOException {
    init(filePattern, DEFAULT_FILTER);
  }

  GlobFilter(String filePattern, PathFilter filter) throws IOException {
    init(filePattern, filter);
  }

  void init(String filePattern, PathFilter filter) throws IOException {
    try {
      userFilter = filter;
      pattern = new GlobPattern(filePattern);
    }
    catch (PatternSyntaxException e) {
      // Existing code expects IOException startWith("Illegal file pattern")
      throw new IOException("Illegal file pattern: "+ e.getMessage(), e);
    }
  }

  boolean hasPattern() {
    return pattern.hasWildcard();
  }

  public boolean accept(Path path) {
    return pattern.matches(path.getName()) && userFilter.accept(path);
  }
}
